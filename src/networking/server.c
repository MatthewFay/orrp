/*
 * =====================================================================================
 *
 * Filename:  server.c
 *
 * Description:  High-performance TCP server for a custom database engine using
 * libuv.
 *
 * =====================================================================================
 */

#include "networking/server.h"
#include "core/queue.h"
#include "engine/api.h"
#include "log/log.h"
#include "networking/translator.h"
#include "query/parser.h"
#include "query/tokenizer.h"
#include "uv.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

LOG_INIT(server);

// --- Server Configuration ---
// TODO: Load from a config file
#define MAX_CONCURRENT_CONNECTIONS 1024 // Max number of clients
#define CONNECTION_IDLE_TIMEOUT 600000  // 10 minutes in milliseconds
#define MAX_COMMAND_LENGTH 8192         // 8KB max command size
#define READ_BUFFER_SIZE 65536          // 64KB per-client read buffer
#define SERVER_BACKLOG 511              // Listen backlog connections

#define INTERNAL_SERVER_ERROR_MSG "Error: Internal server error\n"

// --- Globals ---
static uv_tcp_t server_handle;    // Main server handle
static uv_signal_t signal_handle; // Signal handler for SIGINT
static int active_connections = 0;
static unsigned long long next_client_id = 0;

/*
 * Client structure
 * We create one of these for each connected client.
 */
typedef struct {
  uv_tcp_t handle;
  uv_timer_t timeout_timer;
  char read_buffer[READ_BUFFER_SIZE];
  int buffer_len;
  long long client_id;
  // --- Reference counter for associated handles ---
  // This counter tracks the number of open handles (TCP and timer) for this
  // client. The client struct is only freed when this count reaches zero.
  int open_handles;

  // --- Outstanding work reference count ---
  // Incremented when we queue background work; decremented when work completes.
  int work_refs;

  // Simple 'connected' flag. Set to 0 in on_close.
  int connected;
} client_t;

// Forward declarations for callbacks
void on_close(uv_handle_t *handle);
void on_write(uv_write_t *req, int status);
void process_data_buffer(client_t *client);

/**
 * @brief Simple write request structure.
 */
typedef struct {
  uv_write_t req;
  uv_buf_t buf;
  char write_buf[]; // Use a flexible array member (FAM) for the buffer.
} write_req_t;

/**
 * @brief Work context for processing commands in the thread pool
 */
typedef struct {
  uv_work_t req;
  client_t *client;
  char *command;
  char *response;
  // Points to the memory to free (NULL if static, same as response if heap)
  char *response_to_free;
} work_ctx_t;

/**
 * @brief Helper function to close all handles associated with a client.
 * This ensures that both the TCP and timer handles are closed, which is
 * crucial for the reference counting in on_close to work correctly.
 *
 * @param client The client to close.
 */
static void _close_client_connection(client_t *client) {
  // Use uv_is_closing to prevent calling uv_close on a handle that is
  // already in the process of closing.
  if (client->connected && !uv_is_closing((uv_handle_t *)&client->handle)) {
    uv_close((uv_handle_t *)&client->handle, on_close);
  }
  if (!uv_is_closing((uv_handle_t *)&client->timeout_timer)) {
    uv_close((uv_handle_t *)&client->timeout_timer, on_close);
  }
}

/**
 * @brief Callback function for when a client's idle timer fires.
 *
 * @param timer The timer handle that fired.
 */
void on_timeout(uv_timer_t *timer) {
  client_t *client = timer->data;
  LOG_ACTION_WARN(ACT_CLIENT_TIMEOUT, "client_id=%lld", client->client_id);
  _close_client_connection(client);
}

/**
 * @brief Allocates a buffer for libuv to read data into.
 * This is a required libuv callback. Whenever libuv is ready to read data
 * from a socket, it calls this function to ask our application for a
 * memory buffer to store that data in. We point it to the client's dedicated
 * read buffer.
 *
 * @param handle The client handle.
 * @param suggested_size A size suggestion from libuv.
 * @param buf The buffer structure to be filled.
 */
void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  (void)suggested_size;
  client_t *client = handle->data;
  buf->base = client->read_buffer + client->buffer_len;
  buf->len = READ_BUFFER_SIZE - client->buffer_len;
}

/**
 * @brief Callback for closing rejected connections (due to max connections).
 * This is used to free the temporary handle allocated to reject a connection.
 * @param handle The handle that was closed.
 */
void on_rejected_close(uv_handle_t *handle) { free(handle); }

/**
 * @brief Callback function for when a handle is fully closed.
 * A cleanup callback. After we tell libuv to close a handle
 * (like a client connection), it performs the close operation
 * asynchronously. When it's fully done, it calls on_close so we can
 * free any memory associated with that client.
 *
 * This function acts as a reference counter. A client has two handles
 * (TCP and timer). We only free the client's memory after BOTH handles have
 * been successfully closed and this callback has been fired for each of them.
 *
 * @param handle The handle that was closed.
 */
void on_close(uv_handle_t *handle) {
  client_t *client = handle->data;
  if (!client)
    return;

  client->open_handles--;
  // mark disconnected if the TCP handle closed
  if (handle->type == UV_TCP) {
    client->connected = 0;
  }

  // Centralized cleanup logic. Check if this is the last reference.
  if (client->open_handles == 0 && client->work_refs == 0) {
    LOG_ACTION_INFO(ACT_CLIENT_FREED, "client_id=%lld", client->client_id);
    active_connections--;
    free(client);
  }
}

/**
 * @brief Sends a response back to the client.
 * A helper function to send data back to a client.
 * It wraps the uv_write logic.
 *
 * @param client The client to respond to.
 * @param response The null-terminated string to send.
 */
void send_response(client_t *client, const char *response) {
  if (!client || !client->connected || !response)
    return;

  size_t len = strlen(response);
  if (len == 0)
    return;

  // Use flexible array member (FAM) for a single allocation.
  write_req_t *wr = malloc(sizeof(write_req_t) + len);
  if (!wr) {
    LOG_ACTION_ERROR(ACT_MEMORY_ALLOC_FAILED,
                     "context=\"write_request\" client_id=%lld",
                     client->client_id);
    return;
  }

  memcpy(wr->write_buf, response, len);
  wr->buf = uv_buf_init(wr->write_buf, (unsigned int)len);

  int r =
      uv_write(&wr->req, (uv_stream_t *)&client->handle, &wr->buf, 1, on_write);
  if (r) {
    LOG_ACTION_ERROR(ACT_WRITE_FAILED, "client_id=%lld err=\"%s\"",
                     client->client_id, uv_strerror(r));
    free(wr); // Free on failure.
  }
}

/**
 * @brief Callback function for when a write operation completes.
 * The callback for send_response. After libuv finishes writing
 * our data to the socket, it calls this function so we can free
 * the memory we allocated for the write request and the message buffer.
 * @param req The write request.
 * @param status Status of the write operation.
 */
void on_write(uv_write_t *req, int status) {
  if (status < 0) {
    LOG_ACTION_ERROR(ACT_WRITE_FAILED, "err=\"%s\"", uv_strerror(status));
  }
  // Free the write request struct itself, which also frees the buffer (FAM).
  free(req);
}

/* Centralized cleanup logic for work contexts. */
static void _decrement_work_and_cleanup(client_t *client) {
  client->work_refs--;
  if (client->open_handles == 0 && client->work_refs == 0) {
    LOG_ACTION_INFO(ACT_CLIENT_FREED, "client_id=%lld reason=\"work_complete\"",
                    client->client_id);
    active_connections--;
    free(client);
  }
}

/**
 * @brief Background worker: runs in libuv thread-pool
 * Handles tokenization, parsing, and command execution for all command types
 */
static void _work_cb(uv_work_t *req) {
  work_ctx_t *ctx = (work_ctx_t *)req;

  LOG_ACTION_DEBUG(ACT_CMD_PROCESSING, "client_id=%lld",
                   ctx->client->client_id);

  Queue *tokens = NULL;
  parse_result_t *parsed = NULL;
  api_response_t *api_resp = NULL;
  translator_result_t tr = {0};

  tokens = tok_tokenize(ctx->command);
  if (!tokens) {
    LOG_ACTION_DEBUG(ACT_TOKENIZATION_FAILED, "client_id=%lld",
                     ctx->client->client_id);
    ctx->response = "Error: Unrecognized character\n";
    goto cleanup;
  }

  parsed = parse(tokens);
  if (parsed->type == PARSER_OP_TYPE_ERROR) {
    LOG_ACTION_DEBUG(ACT_PARSE_FAILED, "client_id=%lld err=\"%s\"",
                     ctx->client->client_id,
                     parsed->error_message ? parsed->error_message : "unknown");
    ctx->response = "Error: Syntax error\n";
    goto cleanup;
  }

  api_resp = api_exec(parsed->ast);

  if (!api_resp) {
    LOG_ACTION_ERROR(ACT_API_EXEC_FAILED,
                     "client_id=%lld err=\"returned_null\"",
                     ctx->client->client_id);
    ctx->response = INTERNAL_SERVER_ERROR_MSG;
  } else if (!api_resp->is_ok) {
    const char *err =
        api_resp->err_msg ? api_resp->err_msg : "Execution failed";
    LOG_ACTION_ERROR(ACT_CMD_EXEC_FAILED, "client_id=%lld err=\"%s\"",
                     ctx->client->client_id, err);
    ctx->response = (char *)err;
  } else {
    translate(api_resp, TRANSLATOR_RESP_FORMAT_TYPE_TEXT, &tr);
    if (!tr.success) {
      LOG_ACTION_ERROR(ACT_TRANSLATION_ERROR, "client_id=%lld err=\"%s\"",
                       ctx->client->client_id, tr.err_msg);
      ctx->response = INTERNAL_SERVER_ERROR_MSG;
    } else {
      ctx->response = ctx->response_to_free = tr.response;
    }
  }

cleanup:
  if (tokens) {
    tok_clear_all(tokens);
    q_destroy(tokens);
  }

  if (parsed) {
    parse_free_result(parsed);
  }

  if (api_resp) {
    free_api_response(api_resp);
  }

  free(ctx->command);
  ctx->command = NULL;
}

/**
 * @brief after_work_cb: runs on the loop thread after work_cb completes
 * Sends the response back to the client.
 * IMPORTANT: the Main Loop (after_work_cb) must remain non-blocking
 */
static void _after_work_cb(uv_work_t *req, int status) {
  work_ctx_t *ctx = (work_ctx_t *)req;
  client_t *client = ctx->client;

  if (status != 0) {
    // System-level error from libuv
    LOG_ACTION_ERROR(ACT_WORK_QUEUE_FAILED, "client_id=%lld err=\"%s\"",
                     client->client_id, uv_strerror(status));
    send_response(client, INTERNAL_SERVER_ERROR_MSG);
  } else if (ctx->response) {
    send_response(client, ctx->response);
  } else {
    LOG_ACTION_ERROR(ACT_WORK_QUEUE_FAILED, "client_id=%lld err=\"%s\"",
                     client->client_id, "Missing response");
    send_response(client, INTERNAL_SERVER_ERROR_MSG);
  }

  _decrement_work_and_cleanup(client);

  free(ctx->response_to_free);
  free(ctx);
}

/**
 * @brief Processes a single, complete command from a client.
 *
 * @param client The client who sent the command.
 * @param command The null-terminated command string.
 */
void process_one_command(client_t *client, char *command) {
  uv_loop_t *loop = client->handle.loop;
  size_t cmd_len = strlen(command);

  // Trim trailing CR/LF.
  while (cmd_len > 0 &&
         (command[cmd_len - 1] == '\n' || command[cmd_len - 1] == '\r')) {
    command[--cmd_len] = '\0';
  }

  if (cmd_len == 0)
    return; // Ignore empty commands.

  LOG_ACTION_DEBUG(ACT_CMD_RECEIVED, "client_id=%lld cmd_len=%zu",
                   client->client_id, cmd_len);

  work_ctx_t *ctx = calloc(1, sizeof(work_ctx_t));
  if (!ctx) {
    LOG_ACTION_ERROR(ACT_MEMORY_ALLOC_FAILED,
                     "context=\"work_context\" client_id=%lld",
                     client->client_id);
    send_response(client, "Error: Internal server error (OOM)\n");
    return;
  }

  ctx->command = malloc(cmd_len + 1);
  if (!ctx->command) {
    LOG_ACTION_ERROR(ACT_MEMORY_ALLOC_FAILED,
                     "context=\"command_buffer\" client_id=%lld",
                     client->client_id);
    free(ctx);
    send_response(client, "Error: Internal server error (OOM)\n");
    return;
  }
  memcpy(ctx->command, command, cmd_len + 1);

  ctx->client = client;
  ctx->req.data = ctx;
  client->work_refs++; // This work context now holds a reference to the client.

  int rc = uv_queue_work(loop, &ctx->req, _work_cb, _after_work_cb);
  if (rc != 0) {
    LOG_ACTION_ERROR(ACT_WORK_QUEUE_FAILED, "client_id=%lld err=\"%s\"",
                     client->client_id, uv_strerror(rc));
    // Rollback
    client->work_refs--;
    free(ctx->command);
    free(ctx);
    send_response(client, "Error: Failed to queue work\n");
  }
}

/**
 * @brief Scans the client's read buffer for complete commands
 * (newline-terminated).
 *
 * A command is considered complete if it's terminated by a newline ('\n').
 * This function handles multiple commands in a single read (pipelining).
 *
 * This function is the "command framer." It scans the client's read buffer
 * for newline characters (\n). For each complete command it finds,
 * it calls `process_one_command` and then cleans up the buffer.
 *
 * This function allows us to handle multiple commands sent in a
 * single network packet (pipelining).
 *
 * @param client The client whose buffer needs processing.
 */
void process_data_buffer(client_t *client) {
  char *buffer_start = client->read_buffer;
  char *buffer_end = client->read_buffer + client->buffer_len;

  while (1) {
    char *newline_pos = memchr(buffer_start, '\n', buffer_end - buffer_start);
    if (!newline_pos)
      break; // No more complete commands.

    *newline_pos = '\0'; // Null-terminate to create a command string.

    // Security: Check command length *before* processing.
    if (newline_pos - buffer_start > MAX_COMMAND_LENGTH) {
      LOG_ACTION_ERROR(ACT_CMD_TOO_LONG, "client_id=%lld max_len=%d",
                       client->client_id, MAX_COMMAND_LENGTH);
      send_response(client, "Error: Command too long\n");
      _close_client_connection(client);
      return;
    }

    process_one_command(client, buffer_start);

    // If client was disconnected by `process_one_command`, stop processing its
    // buffer.
    if (!client->connected)
      return;

    // Advance the buffer start past the processed command and the newline
    buffer_start = newline_pos + 1;
  }

  // Move any leftover partial command to the beginning of the buffer.
  int remaining_len = buffer_end - buffer_start;
  if (remaining_len > 0 && buffer_start != client->read_buffer) {
    memmove(client->read_buffer, buffer_start, remaining_len);
  }
  client->buffer_len = remaining_len;

  // Security: If the buffer is full with no newline, close the connection.
  if (client->buffer_len == READ_BUFFER_SIZE) {
    LOG_ACTION_ERROR(ACT_BUFFER_OVERFLOW, "client_id=%lld buffer_size=%d",
                     client->client_id, READ_BUFFER_SIZE);
    send_response(client, "Error: Command buffer overflow\n");
    _close_client_connection(client);
  }
}

/**
 * @brief Callback for when data is read from a client socket.
 * The main data-handling callback. libuv calls this whenever it has
 * successfully read data from a client. It updates the client's buffer,
 * resets their idle timer, and calls `process_data_buffer` to check
 * for complete commands.
 *
 * @param stream The client stream.
 * @param nread The number of bytes read.
 * @param buf The buffer containing the data.
 */
void on_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
  (void)buf; // We don't need buf, we use the one from our alloc_buffer.
  client_t *client = stream->data;

  if (nread > 0) {
    // Data received, update buffer length and reset idle timer
    client->buffer_len += nread;
    LOG_ACTION_DEBUG(ACT_DATA_RECEIVED, "client_id=%lld bytes=%zd",
                     client->client_id, nread);
    uv_timer_again(&client->timeout_timer); // Reset idle timer.
    // Process any complete commands in the buffer
    process_data_buffer(client);
  } else if (nread < 0) {
    if (nread != UV_EOF) {
      LOG_ACTION_ERROR(ACT_READ_FAILED, "client_id=%lld err=\"%s\"",
                       client->client_id, uv_strerror(nread));
    } else {
      LOG_ACTION_INFO(ACT_CLIENT_DISCONNECTED, "client_id=%lld reason=EOF",
                      client->client_id);
    }
    _close_client_connection(client);
  }
  // nread == 0 is possible and means nothing to read right now.
}

/**
 * @brief Callback for when a new client connects to the server.
 * This callback is executed by libuv every time a new client connects
 * to your server's listening port. It checks the connection limit,
 * allocates a new client_t struct, accepts the connection,
 * and starts the read and timer loops for that new client.
 *
 * @param server The server stream handle.
 * @param status Status of the connection attempt.
 */
void on_new_connection(uv_stream_t *server, int status) {
  if (status < 0) {
    LOG_ACTION_ERROR(ACT_CONNECTION_FAILED, "err=\"%s\"", uv_strerror(status));
    return;
  }

  uv_loop_t *loop = server->loop;

  if (active_connections >= MAX_CONCURRENT_CONNECTIONS) {
    LOG_ACTION_WARN(ACT_CONNECTION_REJECTED, "reason=max_connections max=%d",
                    MAX_CONCURRENT_CONNECTIONS);
    // To reject, we must still accept, then immediately close.
    uv_tcp_t *temp_client = malloc(sizeof(uv_tcp_t));
    if (temp_client && uv_tcp_init(loop, temp_client) == 0) {
      if (uv_accept(server, (uv_stream_t *)temp_client) == 0) {
        // Use a dedicated close callback that just frees the handle
        uv_close((uv_handle_t *)temp_client, on_rejected_close);
      } else {
        free(temp_client);
      }
    }
    return;
  }

  // Use calloc for zero-initialized memory.
  client_t *client = calloc(1, sizeof(client_t));
  if (!client) {
    LOG_ACTION_FATAL(ACT_MEMORY_ALLOC_FAILED, "context=\"new_client\"");
    return; // Cannot recover.
  }

  client->client_id = ++next_client_id;
  uv_tcp_init(loop, &client->handle);
  client->handle.data = client; // Link client state to the handle
  client->open_handles = 1;
  client->connected = 1;

  // Accept the connection
  if (uv_accept(server, (uv_stream_t *)&client->handle) == 0) {
    active_connections++;
    LOG_ACTION_INFO(ACT_CLIENT_CONNECTED, "client_id=%lld total_connections=%d",
                    client->client_id, active_connections);

    // --- Initialize the timer handle and increment the handle counter ---
    uv_timer_init(loop, &client->timeout_timer);
    client->timeout_timer.data = client;
    client->open_handles++;
    uv_timer_start(&client->timeout_timer, on_timeout, CONNECTION_IDLE_TIMEOUT,
                   0);

    // Start reading data from the client
    uv_read_start((uv_stream_t *)&client->handle, alloc_buffer, on_read);
  } else {
    LOG_ACTION_ERROR(ACT_CONNECTION_FAILED, "err=\"accept_failed\"");
    uv_close((uv_handle_t *)&client->handle, on_close);
  }
}

/**
 * @brief Iterates over all active handles and closes them during shutdown.
 * This function is used during graceful shutdown to close all client
 * connections and their associated timers.
 * @param handle The handle to inspect.
 * @param arg User-provided argument (not used here).
 */
static void _close_walk_cb(uv_handle_t *handle, void *arg) {
  (void)arg;
  if (handle != (uv_handle_t *)&server_handle &&
      handle != (uv_handle_t *)&signal_handle) {
    if (!uv_is_closing(handle)) {
      LOG_ACTION_DEBUG(ACT_HANDLE_CLOSING, "handle_type=%s",
                       uv_handle_type_name(handle->type));
      uv_close(handle, on_close);
    }
  }
}

// A function to handle all shutdown logic.
static void _initiate_shutdown(void) {
  // Use a static flag to ensure this only runs once.
  static bool shutting_down = false;
  if (shutting_down)
    return;
  shutting_down = true;

  LOG_ACTION_WARN(ACT_SERVER_SHUTDOWN_INITIATED, "");

  uv_loop_t *loop = server_handle.loop;

  // Close the signal handle so the loop can exit cleanly.
  uv_close((uv_handle_t *)&signal_handle, NULL);

  // Stop accepting new connections.
  uv_close((uv_handle_t *)&server_handle, NULL);

  // Close all client-related handles.
  uv_walk(loop, _close_walk_cb, NULL);
}

/**
 * @brief Callback for handling SIGINT (Ctrl+C) for graceful shutdown.
 */
static void _on_signal(uv_signal_t *handle, int signum) {
  (void)signum;
  LOG_ACTION_WARN(ACT_SIGNAL_RECEIVED,
                  "signal=SIGINT action=graceful_shutdown");

  // Stop the signal handler to prevent multiple shutdown signals
  uv_signal_stop(handle);
  _initiate_shutdown();
}

/**
 * @brief Starts the database server and runs the event loop.
 * The main public function. It initializes libuv, sets up the
 * TCP listener on the specified host and port, and starts the
 * libuv event loop, which is the heart of the server that waits
 * for and dispatches all events.
 *
 * @param host The IP address to bind to (e.g., "0.0.0.0").
 * @param port The port to listen on.
 * @param loop Main event loop.
 */
void start_server(const char *host, int port, uv_loop_t *loop) {
  log_init_server();

  if (!LOG_CATEGORY) {
    fprintf(stderr, "FATAL: Failed to initialize server logging\n");
    return;
  }

  uv_signal_init(loop, &signal_handle);
  uv_signal_start(&signal_handle, _on_signal, SIGINT);

  struct sockaddr_in addr;
  uv_ip4_addr(host, port, &addr);

  uv_tcp_init(loop, &server_handle);
  uv_tcp_bind(&server_handle, (const struct sockaddr *)&addr, 0);

  int r = uv_listen((uv_stream_t *)&server_handle, SERVER_BACKLOG,
                    on_new_connection);
  if (r) {
    LOG_ACTION_FATAL(ACT_SERVER_START_FAILED, "err=\"%s\"", uv_strerror(r));
    return;
  }

  LOG_ACTION_INFO(ACT_SERVER_STARTED, "host=\"%s\" port=%d", host, port);
  LOG_ACTION_INFO(ACT_SERVER_CONFIG,
                  "max_conn=%d timeout_ms=%d max_cmd_len=%d backlog=%d",
                  MAX_CONCURRENT_CONNECTIONS, CONNECTION_IDLE_TIMEOUT,
                  MAX_COMMAND_LENGTH, SERVER_BACKLOG);

  // This call blocks until all handles are closed
  uv_run(loop, UV_RUN_DEFAULT);

  // --- Shutdown Sequence ---
  LOG_ACTION_INFO(ACT_SERVER_FINALIZING, "");

  // Ensure all close callbacks are processed.
  uv_run(loop, UV_RUN_NOWAIT);

  // Close the loop itself.
  int close_status = uv_loop_close(loop);
  if (close_status != 0) {
    LOG_ACTION_WARN(ACT_LOOP_CLOSE_FAILED, "status=%d err=\"%s\"", close_status,
                    uv_strerror(close_status));
    // Forcibly clean up remaining handles if any exist.
    uv_walk(loop, (uv_walk_cb)uv_close, NULL);
    uv_run(loop, UV_RUN_ONCE); // allow close callbacks to run
    uv_loop_close(loop);       // try again
  }

  LOG_ACTION_INFO(ACT_SYSTEM_SHUTDOWN, "component=server status=complete");
}