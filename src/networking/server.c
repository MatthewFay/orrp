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

// TODO: Split this file into multiple smaller modules

#include "networking/server.h"
#include "api.h"
#include "core/queue.h"
#include "log.h"
#include "query/parser.h"
#include "query/tokenizer.h"
#include "uv.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// TODO: Implement Correlation IDs for Pipelining.
// The current implementation processes read commands in parallel using a thread
// pool. This can cause responses to be sent back to the client out of order if
// multiple commands are pipelined on the same connection (e.g., a fast command
// can finish before a slow one that was received earlier).
//
// To fix this and enable safe, high-performance client-side pipelining, the
// protocol should be updated to include a client-provided request/correlation
// ID. The server must parse this ID and echo it back in the corresponding
// response. This allows the client to correctly match asynchronous responses
// to their original requests.

/*
  IMPORTANT operational note:
  - We may want to increase the libuv threadpool size
    (example): `export UV_THREADPOOL_SIZE=16` before starting the server.
*/

// --- Server Configuration ---
// TODO: Load from a config file
#define MAX_CONCURRENT_CONNECTIONS 1024 // Max number of clients
#define CONNECTION_IDLE_TIMEOUT 600000  // 10 minutes in milliseconds
#define MAX_COMMAND_LENGTH 8192         // 8KB max command size
#define READ_BUFFER_SIZE 65536          // 64KB per-client read buffer
#define SERVER_BACKLOG 511              // Listen backlog connections

// --- Globals ---
static uv_tcp_t server_handle;    // Main server handle
static uv_signal_t signal_handle; // Signal handler for SIGINT
static int active_connections = 0;
static long long next_client_id =
    0; // Use a rolling ID for better logging. TODO: Consider UUID

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
 * @brief Helper function to close all handles associated with a client.
 * This ensures that both the TCP and timer handles are closed, which is
 * crucial for the reference counting in on_close to work correctly.
 *
 * @param client The client to close.
 */
static void close_client_connection(client_t *client) {
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
  log_warn("Client %lld timed out due to inactivity. Closing connection.",
           client->client_id);
  close_client_connection(client);
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
  // We use a fixed-size buffer on the client struct, so we just point to it.
  // This is simpler than malloc/free for every read.
  client_t *client = handle->data;
  // Point to the remaining space in our fixed-size buffer.
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

  // Centralized cleanup logic. Check if this is the last
  // reference.
  if (client->open_handles == 0 && client->work_refs == 0) {
    log_info("Client %lld connection fully closed and resources freed.",
             client->client_id);
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
    log_error("Failed to allocate write request for client %lld",
              client->client_id);
    return;
  }

  memcpy(wr->write_buf, response, len);
  wr->buf = uv_buf_init(wr->write_buf, (unsigned int)len);

  int r =
      uv_write(&wr->req, (uv_stream_t *)&client->handle, &wr->buf, 1, on_write);
  if (r) {
    log_error("uv_write failed to client %lld: %s", client->client_id,
              uv_strerror(r));
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
    log_error("Write error: %s", uv_strerror(status));
  }
  // Free the write request struct itself, which also frees the buffer (FAM).
  free(req);
}

/* ------------------ Background work & writer thread infra ------------------
 */

typedef enum {
  WORK_STATUS_PENDING = 0,
  WORK_STATUS_DONE_OK,
  WORK_STATUS_DONE_ERR,
  WORK_STATUS_PENDING_WRITE,
} work_status_t;

typedef struct {
  uv_work_t req;
  client_t *client;
  char *command;
  api_response_t *result;
  work_status_t status;
} work_ctx_t;

typedef struct writer_task_s {
  parse_result_t *parsed;
  work_ctx_t *ctx;
  struct writer_task_s *next;
} writer_task_t;

typedef struct completion_item_s {
  api_response_t *result;
  work_ctx_t *ctx;
  struct completion_item_s *next;
} completion_item_t;

/* --- Queues and Threading Primitives --- */
static uv_mutex_t writer_queue_mutex;
static uv_cond_t writer_queue_cond;
static writer_task_t *writer_queue_head = NULL;
static writer_task_t *writer_queue_tail = NULL;
static volatile int writer_stop = 0; // Use volatile for multi-threaded access.
static uv_thread_t writer_thread;

static uv_mutex_t completion_mutex;
static completion_item_t *completion_head = NULL;
static completion_item_t *completion_tail = NULL;
static uv_async_t completion_async;

/* --- Queue Helper Functions (Locked Operations) --- */
static void writer_queue_push_locked(writer_task_t *task) {
  task->next = NULL;
  if (writer_queue_tail) {
    writer_queue_tail->next = task;
    writer_queue_tail = task;
  } else {
    writer_queue_head = writer_queue_tail = task;
  }
}

static writer_task_t *writer_queue_pop_locked(void) {
  writer_task_t *t = writer_queue_head;
  if (t) {
    writer_queue_head = t->next;
    if (!writer_queue_head)
      writer_queue_tail = NULL;
    t->next = NULL;
  }
  return t;
}

static void completion_push_locked(completion_item_t *it) {
  it->next = NULL;
  if (completion_tail) {
    completion_tail->next = it;
    completion_tail = it;
  } else {
    completion_head = completion_tail = it;
  }
}

static completion_item_t *completion_pop_locked(void) {
  completion_item_t *t = completion_head;
  if (t) {
    completion_head = t->next;
    if (!completion_head)
      completion_tail = NULL;
    t->next = NULL;
  }
  return t;
}

/* Writer thread main loop */
static void writer_thread_main(void *arg) {
  (void)arg;
  while (1) {
    uv_mutex_lock(&writer_queue_mutex);
    while (!writer_stop && writer_queue_head == NULL) {
      uv_cond_wait(&writer_queue_cond, &writer_queue_mutex);
    }
    if (writer_stop && writer_queue_head == NULL) {
      uv_mutex_unlock(&writer_queue_mutex);
      break;
    }
    writer_task_t *task = writer_queue_pop_locked();
    uv_mutex_unlock(&writer_queue_mutex);

    if (!task)
      continue;

    // Execute the write request (e.g., LMDB transaction)
    api_response_t *r =
        api_exec(task->parsed->ast, task->ctx->client->handle.loop->data);
    parse_free_result(task->parsed);

    // Create completion item and enqueue it.
    completion_item_t *c = malloc(sizeof(completion_item_t));
    if (!c) {
      // Robust OOM handling.
      // Catastrophic failure. We can't notify the client.
      // We must still free the result from the DB engine to prevent a leak
      // here.
      log_fatal("writer_thread_main: OOM! Failed to allocate completion_item. "
                "Dropping task for client %lld. This will leak the work "
                "context and the client may hang.",
                task->ctx->client->client_id);
      free_api_response(r); // Free the result we got from the DB engine.
      free(task);           // Free the task container.
      continue;             // The original work_ctx will be leaked.
    }

    c->result = r; // May be NULL, indicating an error during execution.
    c->ctx = task->ctx;
    c->ctx->status = (c->ctx->result && c->ctx->result->is_ok)
                         ? WORK_STATUS_DONE_OK
                         : WORK_STATUS_DONE_ERR;

    uv_mutex_lock(&completion_mutex);
    completion_push_locked(c);
    uv_mutex_unlock(&completion_mutex);

    // Notify loop that a completion is available.
    // uv_async_send is guaranteed not to fail.
    uv_async_send(&completion_async);

    free(task); // Free writer_task container.
  }
}

/* Centralized cleanup logic for work contexts. */
static void decrement_work_and_cleanup(client_t *client) {
  client->work_refs--;
  if (client->open_handles == 0 && client->work_refs == 0) {
    log_info("Client %lld all work done and handles closed; freeing client.",
             client->client_id);
    active_connections--;
    free(client);
  }
}

/* Background worker: runs in libuv thread-pool */
static void work_cb(uv_work_t *req) {
  work_ctx_t *ctx = (work_ctx_t *)req;
  ctx->status = WORK_STATUS_DONE_ERR; // Default to error

  // All resources that need cleanup
  Queue *tokens = NULL;
  parse_result_t *parsed = NULL;
  writer_task_t *w = NULL;

  tokens = tok_tokenize(ctx->command);
  if (!tokens) {
    log_debug("Lexical error for client %lld", ctx->client->client_id);
    goto cleanup;
  }

  parsed = parse(tokens);
  if (parsed->type == OP_TYPE_ERROR) {
    log_debug("Parse error for client %lld", ctx->client->client_id);
    goto cleanup;
  }

  // Dispatch
  if (parsed->type == OP_TYPE_WRITE) {
    w = malloc(sizeof(writer_task_t));
    if (!w) {
      log_error("work_cb: OOM failed to allocate writer_task for client %lld",
                ctx->client->client_id);
      goto cleanup;
    }
    w->parsed = parsed;
    w->ctx = ctx;

    // The writer now owns the parsed result, so we NULL it out
    // to prevent it from being freed in our cleanup block.
    parsed = NULL;

    ctx->status = WORK_STATUS_PENDING_WRITE;

    uv_mutex_lock(&writer_queue_mutex);
    writer_queue_push_locked(w);
    uv_cond_signal(&writer_queue_cond);
    uv_mutex_unlock(&writer_queue_mutex);

    // For the write path, we are done. Free command and return.
    // The writer_task will be freed by the writer thread.
    free(ctx->command);
    ctx->command = NULL;
    return;
  } else { // Read path
    // ctx->result = execute_read_request(parsed);
    ctx->status = (ctx->result && ctx->result->is_ok) ? WORK_STATUS_DONE_OK
                                                      : WORK_STATUS_DONE_ERR;
    goto cleanup;
  }

cleanup:
  // Centralized cleanup point for all error and read paths.
  free(ctx->command);
  ctx->command = NULL;

  tok_clear_all(tokens);
  q_destroy(tokens);

  if (parsed) {
    parse_free_result(parsed);
  }
  // Note: 'w' is not freed here because its ownership is transferred.
}

/* after_work_cb: runs on the loop thread after work_cb completes */
static void after_work_cb(uv_work_t *req, int status) {
  work_ctx_t *ctx = (work_ctx_t *)req;
  client_t *client = ctx->client;

  if (ctx->status == WORK_STATUS_PENDING_WRITE) {
    // Do nothing. Completion will be handled via the async callback.
    // The ctx is still "in-flight".
    return;
  }

  // Handle reads or immediate errors from the worker thread.
  if (status != 0) { // System-level error from libuv.
    send_response(client, "ERROR: Internal server error\n");
  } else if (ctx->status != WORK_STATUS_DONE_OK) {
    send_response(client, "ERROR: Invalid command or execution failed\n");
  } else {
    // send_response(client, ctx->result);
    send_response(client, "OK read");
  }

  free(ctx->result);
  // Free work context and decrement client work reference.
  decrement_work_and_cleanup(client);
  free(ctx);
}

/* Completion async callback: runs on loop thread for completed writes. */
static void completion_async_cb(uv_async_t *handle) {
  (void)handle;
  while (1) {
    uv_mutex_lock(&completion_mutex);
    completion_item_t *it = completion_pop_locked();
    uv_mutex_unlock(&completion_mutex);

    if (!it)
      // This break statement ensures the loop terminates as soon as the
      // completion queue is empty
      break;

    work_ctx_t *ctx = it->ctx;
    client_t *client = ctx->client;

    if (it->result && it->result->is_ok) {
      // send_response(client, it->result);
      send_response(client, "OK\n");
    } else {
      send_response(client, "ERROR: Write operation failed\n");
    }

    free(it->result); // Free result string from writer.
    // Free work context and decrement client work reference.
    decrement_work_and_cleanup(client);
    free(ctx);
    free(it); // Free the completion item container.
  }
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

  work_ctx_t *ctx = calloc(1, sizeof(work_ctx_t));
  if (!ctx) {
    send_response(client, "ERROR: Internal server error (OOM)\n");
    return;
  }

  ctx->command = malloc(cmd_len + 1);
  if (!ctx->command) {
    free(ctx);
    send_response(client, "ERROR: Internal server error (OOM)\n");
    return;
  }
  memcpy(ctx->command, command, cmd_len + 1);

  ctx->client = client;
  ctx->req.data = ctx;
  client->work_refs++; // This work context now holds a reference to the client.

  int rc = uv_queue_work(loop, &ctx->req, work_cb, after_work_cb);
  if (rc != 0) {
    log_error("uv_queue_work failed: %s", uv_strerror(rc));
    // Rollback
    client->work_refs--;
    free(ctx->command);
    free(ctx);
    send_response(client, "ERROR: Failed to queue work\n");
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
      log_error(
          "Client %lld sent command exceeding MAX_COMMAND_LENGTH. Closing.",
          client->client_id);
      send_response(client, "ERROR: Command too long\n");
      close_client_connection(client);
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
    log_error("Client %lld buffer full without a newline. Closing.",
              client->client_id);
    send_response(client, "ERROR: Command buffer overflow\n");
    close_client_connection(client);
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
    log_trace("Read %zd bytes from client %lld", nread, client->client_id);
    uv_timer_again(&client->timeout_timer); // Reset idle timer.
    // Process any complete commands in the buffer
    process_data_buffer(client);
  } else if (nread < 0) {
    if (nread != UV_EOF) {
      log_error("Read error on client %lld: %s. Closing connection.",
                client->client_id, uv_strerror(nread));
    }
    close_client_connection(client);
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
    log_error("New connection error: %s", uv_strerror(status));
    return;
  }

  uv_loop_t *loop = server->loop;

  if (active_connections >= MAX_CONCURRENT_CONNECTIONS) {
    log_warn("Max connections reached (%d). Rejecting new connection.",
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
    log_fatal("Failed to allocate memory for new client");
    return; // Cannot recover.
  }

  // TODO: consider using UUID for client id
  client->client_id = ++next_client_id;
  uv_tcp_init(loop, &client->handle);
  client->handle.data = client; // Link client state to the handle
  client->open_handles = 1;
  client->connected = 1;

  // Accept the connection
  if (uv_accept(server, (uv_stream_t *)&client->handle) == 0) {
    active_connections++;
    log_info("Client %lld connected. Total connections: %d", client->client_id,
             active_connections);

    // --- Initialize the timer handle and increment the handle counter ---
    uv_timer_init(loop, &client->timeout_timer);
    client->timeout_timer.data = client;
    client->open_handles++;
    uv_timer_start(&client->timeout_timer, on_timeout, CONNECTION_IDLE_TIMEOUT,
                   0);

    // Start reading data from the client
    uv_read_start((uv_stream_t *)&client->handle, alloc_buffer, on_read);
  } else {
    log_error("Failed to accept client connection.");
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
static void close_walk_cb(uv_handle_t *handle, void *arg) {
  (void)arg;
  if (handle != (uv_handle_t *)&server_handle &&
      handle != (uv_handle_t *)&signal_handle &&
      handle != (uv_handle_t *)&completion_async) {
    if (!uv_is_closing(handle)) {
      log_debug("Shutdown: closing handle of type %s",
                uv_handle_type_name(handle->type));
      uv_close(handle, on_close);
    }
  }
}

// A function to handle all shutdown logic.
static void initiate_shutdown(void) {
  // Use a static flag to ensure this only runs once.
  static bool shutting_down = false;
  if (shutting_down)
    return;
  shutting_down = true;

  log_warn("Shutdown initiated...");

  uv_loop_t *loop = server_handle.loop;

  // Close the signal handle so the loop can exit cleanly.
  uv_close((uv_handle_t *)&signal_handle, NULL);

  // Stop accepting new connections.
  uv_close((uv_handle_t *)&server_handle, NULL);

  // Signal the writer thread to stop.
  uv_mutex_lock(&writer_queue_mutex);
  writer_stop = 1;
  uv_cond_signal(&writer_queue_cond);
  uv_mutex_unlock(&writer_queue_mutex);

  // Close all client-related handles.
  uv_walk(loop, close_walk_cb, NULL);

  // Close the async handle to allow the loop to exit.
  uv_close((uv_handle_t *)&completion_async, NULL);
}

/**
 * @brief Callback for handling SIGINT (Ctrl+C) for graceful shutdown.
 */
static void on_signal(uv_signal_t *handle, int signum) {
  (void)signum;
  log_warn("SIGINT received, initiating graceful shutdown...");

  // Stop the signal handler to prevent multiple shutdown signals
  uv_signal_stop(handle);
  initiate_shutdown();
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
 * @param loop Main event loop. `loop->data` is set to initialized database.
 */
void start_server(const char *host, int port, uv_loop_t *loop) {
  uv_mutex_init(&writer_queue_mutex);
  uv_cond_init(&writer_queue_cond);
  uv_mutex_init(&completion_mutex);
  uv_async_init(loop, &completion_async, completion_async_cb);

  uv_thread_create(&writer_thread, writer_thread_main, NULL);

  uv_signal_init(loop, &signal_handle);
  uv_signal_start(&signal_handle, on_signal, SIGINT);

  struct sockaddr_in addr;
  uv_ip4_addr(host, port, &addr);

  uv_tcp_init(loop, &server_handle);
  uv_tcp_bind(&server_handle, (const struct sockaddr *)&addr, 0);

  int r = uv_listen((uv_stream_t *)&server_handle, SERVER_BACKLOG,
                    on_new_connection);
  if (r) {
    log_fatal("Listen error: %s", uv_strerror(r));
    return;
  }

  log_info("🚀 Database server started on %s:%d", host, port);
  log_info("Configuration: max_conn=%d, timeout=%dms, max_cmd_len=%d",
           MAX_CONCURRENT_CONNECTIONS, CONNECTION_IDLE_TIMEOUT,
           MAX_COMMAND_LENGTH);

  // This call blocks until all handles are closed
  uv_run(loop, UV_RUN_DEFAULT);

  // --- Shutdown Sequence ---
  log_info("Event loop stopped. Finalizing shutdown...");

  // Wait for writer thread to finish its work and exit.
  uv_thread_join(&writer_thread);
  log_info("Writer thread joined.");

  // Ensure all close callbacks are processed.
  uv_run(loop, UV_RUN_NOWAIT);

  // Destroy all synchronization primitives.
  uv_mutex_destroy(&writer_queue_mutex);
  uv_cond_destroy(&writer_queue_cond);
  uv_mutex_destroy(&completion_mutex);

  // Close the loop itself.
  int close_status = uv_loop_close(loop);
  if (close_status != 0) {
    log_warn("uv_loop_close failed with status %d: %s. Some handles were "
             "likely not closed.",
             close_status, uv_strerror(close_status));
    // Forcibly clean up remaining handles if any exist.
    uv_walk(loop, (uv_walk_cb)uv_close, NULL);
    uv_run(loop, UV_RUN_ONCE); // allow close callbacks to run
    uv_loop_close(loop);       // try again
  }

  log_info("Server shut down cleanly.");
}