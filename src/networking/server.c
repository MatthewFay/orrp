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
#include "log.h"
#include "uv.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// --- Server Configuration ---
// TODO: Load from a config file
#define MAX_CONCURRENT_CONNECTIONS 1024 // Max number of clients
#define CONNECTION_IDLE_TIMEOUT 600000  // 10 minutes in milliseconds
#define MAX_COMMAND_LENGTH 8192         // 8KB max command size
#define READ_BUFFER_SIZE 65536          // 64KB per-client read buffer

// --- Globals ---
static uv_loop_t *loop;
static uv_tcp_t server_handle;    // Main server handle
static uv_signal_t signal_handle; // Signal handler for SIGINT
static int active_connections = 0;

// --- Client State Structure ---
// We create one of these for each connected client.
typedef struct {
  uv_tcp_t handle;
  uv_timer_t timeout_timer;
  char read_buffer[READ_BUFFER_SIZE];
  int buffer_len;
  int client_id; // Simple counter for logging
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
} write_req_t;

/**
 * @brief Callback function for when a client's idle timer fires.
 *
 * @param timer The timer handle that fired.
 */
void on_timeout(uv_timer_t *timer) {
  client_t *client = timer->data;
  log_warn("Client %d timed out due to inactivity. Closing connection.",
           client->client_id);
  uv_close((uv_handle_t *)&client->handle, on_close);
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
 * @param handle The handle that was closed.
 */
void on_close(uv_handle_t *handle) {
  client_t *client = handle->data;
  // During shutdown, client can be NULL if a timer is closed after its client
  if (client) {
    log_info("Client %d connection closed.", client->client_id);
    active_connections--;
    // Set data to NULL to prevent double-free if timer's on_close runs second
    client->handle.data = NULL;
    client->timeout_timer.data = NULL;
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
  write_req_t *wr = (write_req_t *)malloc(sizeof(write_req_t));
  if (!wr) {
    log_fatal("Failed to allocate write request");
    return;
  }

  size_t len = strlen(response);
  // IMPORTANT: The buffer for uv_write must exist until the on_write callback
  // fires. So we allocate memory for the response and copy it.
  char *write_buf = malloc(len);
  if (!write_buf) {
    log_fatal("Failed to allocate write buffer");
    free(wr);
    return;
  }
  memcpy(write_buf, response, len);

  wr->buf = uv_buf_init(write_buf, len);
  wr->req.data = write_buf; // Store buffer pointer to free it later

  int r =
      uv_write(&wr->req, (uv_stream_t *)&client->handle, &wr->buf, 1, on_write);
  if (r) {
    log_error("uv_write failed: %s", uv_strerror(r));
    free(wr);
    free(write_buf);
  }
}

/**
 * @brief Callback function for when a write operation completes.
 * The callback for send_response. After libuv finishes writing
 * our data to the socket, it calls this function so we can free
 * the memory we allocated for the write request and the message buffer.
 *
 * @param req The write request.
 * @param status Status of the write operation.
 */
void on_write(uv_write_t *req, int status) {
  if (status) {
    log_error("Write error: %s", uv_strerror(status));
  }
  // Free the buffer that we allocated in send_response
  free(req->data);
  // Free the write request struct itself
  free(req);
}

/**
 * @brief Processes a single, complete command from a client.
 * Core logic. It takes a single, complete command string and
 * is where we call our tokenizer and parser.
 *
 * @param client The client who sent the command.
 * @param command The null-terminated command string.
 */
void process_one_command(client_t *client, char *command) {
  // Trim trailing newline/carriage return if present
  command[strcspn(command, "\r\n")] = 0;

  log_debug("Client %d sent command: '%s'", client->client_id, command);

  // ===================================================================
  // TODO: Integrate tokenizer and parser.
  // The result of database operation determines the response.
  // ===================================================================

  // For now, we just send a simple acknowledgement.
  if (strlen(command) > 0) {
    send_response(client, "OK\n");
  }
}

/**
 * @brief Scans the client's read buffer for complete commands and processes
 * them.
 *
 * A command is considered complete if it's terminated by a newline ('\n').
 * This function handles multiple commands in a single read (pipelining).
 *
 * This function is the "command framer." It scans the client's read buffer
 * for newline characters (\n). For each complete command it finds,
 * it calls `process_one_command` and then cleans up the buffer.
 *
 * It's crucial for handling multiple commands sent in a single network
 * packet (pipelining).
 *
 * @param client The client whose buffer needs processing.
 */
void process_data_buffer(client_t *client) {
  char *buffer_start = client->read_buffer;
  char *newline_pos;

  // Find the first newline character in the buffer
  while ((newline_pos = memchr(buffer_start, '\n',
                               client->buffer_len -
                                   (buffer_start - client->read_buffer))) !=
         NULL) {
    // Calculate the length of the command
    int command_len = newline_pos - buffer_start;

    // Check against max command length
    if (command_len > MAX_COMMAND_LENGTH) {
      log_error("Client %d sent command exceeding MAX_COMMAND_LENGTH (%d > "
                "%d). Closing connection.",
                client->client_id, command_len, MAX_COMMAND_LENGTH);
      send_response(client, "ERROR: Command too long\n");
      uv_close((uv_handle_t *)&client->handle, on_close);
      return;
    }

    // Temporarily null-terminate the command to treat it as a C string
    *newline_pos = '\0';
    process_one_command(client, buffer_start);

    // Advance the buffer start past the processed command and the newline
    buffer_start = newline_pos + 1;
  }

  // After processing all complete commands, check if there's any leftover data
  int remaining_len = client->buffer_len - (buffer_start - client->read_buffer);
  if (remaining_len > 0 && buffer_start != client->read_buffer) {
    // Move the partial command to the beginning of the buffer for the next read
    memmove(client->read_buffer, buffer_start, remaining_len);
  }
  client->buffer_len = remaining_len;

  // Safety check: if the buffer is full and has no newline, something is wrong.
  if (client->buffer_len == READ_BUFFER_SIZE) {
    log_error("Client %d buffer is full without a newline. Possible DoS or "
              "malformed command. Closing.",
              client->client_id);
    send_response(client, "ERROR: Command buffer overflow\n");
    uv_close((uv_handle_t *)&client->handle, on_close);
  }
}

/**
 * @brief Callback function for when data is read from a client socket.
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
  (void)buf;
  client_t *client = stream->data;

  if (nread > 0) {
    // Data received, update buffer length and reset idle timer
    client->buffer_len += nread;
    log_trace("Read %zd bytes from client %d", nread, client->client_id);
    uv_timer_again(&client->timeout_timer); // Reset the idle timer

    // Process any complete commands in the buffer
    process_data_buffer(client);

  } else if (nread < 0) {
    if (nread != UV_EOF) {
      log_error("Read error: %s", uv_strerror(nread));
    }
    // Client disconnected (EOF) or read error, close the connection
    uv_close((uv_handle_t *)stream, on_close);
  }
  // nread == 0 is possible, just means nothing to read right now.
}

/**
 * @brief Callback function for when a new client connects to the server.
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

  // Check if we have reached the connection limit
  if (active_connections >= MAX_CONCURRENT_CONNECTIONS) {
    log_warn("Max connections reached (%d). Rejecting new connection.",
             MAX_CONCURRENT_CONNECTIONS);
    // To reject, we must still accept, then immediately close.
    uv_tcp_t *temp_client = (uv_tcp_t *)malloc(sizeof(uv_tcp_t));
    if (temp_client) {
      uv_tcp_init(loop, temp_client);
      if (uv_accept(server, (uv_stream_t *)temp_client) == 0) {
        // Use a dedicated close callback that just frees the handle
        uv_close((uv_handle_t *)temp_client, on_rejected_close);
      } else {
        free(temp_client);
      }
    }
    return;
  }

  // Allocate memory for the new client's state
  client_t *client = (client_t *)calloc(1, sizeof(client_t));
  if (!client) {
    log_fatal("Failed to allocate memory for new client");
    return;
  }
  client->client_id = active_connections; // Simple ID for logging

  // Initialize the client's TCP handle
  uv_tcp_init(loop, &client->handle);
  client->handle.data = client; // Link client state to the handle

  // Accept the connection
  if (uv_accept(server, (uv_stream_t *)&client->handle) == 0) {
    active_connections++;
    log_info("New client connected. Total connections: %d", active_connections);

    // Initialize and start the idle timeout timer
    client->timeout_timer.data = client;
    uv_timer_init(loop, &client->timeout_timer);
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
 * @brief Iterates over all active handles and closes them.
 * This function is used during graceful shutdown to close all client
 * connections and their associated timers.
 * @param handle The handle to inspect.
 * @param arg User-provided argument (not used here).
 */
static void close_walk_cb(uv_handle_t *handle, void *arg) {
  (void)arg;
  // We want to close all handles that are not the main server listener
  // or the signal handler, as they are handled separately.
  if (handle != (uv_handle_t *)&server_handle &&
      handle != (uv_handle_t *)&signal_handle) {
    if (!uv_is_closing(handle)) {
      log_debug("Shutdown: closing handle of type %s",
                uv_handle_type_name(handle->type));
      // on_close is safe for both client TCP and timer handles
      uv_close(handle, on_close);
    }
  }
}

/**
 * @brief Callback for handling SIGINT (Ctrl+C).
 * This function initiates the graceful shutdown procedure.
 * @param handle The signal handle.
 * @param signum The signal number (SIGINT).
 */
static void on_signal(uv_signal_t *handle, int signum) {
  (void)signum;
  log_warn("SIGINT received, initiating graceful shutdown...");

  // 1. Stop the signal handler to prevent multiple shutdown signals
  uv_signal_stop(handle);

  // 2. Stop accepting new connections by closing the server handle
  uv_close((uv_handle_t *)&server_handle, NULL);

  // 3. Close all other active handles (clients, timers)
  uv_walk(loop, close_walk_cb, NULL);
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
 */
void start_server(const char *host, int port) {
  loop = uv_default_loop();
  uv_tcp_init(loop, &server_handle);

  // Initialize and start the signal handler for SIGINT
  uv_signal_init(loop, &signal_handle);
  uv_signal_start(&signal_handle, on_signal, SIGINT);

  struct sockaddr_in addr;
  uv_ip4_addr(host, port, &addr);

  uv_tcp_bind(&server_handle, (const struct sockaddr *)&addr, 0);

  int r = uv_listen((uv_stream_t *)&server_handle, 128, on_new_connection);
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

  // This part is only reached when the loop is stopped
  // Ensure all events are processed before closing the loop
  uv_run(loop, UV_RUN_ONCE);
  uv_loop_close(loop);
  log_info("Server shut down cleanly.");
}