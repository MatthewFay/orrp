#include "engine/engine.h"
#include "log.h"
#include "networking/server.h"
#include "uv.h"

// main.c
// Entry point to start the server.

int main() {
  // --- Initialize Logging ---
  // TODO: Log to file
  log_set_level(LOG_DEBUG);
  log_set_quiet(0); // Ensure logs are not quiet

  uv_loop_t *loop = uv_default_loop();
  if (!loop) {
    log_fatal("Unable to initialize event loop");
    return -1;
  }

  eng_context_t *ctx = eng_init();
  if (!ctx) {
    log_fatal("Unable to initialize database");
    return -1;
  }

  // Attach the engine context to the loop's data field (user data).
  loop->data = ctx;

  const char *host = "0.0.0.0"; // Listen on all available network interfaces
  int port = 7878;              // The port for the database

  // This function will block and run the server until the process is
  // terminated.
  start_server(host, port, loop);

  eng_close_ctx(ctx);

  return 0;
}
