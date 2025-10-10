#include "engine/engine.h"
#include "log/log.h"
#include "networking/server.h"
#include "uv.h"

#define ZLOG_CONF_PATH "config/zlog.conf"

LOG_INIT(main);

// main.c
// Entry point to start the server.
int main() {
  int rc = log_global_init(ZLOG_CONF_PATH);
  if (rc == -1) {
    return -1;
  }

  log_init_main();
  if (!LOG_CATEGORY) {
    return -1;
  }

  uv_loop_t *loop = uv_default_loop();
  if (!loop) {
    LOG_FATAL("Unable to initialize event loop");
    return -1;
  }

  eng_context_t *ctx = eng_init();
  if (!ctx) {
    LOG_FATAL("Unable to initialize database engine");
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

  log_global_shutdown();

  return 0;
}
