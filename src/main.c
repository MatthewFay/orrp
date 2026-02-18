#include "core/ebr.h"
#include "engine/api.h"
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

  ebr_epoch_global_init();

  uv_loop_t *loop = uv_default_loop();
  if (!loop) {
    LOG_ACTION_FATAL(ACT_SYSTEM_INIT,
                     "err=\"unable to initialize event loop\"");
    return -1;
  }

  LOG_ACTION_INFO(ACT_SYSTEM_INIT, "component=engine");

  bool r = eng_init();
  if (!r) {
    LOG_ACTION_FATAL(ACT_SYSTEM_INIT,
                     "component=engine err=\"initialization failed\"");
    return -1;
  }

  LOG_ACTION_INFO(ACT_SYSTEM_INIT, "component=engine status=complete");

  const char *host = "0.0.0.0"; // Listen on all available network interfaces
  int port = 7878;              // The port for the database

  LOG_ACTION_INFO(ACT_SYSTEM_INIT, "component=server host=\"%s\" port=%d", host,
                  port);

  // This function will block and run the server until the process is
  // terminated.
  start_server(host, port, loop);

  LOG_ACTION_INFO(ACT_SYSTEM_SHUTDOWN, "component=engine");

  api_stop_eng();

  LOG_ACTION_INFO(ACT_SYSTEM_SHUTDOWN, "component=engine status=complete");

  log_global_shutdown();

  return 0;
}
