#include "log.h"
#include "networking/server.h"

// main.c
// Entry point to start the server.

int main() {
  // --- Initialize Logging ---
  log_set_level(LOG_DEBUG);
  log_set_quiet(0); // Ensure logs are not quiet

  const char *host = "0.0.0.0"; // Listen on all available network interfaces
  int port = 7878;              // The port for our database

  // This function will block and run the server until the process is
  // terminated.
  start_server(host, port);

  return 0;
}
