#ifndef SERVER_H
#define SERVER_H

/**
 * @brief Starts the database server and runs the libuv event loop.
 *
 * This function initializes the TCP server, binds it to the specified host and
 * port, and starts listening for incoming connections. It will block until the
 * event loop is stopped.
 *
 * @param host The IP address to bind to. Use "0.0.0.0" to listen on all
 * interfaces.
 * @param port The port number to listen on.
 */
void start_server(const char *host, int port);

#endif // SERVER_H
