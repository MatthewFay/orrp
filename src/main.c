#include <stdio.h>
#include "db.h"

/**
 * @brief Main entry point for the database server application.
 *
 * @param argc The number of command-line arguments.
 * @param argv An array of command-line argument strings.
 * @return int Returns 0 on successful execution.
 */
int main(int argc, char *argv[]) {
    // This is a placeholder for the main application logic.
    // In the future, this will initialize the server,
    // parse arguments, and start the main event loop.
    
    // Suppress unused parameter warnings for now
    (void)argc;
    (void)argv;

    print_hello_message();
    
    return 0;
}

/**
 * @brief Prints the initial "Hello, World!" message.
 *
 * This function serves as a simple demonstration that the application
 * can be compiled and run successfully.
 */
void print_hello_message(void) {
    printf("Hello from the database!\n");
}
