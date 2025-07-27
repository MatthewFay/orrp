#ifndef CORE_DB_H
#define CORE_DB_H

#include <stdbool.h>
#include <stddef.h> // For size_t

// Forward declaration for your opaque database type
typedef struct db_s db_t;

// Function to initialize and open the database environment
db_t *db_open(const char *path, size_t map_size);

// Function to put a key-value pair into the database
bool db_put(db_t *db, const char *key, const char *value);

// Function to get a value by key from the database
char *db_get(db_t *db, const char *key);

// Function to close and free the database environment
void db_close(db_t *db);

#endif // CORE_DB_H