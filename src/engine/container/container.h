#ifndef CONTAINER_H
#define CONTAINER_H

#include "container_types.h"
#include "lmdb.h"
#include <stdbool.h>
#include <stdint.h>

/**
 * Initialize the container subsystem
 * Must be called once at startup before any other container operations
 *
 * @param cache_capacity Maximum number of containers to keep in cache
 * @param data_dir Directory where container files are stored (e.g., "data")
 * @param container_size_bytes Initial Size of each container
 * @return true on success, false on failure
 */
bool container_init(size_t cache_capacity, const char *data_dir,
                    size_t container_size_bytes);

/**
 * Shutdown the container subsystem. Closes all containers and frees resources
 * Must be called once at shutdown
 */
void container_shutdown(void);

/**
 * Get user container. Thread-safe.
 * Set `create` to true to create if does not exist.
 * Optional sys_read_txn used during creation
 */
container_result_t container_get_user(const char *name, bool create,
                                      MDB_txn *sys_read_txn);

/**
 * Get the system container
 */
container_result_t container_get_system(void);

/**
 * Release a User container. Thread-safe
 */
void container_release(eng_container_t *container);

/**
 * Get a database handle from a container
 */
bool container_get_db_handle(eng_container_t *c, eng_container_db_key_t *db_key,
                             MDB_dbi *db_out);

/**
 * Free the contents of a database key
 */
void container_free_db_key_contents(eng_container_db_key_t *db_key);

#endif // CONTAINER_H