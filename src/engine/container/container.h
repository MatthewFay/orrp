#ifndef CONTAINER_H
#define CONTAINER_H

#include "container_types.h"
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
 * Get or create a user container. Thread-safe
 *
 * @param name Container name
 * @return Result object with container or error details
 */
container_result_t container_get_or_create_user(const char *name);

/**
 * Get the system container
 */
container_result_t container_get_system(void);

/**
 * Release a User container. Thread-safe
 */
void container_release(eng_container_t *container);

/**
 * Get a database handle from a user container
 *
 * @param c Container (must be USER type)
 * @param db_type Type of database to get
 * @param db_out Output parameter for database handle
 * @return true on success, false on failure
 */
bool container_get_user_db_handle(eng_container_t *c,
                                  eng_dc_user_db_type_t db_type,
                                  MDB_dbi *db_out);

/**
 * Get a database handle from the system container
 *
 * @param c Container (must be SYSTEM type)
 * @param db_type Type of database to get
 * @param db_out Output parameter for database handle
 * @return true on success, false on failure
 */
bool container_get_system_db_handle(eng_container_t *c,
                                    eng_dc_sys_db_type_t db_type,
                                    MDB_dbi *db_out);

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