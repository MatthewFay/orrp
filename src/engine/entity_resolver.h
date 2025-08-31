#ifndef ENTITY_RESOLVER_H
#define ENTITY_RESOLVER_H

#include "container.h"
#include "context.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct er_dirty_item_s er_dirty_item_t;

/**
 * @brief Initializes the entity resolver. Must be called once at server
 * startup.
 *
 * This function loads existing mappings from the system container into the
 * in-memory cache to "warm" it up.
 * @param ctx The global engine context.
 * @param capacity The maximum number of entities to hold in the LRU cache.
 */
void entity_resolver_init(eng_context_t *ctx, int capacity);

/**
 * @brief Destroys the entity resolver resources. Called on graceful shutdown.
 */
void entity_resolver_destroy(void);

/**
 * @brief Resolves a string entity ID to its corresponding integer ID.
 *
 * This is the primary transactional function. It handles caching, database
 * lookups, and the creation of new entities if the ID is not found.
 * This function is thread-safe.
 *
 * @param entity_id_str The string ID to resolve.
 * @param int_id_out A pointer to a uint32_t where the result will be stored.
 * @return True on success, false on failure.
 */
bool entity_resolver_resolve_id(eng_container_t *sys_c,
                                const char *entity_id_str,
                                uint32_t *int_id_out);

/**
 * @brief Resolves an integer entity ID to its corresponding string ID.
 *
 * This function handles caching and database lookups. If an integer ID
 * exists, its string counterpart must also exist.
 *
 * @param int_id The integer ID to resolve.
 * @param str_id_out A pointer to a const char* where the result will be stored.
 * @return True on success, false if the ID cannot be found (data
 * inconsistency).
 */
bool entity_resolver_resolve_string(eng_container_t *sys_c, uint32_t int_id,
                                    const char **str_id_out);

/**
 * @brief Gets the current list of new/dirty mappings for the background writer.
 *
 * This function uses a "lock-and-swap" pattern. The returned list is a
 * self-contained copy of the data needed for persistence and is completely
 * decoupled from the internal cache.
 *
 * @return A pointer to the head of a linked list of dirty items, or NULL if
 * empty. The caller is responsible for freeing this list with
 * entity_resolver_free_dirty_list().
 */
er_dirty_item_t *entity_resolver_get_dirty_mappings(void);

/**
 * @brief Frees the memory for a list of dirty items retrieved by the writer.
 * @param list The list to free.
 */
void entity_resolver_free_dirty_list(er_dirty_item_t *list);

#endif // ENTITY_RESOLVER_H
