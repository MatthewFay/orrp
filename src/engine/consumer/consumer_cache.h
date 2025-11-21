#ifndef consumer_CACHE_H
#define consumer_CACHE_H

/**
Consumer cache public Api.
Used by query threads to read from the cache */

#include "consumer_cache_internal.h"
#include "core/bitmaps.h"
#include <stdbool.h>
#include <stdint.h>

/**
 * @brief Begins a query session.
 *
 * This function marks the start of a safe, read-only critical section.
 */
void consumer_cache_query_begin();

/**
 * @brief Retrieves a read-only bitmap using a query handle.
 *
 * The returned pointer is guaranteed to be valid until
 * consumer_cache_query_end() is called on the handle.
 *
 * @return A CONST pointer to the bitmap, or NULL if not found.
 */
const bitmap_t *consumer_cache_get_bm(consumer_cache_t *consumer_cache,
                                      const char *ser_db_key);

uint32_t consumer_cache_get_u32(consumer_cache_t *consumer_cache,
                                const char *ser_db_key);

/**
 * @brief Ends a query session.
 *
 * This function releases the epoch protection.
 * After this call, any pointers obtained with the handle are no longer
 * guaranteed to be valid.
 *
 * @param handle The handle to end.
 */
void consumer_cache_query_end();

#endif // consumer_CACHE_H