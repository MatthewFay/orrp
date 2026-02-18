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
 * @brief Retrieves a read-only bitmap.
 *
 * IMPORTANT: Must call this within EBR critical section!
 *
 * @return A CONST pointer to the bitmap, or NULL if not found.
 */
const bitmap_t *consumer_cache_get_bm(consumer_cache_t *consumer_cache,
                                      const char *ser_db_key);

#endif // consumer_CACHE_H