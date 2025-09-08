#ifndef BITMAP_CACHE_H
#define BITMAP_CACHE_H

#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/container.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct bitmap_cache_handle_s bitmap_cache_handle_t;

typedef struct bitmap_cache_key_s {
  const char *container_name;
  eng_user_dc_db_type_t db_type;
  const db_key_t *db_key;
} bitmap_cache_key_t;

bool bitmap_cache_init(void);
void bitmap_cache_shutdown(void);
void bitmap_cache_flush_all(void);

bool bitmap_cache_ingest(const bitmap_cache_key_t *key, uint32_t value,
                         const char *idempotency_key);

/**
 * @brief Begins a query session and returns a handle.
 *
 * This function marks the start of a safe, read-only critical section.
 *
 * @return A handle to be used for subsequent queries, or NULL on failure.
 */
bitmap_cache_handle_t *bitmap_cache_query_begin(void);

/**
 * @brief Retrieves a read-only bitmap using a query handle.
 *
 * The returned pointer is guaranteed to be valid until bitmap_cache_query_end()
 * is called on the handle.
 *
 * @param handle The handle obtained from bitmap_cache_query_begin().
 * @param key The key of the bitmap to query.
 * @return A CONST pointer to the bitmap, or NULL if not found.
 */
const bitmap_t *bitmap_cache_get_bitmap(bitmap_cache_handle_t *handle,
                                        const bitmap_cache_key_t *key);

/**
 * @brief Ends a query session.
 *
 * This function releases the epoch protection.
 * After this call, any pointers obtained with the handle are no longer
 * guaranteed to be valid.
 *
 * @param handle The handle to end.
 */
void bitmap_cache_query_end(bitmap_cache_handle_t *handle);

#endif // BITMAP_CACHE_H