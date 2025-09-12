#ifndef BITMAP_CACHE_H
#define BITMAP_CACHE_H

#include "cache_entry.h"
#include "ck_epoch.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/container.h"
#include <stdbool.h>
#include <stdint.h>

#define NUM_SHARDS 16 // Power of 2 for fast modulo

// The THREAD-LOCAL epoch record pointer.
// Each thread gets its own "ID badge".
extern _Thread_local ck_epoch_record_t *bitmap_cache_thread_epoch_record;

// Global epoch for entire cache.
extern ck_epoch_t bitmap_cache_g_epoch;

typedef struct bitmap_cache_handle_s bitmap_cache_handle_t;

typedef struct bitmap_cache_key_s {
  const char *container_name;
  eng_user_dc_db_type_t db_type;
  const db_key_t *db_key;
} bitmap_cache_key_t;

// Dirty list snapshot for flushing
typedef struct bm_cache_dirty_snapshot_s {
  uint32_t shard_id;
  bm_cache_value_entry_t *dirty_entries; // Linked list
  uint32_t entry_count;
} bm_cache_dirty_snapshot_t;

typedef struct bm_cache_flush_batch_s {
  bm_cache_dirty_snapshot_t shards[NUM_SHARDS];
  uint32_t total_entries;
} bm_cache_flush_batch_t;

bool bitmap_cache_init(void);
bool bitmap_cache_shutdown(void);

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

int bm_cache_prepare_flush_batch(bm_cache_flush_batch_t *batch);
int bm_cache_complete_flush_batch(bm_cache_flush_batch_t *batch, bool success);
void bm_cache_free_flush_batch(bm_cache_flush_batch_t *batch);

// Used for epoch reclamation
void bm_cache_dispose(ck_epoch_entry_t *entry);

#endif // BITMAP_CACHE_H