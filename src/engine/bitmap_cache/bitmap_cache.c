#include "bitmap_cache.h"
#include "cache_queue_msg.h"
#include "cache_shard.h"
#include "ck_epoch.h"
#include "ck_pr.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "core/hash.h"
#include "engine/bitmap_cache/cache_entry.h"
#include "engine/bitmap_cache/cache_queue_consumer.h"
#include "uv.h"
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SHARD_MASK (NUM_SHARDS - 1)
#define NUM_CONSUMER_THREADS 4
#define SHARDS_PER_CONSUMER (NUM_SHARDS / NUM_CONSUMER_THREADS)

#define MAX_CACHE_KEY_SIZE 256
#define MAX_ENQUEUE_ATTEMPTS 3

_Thread_local ck_epoch_record_t *bitmap_cache_thread_epoch_record = NULL;

ck_epoch_t bitmap_cache_g_epoch;

typedef struct bitmap_cache_handle_s {
  ck_epoch_section_t epoch_section;
} bitmap_cache_handle_t;

typedef struct bm_cache_s {
  bm_cache_shard_t shards[NUM_SHARDS];
  bm_cache_consumer_t consumers[NUM_CONSUMER_THREADS];
  bool is_initialized;
} bm_cache_t;

// =============================================================================
// --- Epoch Reclamation Callback ---
// =============================================================================

CK_EPOCH_CONTAINER(bitmap_t, epoch_entry, get_bitmap_from_epoch)

void bm_cache_dispose(ck_epoch_entry_t *entry) {
  bitmap_t *bm = get_bitmap_from_epoch(entry);
  bitmap_free(bm);
}

// Get shard index from key
static int _get_shard_index(const char *key) {
  return xxhash64(key, strlen(key), 0) & SHARD_MASK;
}

// Get unique cache key for entry
static bool _get_cache_key(char *buffer, size_t buffer_size,
                           const char *container_name,
                           eng_user_dc_db_type_t db_type,
                           const db_key_t db_key) {
  int r = -1;
  if (db_key.type == DB_KEY_INTEGER) {
    r = snprintf(buffer, buffer_size, "%s:%d:%u", container_name, (int)db_type,
                 db_key.key.i);
  } else if (db_key.type == DB_KEY_STRING) {
    r = snprintf(buffer, buffer_size, "%s:%d:%s", container_name, (int)db_type,
                 db_key.key.s);
  } else {
    return false;
  }
  if (r < 0 || (size_t)r >= buffer_size) {
    return false;
  }
  return true;
}

// Global instance of our cache
static bm_cache_t g_cache;

static bool _enqueue_msg(const char *cache_key, bm_cache_queue_msg_t *msg) {
  int s_idx = _get_shard_index(cache_key);
  bool enqueued = false;
  for (int i = 0; i < MAX_ENQUEUE_ATTEMPTS; i++) {
    if (shard_enqueue_msg(&g_cache.shards[s_idx], msg)) {
      enqueued = true;
      break;
    }
    // Ring buffer is full
    ck_pr_stall();
    // might add a short sleep here
  }
  return enqueued;
}

// --- Public API Implementations ---

bool bitmap_cache_init(void) {
  for (int i = 0; i < NUM_SHARDS; ++i) {
    if (!bm_init_shard(&g_cache.shards[i])) {
      return false;
    }
  }

  for (int i = 0; i < NUM_CONSUMER_THREADS; i++) {
    bm_cache_consumer_config_t config = {.shards = g_cache.shards,
                                         .shard_start = i * SHARDS_PER_CONSUMER,
                                         .shard_count = SHARDS_PER_CONSUMER,
                                         .consumer_id = i};

    if (!bm_cache_consumer_start(&g_cache.consumers[i], &config)) {
      return false;
    }
  }

  g_cache.is_initialized = true;
  return true;
}

bool bitmap_cache_ingest(const bitmap_cache_key_t *key, uint32_t value,
                         const char *idempotency_key) {
  char cache_key[MAX_CACHE_KEY_SIZE];
  (void)idempotency_key;
  if (!key || !key->container_name)
    return false;
  if (!_get_cache_key(cache_key, sizeof(cache_key), key->container_name,
                      key->db_type, *key->db_key)) {
    return false;
  }

  bm_cache_queue_msg_t *msg =
      bm_cache_create_msg(BM_CACHE_ADD_VALUE, key, value, cache_key);
  if (!msg) {
    return false;
  }

  if (!_enqueue_msg(cache_key, msg)) {
    bm_cache_free_msg(msg);
    return false;
  }
  // notify queue consumer using libuv
  return true;
}

bitmap_cache_handle_t *bitmap_cache_query_begin(void) {
  bitmap_cache_handle_t *h = malloc(sizeof(bitmap_cache_handle_t));
  if (!h)
    return NULL;
  if (bitmap_cache_thread_epoch_record == NULL) {
    ck_epoch_register(&bitmap_cache_g_epoch, bitmap_cache_thread_epoch_record,
                      NULL);
  }
  ck_epoch_begin(bitmap_cache_thread_epoch_record, &h->epoch_section);
  return h;
}

void bitmap_cache_query_end(bitmap_cache_handle_t *handle) {
  if (!handle)
    return;
  ck_epoch_end(bitmap_cache_thread_epoch_record, &handle->epoch_section);
  free(handle);
}

int bm_cache_prepare_flush_batch(bm_cache_flush_batch_t *batch) {
  batch->total_entries = 0;

  for (int i = 0; i < NUM_SHARDS; i++) {
    bm_cache_shard_t *shard = &g_cache.shards[i];

    uv_mutex_lock(&shard->dirty_list_lock);

    // Swap dirty list (atomic operation)
    batch->shards[i].dirty_entries = shard->dirty_head;
    // batch->shards[i].entry_count = shard->dirty_count;
    batch->shards[i].shard_id = i;

    // Reset shard dirty list
    shard->dirty_head = NULL;
    // shard->dirty_count = 0;

    uv_mutex_unlock(&shard->dirty_list_lock);

    batch->total_entries += batch->shards[i].entry_count;
  }

  return 0;
}

int bm_cache_complete_flush_batch(bm_cache_flush_batch_t *batch, bool success) {
  if (!success) {
    // On failure, re-add entries to dirty lists
    for (int i = 0; i < NUM_SHARDS; i++) {
      if (batch->shards[i].dirty_entries) {
        bm_cache_shard_t *shard = &g_cache.shards[i];
        uv_mutex_lock(&shard->dirty_list_lock);

        // Re-add to dirty list (prepend for efficiency)
        bm_cache_entry_t *last = batch->shards[i].dirty_entries;
        while (last->dirty_next)
          last = last->dirty_next;

        last->dirty_next = shard->dirty_head;
        shard->dirty_head = batch->shards[i].dirty_entries;
        // shard->dirty_count += batch->shards[i].entry_count;

        uv_mutex_unlock(&shard->dirty_list_lock);
      }
    }
  } else {
    // On success, entries can be safely freed or marked clean
    // bm_cache_free_flush_batch(batch);
  }

  return 0;
}

// TODO: This is tricky - need to flush in-flight and in-memory data to disk
bool bitmap_cache_shutdown(void) {
  bool success = true;
  for (int i = 0; i < NUM_CONSUMER_THREADS; i++) {
    if (!bm_cache_consumer_stop(&g_cache.consumers[i])) {
      success = false;
    }
  }

  return success;
}

// void bm_cache_mark_dirty(bm_cache_entry_t *entry) {
//   if (!entry)
//     return;
//   if (!entry->is_dirty) {
//     entry->is_dirty = true;
//     _eng_cache_add_to_dirty_list(entry);
//   }
//   entry->current_version++;
// }

// void eng_cache_remove_from_dirty_list(bm_cache_shard_t *shard,
//                                       bm_cache_entry_t *entry) {
//   uv_mutex_lock(&shared->dirty_list_lock);

//   if (entry->dirty_prev) {
//     entry->dirty_prev->dirty_next = entry->dirty_next;
//   } else {
//     g_cache.dirty_head = entry->dirty_next;
//   }

//   if (entry->dirty_next) {
//     entry->dirty_next->dirty_prev = entry->dirty_prev;
//   } else {
//     g_cache.dirty_tail = entry->dirty_prev;
//   }

//   entry->dirty_prev = NULL;
//   entry->dirty_next = NULL;

//   uv_mutex_unlock(&shard->dirty_list_lock);
// }

// // Lock and swap pattern - very fast - used by background writer
// bm_cache_entry_t *bm_cache_swap_dirty_list(bm_cache_shard_t *shard) {
//   uv_mutex_lock(&shard->dirty_list_lock);
//   bm_cache_entry_t *head = shard->dirty_head;
//   shard->dirty_head = NULL;
//   shard->dirty_tail = NULL;
//   uv_mutex_unlock(&shard->dirty_list_lock);
//   return head;
// }

// static bm_cache_entry_t *create_entry(const char *cache_key, bitmap_t
// *bitmap) {
//   bm_cache_entry_t *entry = malloc(sizeof(bm_cache_entry_t));
//   if (!entry)
//     return NULL;

//   entry->cache_key = strdup(key);
//   if (!entry->key) {
//     free(entry);
//     return NULL;
//   }

//   entry->bitmap = bitmap;
//   entry->ref_count = 1;
//   entry->is_dirty = false;
//   entry->lru_prev = entry->lru_next = NULL;

//   return entry;
// }
