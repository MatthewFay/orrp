#include "bitmap_cache.h"
#include "cache_queue_msg.h"
#include "cache_shard.h"
#include "ck_epoch.h"
#include "ck_ht.h"
#include "ck_pr.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "core/hash.h"
#include "uv.h"
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NUM_SHARDS 16 // Power of 2 for fast modulo
#define SHARD_MASK (NUM_SHARDS - 1)

#define MAX_CACHE_KEY_SIZE 256
#define MAX_ENQUEUE_ATTEMPTS 3

typedef struct bitmap_cache_handle_s {
  ck_epoch_section_t epoch_section;
} bitmap_cache_handle_t;

typedef struct bm_cache_s {
  bm_cache_shard_t shards[NUM_SHARDS];
} bm_cache_t;

// =============================================================================
// --- Epoch Reclamation Callbacks ---
// =============================================================================

// not sure yet who will handle reclamation - prob writer/flush thread
static void _free_value_entry(bm_cache_value_entry_t *value) {
  if (!value)
    return;
  bitmap_t *bm = atomic_load(value->bitmap);
  bitmap_free(bm);
  if (value->db_key.type == DB_KEY_STRING) {
    free((char *)value->db_key.key.s);
  }
  free(value->key_entry);
  free(value);
}

// Callback to free a Cache Entry and its associated Cache Value.
// This is used when an entry is evicted from the cache completely.
static void _entry_dispose_callback(void *p) {
  if (!p)
    return;
  bm_cache_key_entry_t *entry = p;
  ck_ht_entry_t *ht_entry =
      (ck_ht_entry_t *)((char *)entry - sizeof(uintptr_t));
  bm_cache_value_entry_t *value = ck_ht_entry_value(ht_entry);
  _free_value_entry(value);
  free(entry);
}

// Callback to free an old Cache Value
static void _old_value_dispose_callback(void *p) {
  if (!p)
    return;
  bm_cache_value_entry_t *value = p;
  _free_value_entry(value);
}

// =============================================================================
// --- LRU List Helpers ---
// =============================================================================

// Used to add an existing entry to front of LRU
static void _lru_add_to_front(bm_cache_shard_t *shard,
                              bm_cache_value_entry_t *entry) {
  if (!entry || entry == shard->lru_head)
    return;
  entry->lru_prev->lru_next = entry->lru_next;
  if (entry == shard->lru_tail) {
    shard->lru_tail = entry->lru_prev;
  } else {
    entry->lru_next->lru_prev = entry->lru_prev;
  }
  entry->lru_prev = NULL;
  entry->lru_next = shard->lru_head;
  shard->lru_head->lru_prev = entry;
  shard->lru_head = entry;
}

static void _lru_remove_entry(bm_cache_shard_t *shard,
                              bm_cache_value_entry_t *entry) {
  if (entry->lru_prev) {
    entry->lru_prev->lru_next = entry->lru_next;
  } else {
    shard->lru_head = entry->lru_next;
  }

  if (entry->lru_next) {
    entry->lru_next->lru_prev = entry->lru_prev;
  } else {
    shard->lru_tail = entry->lru_prev;
  }

  entry->lru_prev = entry->lru_next = NULL;
}

// Used to add a new entry to the head of LRU
static void _lru_add_to_head(bm_cache_shard_t *shard,
                             bm_cache_value_entry_t *entry) {
  entry->lru_next = shard->lru_head;
  entry->lru_prev = NULL;

  if (shard->lru_head) {
    shard->lru_head->lru_prev = entry;
  } else {
    shard->lru_tail = entry;
  }

  shard->lru_head = entry;
}

static bitmap_t *_load_from_backend_storage(const char *key, size_t key_len) {
  // printf("CACHE MISS: Loading '%.*s' from backend storage (LMDB).\n",
  // (int)key_len, key);
  (void)key;
  (void)key_len;
  return NULL;
}

// Get shard index from key
static int _get_shard_index(const char *key) {
  return xxhash64(key, sizeof(key), 0) & SHARD_MASK;
}

static void _add_entry_to_dirty_list(bm_cache_shard_t *shard,
                                     bm_cache_value_entry_t *entry) {

  // Need mutex to protect against flush thread
  uv_mutex_lock(&shard->dirty_list_lock);

  entry->dirty_next = NULL;

  if (shard->dirty_head) {
    shard->dirty_head->dirty_next = entry;
  }

  shard->dirty_head = entry;

  uv_mutex_unlock(&shard->dirty_list_lock);
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

// The THREAD-LOCAL epoch record pointer.
// Each thread gets its own "ID badge".
static _Thread_local ck_epoch_record_t *thread_epoch_record;

// Global epoch for entire cache.
static ck_epoch_t g_epoch;

// --- Public API Implementations ---

bool bitmap_cache_init(void) {
  for (int i = 0; i < NUM_SHARDS; ++i) {
    if (!bm_init_shard(&g_cache.shards[i])) {
      return false;
    }
  }
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
  if (!enqueued) {
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
  if (thread_epoch_record == NULL) {
    ck_epoch_register(&g_epoch, thread_epoch_record, NULL);
  }
  ck_epoch_begin(thread_epoch_record, &h->epoch_section);
  return h;
}

void bitmap_cache_query_end(bitmap_cache_handle_t *handle) {
  if (!handle)
    return;
  ck_epoch_end(thread_epoch_record, &handle->epoch_section);
  free(handle);
}

// Evicts the least recently used entry if it's not in use.
// static void _evict_lru_entry() {
//   bm_cache_entry_t *entry_to_evict = g_cache.lru_tail;
//   if (!entry_to_evict || entry_to_evict->evict ||
//       entry_to_evict->ref_count > 0) {
//     return;
//   }

//   if (entry_to_evict->is_dirty) {
//     // If dirty, DO NOT remove from cache, else risk data corruption.
//     // Background writer only removes after successful flush.
//     entry_to_evict->evict = true; // Mark for later eviction
//     return;
//   }

//   if (entry_to_evict->prev) {
//     entry_to_evict->prev->next = entry_to_evict->next;
//   } else {
//     g_cache.lru_head = entry_to_evict->next;
//   }

//   if (entry_to_evict->next) {
//     entry_to_evict->next->prev = entry_to_evict->prev;
//   } else {
//     g_cache.lru_tail = entry_to_evict->prev;
//   }

//   HASH_DEL(g_cache.entrys_hash, entry_to_evict);

//   eng_cache_free_entry(entry_to_evict);

//   g_cache.size--;
// }

void bitmap_cache_shutdown(void) {}

// bm_cache_entry_t *
// eng_cache_get_or_create(eng_container_t *c, eng_user_dc_db_type_t db_type,
//                         db_key_t db_key,
//                         eng_cache_entry_lock_type_t lock_type) {
//   bm_cache_entry_t *entry = NULL;
//   char cache_key[MAX_CACHE_KEY_SIZE];
//   if (!_get_cache_key(cache_key, sizeof(cache_key), c->name, db_type,
//   db_key)) {
//     return NULL;
//   }

//   /*
//   TODO: This is a bottleneck that serializes all cache access.
//   Branch on lock_type?
//   */
//   uv_rwlock_wrlock(&g_cache.lock);

//   HASH_FIND_STR(g_cache.entrys_hash, cache_key, entry);

//   if (entry) {
//     atomic_fetch_add(&entry->ref_count, 1);
//     _move_to_front(entry);
//     uv_rwlock_wrlock(
//         &entry->entry_lock); // need this but then writes happen serially
//     uv_rwlock_wrunlock(&g_cache.lock);
//     return entry;
//   }

//   if (g_cache.size >= g_cache.capacity) {
//     // (Note: eviction might fail if the LRU entry is in use)
//     _evict_lru_entry();
//   }

//   entry = malloc(sizeof(bm_cache_entry_t));
//   if (!entry) {
//     uv_rwlock_wrunlock(&g_cache.lock);
//     return NULL;
//   }

//   if (snprintf(entry->container_name, sizeof(entry->container_name), "%s",
//                c->name) < 0 ||
//       snprintf(entry->cache_key, sizeof(entry->cache_key), "%s", cache_key) <
//           0) {
//     eng_cache_free_entry(entry);
//     uv_rwlock_wrunlock(&g_cache.lock);
//     return NULL;
//   }
//   entry->db_type = db_type;
//   entry->db_key = db_key;
//   if (db_key.type == DB_KEY_STRING) {
//     entry->db_key.key.s = strdup(db_key.key.s);
//   }
//   atomic_init(&entry->ref_count, 1);
//   entry->current_version = 1;
//   entry->flush_version = 0;

//   // Caller is responsible for loading data object!
//   entry->data_object = NULL;
//   entry->is_dirty = false;
//   entry->evict = false;
//   entry->is_flushing = false;
//   uv_rwlock_init(&entry->entry_lock);

//   HASH_ADD_KEYPTR(hh, g_cache.entrys_hash, entry->cache_key,
//                   strlen(entry->cache_key), entry);

//   if (g_cache.lru_head) {
//     entry->next = g_cache.lru_head;
//     g_cache.lru_head->prev = entry;
//     g_cache.lru_head = entry;
//   } else {
//     g_cache.lru_head = entry;
//     g_cache.lru_tail = entry;
//   }

//   g_cache.size++;

//   // Important:
//   // Before releasing the global lock, acquire the lock on the specific
//   entry. if (lock_type == CACHE_LOCK_WRITE) {
//     uv_rwlock_wrlock(&entry->entry_lock);
//   } else {
//     uv_rwlock_rdlock(&entry->entry_lock);
//   }

//   // Unlock global cache lock
//   uv_rwlock_wrunlock(&g_cache.lock);
//   return entry;
// }

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
