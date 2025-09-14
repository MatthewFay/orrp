#include "cache_shard.h"
#include "ck_ht.h"
#include "ck_ring.h"
#include "engine/bitmap_cache/cache_entry.h"
#include <stdbool.h>
#include <stdint.h>

#define HT_SEED 0

static void _add_entry_to_dirty_list(bm_cache_shard_t *shard,
                                     bm_cache_entry_t *entry) {
  // Need mutex to protect against flush thread
  uv_mutex_lock(&shard->dirty_list_lock);

  entry->dirty_next = NULL;

  if (shard->dirty_head) {
    shard->dirty_head->dirty_next = entry;
  }

  shard->dirty_head = entry;

  uv_mutex_unlock(&shard->dirty_list_lock);
}

// =============================================================================
// --- LRU List Helpers ---
// =============================================================================

// Used to move an existing entry (already in LRU) to front of LRU
void shard_lru_move_to_front(bm_cache_shard_t *shard, bm_cache_entry_t *entry, bool dirty) {
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

  if (dirty) {
    _add_entry_to_dirty_list(shard, entry);
  }
}

// Remove entry from LRU
static void _lru_remove_entry(bm_cache_shard_t *shard,
                              bm_cache_entry_t *entry) {
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
static void _lru_add_to_head(bm_cache_shard_t *shard, bm_cache_entry_t *entry) {
  entry->lru_next = shard->lru_head;
  entry->lru_prev = NULL;

  if (shard->lru_head) {
    shard->lru_head->lru_prev = entry;
  } else {
    shard->lru_tail = entry;
  }

  shard->lru_head = entry;
}

static void *_hs_malloc(size_t r) { return malloc(r); }
static void _hs_free(void *p, size_t b, bool r) {
  (void)b;
  (void)r;
  free(p);
}
static struct ck_malloc _my_allocator = {.malloc = _hs_malloc,
                                         .free = _hs_free};

bool bm_init_shard(bm_cache_shard_t *shard) {
  uv_mutex_init(&shard->dirty_list_lock);

  shard->lru_head = shard->lru_tail = NULL;
  shard->dirty_head = NULL;
  shard->n_entries = 0;
  ck_ring_init(&shard->ring, CAPACITY_PER_SHARD);
  if (!ck_ht_init(&shard->table, CK_HT_MODE_BYTESTRING,
                  NULL, // Initialize with Murmur64 (default)
                  &_my_allocator, CAPACITY_PER_SHARD, HT_SEED)) {
    return false;
  }
  return true;
}

bool shard_enqueue_msg(bm_cache_shard_t *shard, bm_cache_queue_msg_t *msg) {
  if (!shard || !msg)
    return false;
  // MPSC: Multiple producer, single consumer
  return ck_ring_enqueue_mpsc(&shard->ring, shard->ring_buffer, msg);
}

bool shard_dequeue_msg(bm_cache_shard_t *shard,
                       bm_cache_queue_msg_t **msg_out) {
  if (!shard)
    return false;
  // MPSC: Multi Producer, Single Consumer
  return ck_ring_dequeue_mpsc(&shard->ring, shard->ring_buffer, msg_out);
}

bool shard_get_entry(bm_cache_shard_t *shard, const char *cache_key,
                     bm_cache_entry_t **entry_out) {
  ck_ht_hash_t hash;
  ck_ht_entry_t entry;

  if (!cache_key)
    return false;
  unsigned long key_len = strlen(cache_key);
  ck_ht_hash(&hash, &shard->table, cache_key, key_len);
  ck_ht_entry_set(&entry, hash, cache_key, key_len, NULL);
  if (!ck_ht_get_spmc(&shard->table, hash, &entry)) {
    return false;
  }
  *entry_out = (bm_cache_entry_t *)entry.value;
  return true;
}

static void _evict_lru(bm_cache_shard_t *shard) {
  (void)shard;
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
}

bool shard_add_entry(bm_cache_shard_t *shard, const char *cache_key,
                     bm_cache_entry_t *entry, bool dirty) {
  ck_ht_hash_t hash;
  ck_ht_entry_t ck_entry;

  if (!shard || !cache_key || !entry)
    return false;

  uint32_t new_size = shard->n_entries + 1;
  _lru_add_to_head(shard, entry);

  if (new_size > CAPACITY_PER_SHARD) {
    _evict_lru(shard);
  }

  unsigned long key_len = strlen(cache_key);
  ck_ht_hash(&hash, &shard->table, cache_key, key_len);
  char *key = strdup(cache_key);
  ck_ht_entry_set(&ck_entry, hash, key, key_len, entry);
  bool put_r = ck_ht_put_spmc(&shard->table, hash, &ck_entry);
  if (put_r) {
    shard->n_entries = new_size;
    if (dirty) {
      _add_entry_to_dirty_list(shard, entry);
    }
    return true;
  }
  _lru_remove_entry(shard, entry);
  free(key);
  return false;
}