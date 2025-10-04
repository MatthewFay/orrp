#include "engine/consumer/consumer_cache_internal.h"
#include "ck_ht.h"
#include "ck_ring.h"
#include "consumer_cache_ebr.h"
#include "consumer_cache_entry.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

#define HT_SEED 0

// =============================================================================
// --- Dirty List ---
// =============================================================================

static void _add_entry_to_dirty_list(consumer_cache_t *cache,
                                     consumer_cache_entry_t *entry) {
  entry->dirty_next = NULL;

  if (cache->dirty_head) {
    cache->dirty_tail->dirty_next = entry;
    cache->dirty_tail = entry;
  } else {
    cache->dirty_head = entry;
    cache->dirty_tail = entry;
  }

  cache->num_dirty_entries++;
}

consumer_cache_dirty_snapshot_t *
cache_get_dirty_snapshot(consumer_cache_t *cache) {
  if (!cache)
    return NULL;
  uint32_t num_dirty = cache->num_dirty_entries;
  if (!num_dirty) {
    return NULL;
  }
  consumer_cache_entry_t *dirty_cache_entry = cache->dirty_head;
  if (!dirty_cache_entry) {
    return NULL;
  }
  consumer_cache_dirty_snapshot_t *ds =
      calloc(1, sizeof(consumer_cache_dirty_snapshot_t));
  if (!ds) {
    return NULL;
  }

  ds->cache = cache;
  ds->entry_count = num_dirty;

  ds->dirty_copies = calloc(1, sizeof(consumer_cache_dirty_copy_t) * num_dirty);
  if (!ds->dirty_copies) {
    free(ds);
    return NULL;
  }

  uint32_t i = 0;
  while (dirty_cache_entry) {
    consumer_cache_dirty_copy_t *copy = &ds->dirty_copies[i];
    bitmap_t *bm = atomic_load(&dirty_cache_entry->bitmap);
    if (!bm) {
      cache_free_dirty_snapshot(ds);
      return NULL;
    }
    copy->bitmap = bitmap_copy(bm);
    if (!copy->bitmap) {
      cache_free_dirty_snapshot(ds);
      return NULL;
    }
    copy->flush_version_ptr = &dirty_cache_entry->flush_version;
    copy->container_name = strdup(dirty_cache_entry->container_name);
    copy->db_type = dirty_cache_entry->db_type;
    copy->db_key.type = dirty_cache_entry->db_key.type;
    if (dirty_cache_entry->db_key.type == DB_KEY_STRING) {
      copy->db_key.key.s = strdup(dirty_cache_entry->db_key.key.s);
    } else {
      copy->db_key.key.i = dirty_cache_entry->db_key.key.i;
    }
    dirty_cache_entry = dirty_cache_entry->dirty_next;
    ++i;
  }
  return ds;
}

void cache_clear_dirty_list(consumer_cache_t *cache) {
  cache->dirty_head = cache->dirty_tail = NULL;
  cache->num_dirty_entries = 0;
}

void cache_free_dirty_snapshot(consumer_cache_dirty_snapshot_t *snapshot) {
  if (!snapshot)
    return;

  for (uint32_t i = 0; i < snapshot->entry_count; i++) {
    consumer_cache_dirty_copy_t *copy = &snapshot->dirty_copies[i];
    bitmap_free(copy->bitmap);
    free(copy->flush_version_ptr);
    free(copy->container_name);
    if (copy->db_key.type == DB_KEY_STRING) {
      free((char *)copy->db_key.key.s);
    }
  }
  free(snapshot);
}

// =============================================================================
// --- LRU List Helpers ---
// =============================================================================

// Used to move an existing entry (already in LRU) to front of LRU
static void _lru_move_to_front(consumer_cache_t *cache,
                               consumer_cache_entry_t *entry) {
  if (!entry || entry == cache->lru_head)
    return;
  entry->lru_prev->lru_next = entry->lru_next;
  if (entry == cache->lru_tail) {
    cache->lru_tail = entry->lru_prev;
  } else {
    entry->lru_next->lru_prev = entry->lru_prev;
  }
  entry->lru_prev = NULL;
  entry->lru_next = cache->lru_head;
  cache->lru_head->lru_prev = entry;
  cache->lru_head = entry;
}

// Remove entry from LRU
static void _lru_remove_entry(consumer_cache_t *cache,
                              consumer_cache_entry_t *entry) {
  if (entry->lru_prev) {
    entry->lru_prev->lru_next = entry->lru_next;
  } else {
    cache->lru_head = entry->lru_next;
  }

  if (entry->lru_next) {
    entry->lru_next->lru_prev = entry->lru_prev;
  } else {
    cache->lru_tail = entry->lru_prev;
  }

  entry->lru_prev = entry->lru_next = NULL;
}

// Used to add a new entry to the head of LRU
static void _lru_add_to_head(consumer_cache_t *cache,
                             consumer_cache_entry_t *entry) {
  entry->lru_next = cache->lru_head;
  entry->lru_prev = NULL;

  if (cache->lru_head) {
    cache->lru_head->lru_prev = entry;
  } else {
    cache->lru_tail = entry;
  }

  cache->lru_head = entry;
}

static void *_hs_malloc(size_t r) { return malloc(r); }
static void _hs_free(void *p, size_t b, bool r) {
  (void)b;
  (void)r;
  free(p);
}
static struct ck_malloc _my_allocator = {.malloc = _hs_malloc,
                                         .free = _hs_free};

bool consumer_cache_init(consumer_cache_t *cache) {

  cache->lru_head = cache->lru_tail = NULL;
  cache->dirty_head = NULL;
  cache->dirty_tail = NULL;
  cache->n_entries = 0;
  cache->num_dirty_entries = 0;
  if (!ck_ht_init(&cache->table, CK_HT_MODE_BYTESTRING,
                  NULL, // Initialize with Murmur64 (default)
                  &_my_allocator, CAPACITY_PER_cache, HT_SEED)) {
    return false;
  }
  return true;
}

bool cache_enqueue_msg(consumer_cache_t *cache,
                       consumer_cache_queue_msg_t *msg) {
  if (!cache || !msg)
    return false;
  // MPSC: Multiple producer, single consumer
  return ck_ring_enqueue_mpsc(&cache->ring, cache->ring_buffer, msg);
}

bool cache_dequeue_msg(consumer_cache_t *cache,
                       consumer_cache_queue_msg_t **msg_out) {
  if (!cache)
    return false;
  // MPSC: Multi Producer, Single Consumer
  return ck_ring_dequeue_mpsc(&cache->ring, cache->ring_buffer, msg_out);
}

bool consumer_cache_get_entry(consumer_cache_t *consumer_cache,
                              const char *ser_db_key,
                              consumer_cache_entry_t **entry_out) {
  ck_ht_hash_t hash;
  ck_ht_entry_t entry;

  if (!ser_db_key)
    return false;
  unsigned long key_len = strlen(ser_db_key);
  ck_ht_hash(&hash, &consumer_cache->table, ser_db_key, key_len);
  ck_ht_entry_set(&entry, hash, ser_db_key, key_len, NULL);
  if (!ck_ht_get_spmc(&cache->table, hash, &entry)) {
    return false;
  }
  *entry_out = (consumer_cache_entry_t *)entry.value;
  _lru_move_to_front(consumer_cache, *entry_out);
  return true;
}

static bool _rem_from_cache_table(consumer_cache_t *cache,
                                  consumer_cache_entry_t *entry) {
  ck_ht_hash_t hash;
  ck_ht_entry_t ck_entry;
  unsigned long key_len = strlen(entry->ser_db_key);
  ck_ht_hash(&hash, &cache->table, entry->ser_db_key, key_len);
  ck_ht_entry_set(&ck_entry, hash, entry->ser_db_key, key_len, entry);
  if (!ck_ht_remove_spmc(&cache->table, hash, &ck_entry)) {
    return false;
  }
  return true;
}

static void _evict_lru(consumer_cache_t *cache) {
  if (!cache || !cache->lru_tail)
    return;
  consumer_cache_entry_t *lru_tail = cache->lru_tail;
  bitmap_t *bm = atomic_load(&lru_tail->bitmap);
  uint64_t flush_v = atomic_load(&lru_tail->flush_version);
  if (bm->version != flush_v) {
    // dirty! do not evict.
    return;
  }
  if (!_rem_from_cache_table(cache, lru_tail)) {
    return;
  }
  _lru_remove_entry(cache, lru_tail);

  // Do not free bitmap on purpose - will be free'd later using EBR
  consumer_cache_ebr_retire(&lru_tail->bitmap->epoch_entry);
  consumer_cache_free_entry(lru_tail);

  cache->n_entries--;
}

bool cache_add_entry(consumer_cache_t *cache, const char *ser_db_key,
                     consumer_cache_entry_t *entry, bool dirty) {
  ck_ht_hash_t hash;
  ck_ht_entry_t ck_entry;

  if (!cache || !ser_db_key || !entry)
    return false;

  uint32_t new_size = cache->n_entries + 1;
  _lru_add_to_head(cache, entry);

  if (new_size > CAPACITY_PER_cache) {
    _evict_lru(cache);
  }

  unsigned long key_len = strlen(ser_db_key);
  ck_ht_hash(&hash, &cache->table, ser_db_key, key_len);
  char *key = strdup(ser_db_key);
  ck_ht_entry_set(&ck_entry, hash, key, key_len, entry);
  bool put_r = ck_ht_put_spmc(&cache->table, hash, &ck_entry);
  if (put_r) {
    cache->n_entries = new_size;
    if (dirty) {
      _add_entry_to_dirty_list(cache, entry);
    }
    return true;
  }
  _lru_remove_entry(cache, entry);
  free(key);
  return false;
}