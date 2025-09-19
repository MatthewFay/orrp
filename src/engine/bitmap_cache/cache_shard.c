#include "cache_shard.h"
#include "cache_entry.h"
#include "ck_ht.h"
#include "ck_ring.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/bitmap_cache/cache_ebr.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

#define HT_SEED 0

// =============================================================================
// --- Dirty List ---
// =============================================================================

static void _add_entry_to_dirty_list(bm_cache_shard_t *shard,
                                     bm_cache_entry_t *entry) {
  entry->dirty_next = NULL;

  if (shard->dirty_head) {
    shard->dirty_tail->dirty_next = entry;
    shard->dirty_tail = entry;
  } else {
    shard->dirty_head = entry;
    shard->dirty_tail = entry;
  }

  shard->num_dirty_entries++;
}

bm_cache_dirty_snapshot_t *shard_get_dirty_snapshot(bm_cache_shard_t *shard) {
  if (!shard)
    return NULL;
  uint32_t num_dirty = shard->num_dirty_entries;
  if (!num_dirty) {
    return NULL;
  }
  bm_cache_entry_t *dirty_cache_entry = shard->dirty_head;
  if (!dirty_cache_entry) {
    return NULL;
  }
  bm_cache_dirty_snapshot_t *ds = calloc(1, sizeof(bm_cache_dirty_snapshot_t));
  if (!ds) {
    return NULL;
  }

  ds->shard = shard;
  ds->entry_count = num_dirty;

  ds->dirty_copies = calloc(1, sizeof(bm_cache_dirty_copy_t) * num_dirty);
  if (!ds->dirty_copies) {
    free(ds);
    return NULL;
  }

  uint32_t i = 0;
  while (dirty_cache_entry) {
    bm_cache_dirty_copy_t *copy = &ds->dirty_copies[i];
    bitmap_t *bm = atomic_load(&dirty_cache_entry->bitmap);
    if (!bm) {
      shard_free_dirty_snapshot(ds);
      return NULL;
    }
    copy->bitmap = bitmap_copy(bm);
    if (!copy->bitmap) {
      shard_free_dirty_snapshot(ds);
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

void shard_clear_dirty_list(bm_cache_shard_t *shard) {
  shard->dirty_head = shard->dirty_tail = NULL;
  shard->num_dirty_entries = 0;
}

void shard_free_dirty_snapshot(bm_cache_dirty_snapshot_t *snapshot) {
  if (!snapshot)
    return;

  for (uint32_t i = 0; i < snapshot->entry_count; i++) {
    bm_cache_dirty_copy_t *copy = &snapshot->dirty_copies[i];
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
void shard_lru_move_to_front(bm_cache_shard_t *shard, bm_cache_entry_t *entry,
                             bool dirty) {
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

  shard->lru_head = shard->lru_tail = NULL;
  shard->dirty_head = NULL;
  shard->dirty_tail = NULL;
  shard->n_entries = 0;
  shard->num_dirty_entries = 0;
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

static bool _rem_from_cache_table(bm_cache_shard_t *shard,
                                  bm_cache_entry_t *entry) {
  ck_ht_hash_t hash;
  ck_ht_entry_t ck_entry;
  unsigned long key_len = strlen(entry->cache_key);
  ck_ht_hash(&hash, &shard->table, entry->cache_key, key_len);
  ck_ht_entry_set(&ck_entry, hash, entry->cache_key, key_len, entry);
  if (!ck_ht_remove_spmc(&shard->table, hash, &ck_entry)) {
    return false;
  }
  return true;
}

static void _evict_lru(bm_cache_shard_t *shard) {
  if (!shard || !shard->lru_tail)
    return;
  bm_cache_entry_t *lru_tail = shard->lru_tail;
  bitmap_t *bm = atomic_load(&lru_tail->bitmap);
  uint64_t flush_v = atomic_load(&lru_tail->flush_version);
  if (bm->version != flush_v) {
    // dirty! do not evict.
    return;
  }
  if (!_rem_from_cache_table(shard, lru_tail)) {
    return;
  }
  _lru_remove_entry(shard, lru_tail);

  // Do not free bitmap on purpose - will be free'd later using EBR
  bm_cache_ebr_retire(&lru_tail->bitmap->epoch_entry);
  bm_cache_free_entry(lru_tail);

  shard->n_entries--;
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