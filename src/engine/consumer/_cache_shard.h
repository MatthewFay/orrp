#ifndef BM_CACHE_SHARD_H
#define BM_CACHE_SHARD_H

#include "cache_entry.h"
#include "cache_queue_msg.h"
#include "ck_ht.h"
#include "ck_ring.h"
#include "uv.h" // IWYU pragma: keep
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

#define CAPACITY_PER_SHARD 16384

typedef struct bm_cache_shard_s {
  // Key: cache key, Value: cache entry
  ck_ht_t table;
  ck_ring_t ring;
  ck_ring_buffer_t ring_buffer[CAPACITY_PER_SHARD];

  uint32_t n_entries;
  bm_cache_entry_t *lru_head; // Head of the LRU list (most recently used).
  bm_cache_entry_t *lru_tail; // Tail of the LRU list (least recently used).

  bm_cache_entry_t *dirty_head;
  bm_cache_entry_t *dirty_tail;
  uint32_t num_dirty_entries;
} bm_cache_shard_t;

typedef struct bm_cache_dirty_copy_s {
  bitmap_t *bitmap;
  _Atomic(uint64_t) *flush_version_ptr;
  char *container_name;
  eng_user_dc_db_type_t db_type;
  db_key_t db_key;
} bm_cache_dirty_copy_t;

// Dirty list snapshot for flushing - copied data for safety purposes
typedef struct bm_cache_dirty_snapshot_s {
  bm_cache_shard_t *shard;
  bm_cache_dirty_copy_t *dirty_copies;
  uint32_t entry_count;
} bm_cache_dirty_snapshot_t;

bool bm_init_shard(bm_cache_shard_t *shard);

bool shard_enqueue_msg(bm_cache_shard_t *shard, bm_cache_queue_msg_t *msg);

bool shard_dequeue_msg(bm_cache_shard_t *shard, bm_cache_queue_msg_t **msg_out);

bool shard_get_entry(bm_cache_shard_t *shard, const char *cache_key,
                     bm_cache_entry_t **entry_out);

bool shard_add_entry(bm_cache_shard_t *shard, const char *cache_key,
                     bm_cache_entry_t *entry, bool dirty);

void shard_lru_move_to_front(bm_cache_shard_t *shard, bm_cache_entry_t *entry,
                             bool dirty);

bm_cache_dirty_snapshot_t *shard_get_dirty_snapshot(bm_cache_shard_t *shard);

void shard_clear_dirty_list(bm_cache_shard_t *shard);

void shard_free_dirty_snapshot(bm_cache_dirty_snapshot_t *snapshot);

#endif