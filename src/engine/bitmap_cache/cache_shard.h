#ifndef BM_CACHE_SHARD_H
#define BM_CACHE_SHARD_H

#include "cache_entry.h"
#include "cache_queue_msg.h"
#include "ck_ht.h"
#include "ck_ring.h"
#include "uv.h" // IWYU pragma: keep
#include <stdbool.h>

#define CAPACITY_PER_SHARD 10240

typedef struct bm_cache_shard_s {
  ck_ht_t table;
  ck_ring_t ring;
  ck_ring_buffer_t ring_buffer[CAPACITY_PER_SHARD];

  uint32_t n_entries;
  bm_cache_value_entry_t
      *lru_head; // Head of the LRU list (most recently used).
  bm_cache_value_entry_t
      *lru_tail; // Tail of the LRU list (least recently used).

  // Reversed linked list
  bm_cache_value_entry_t *dirty_head;
  uv_mutex_t dirty_list_lock;
} bm_cache_shard_t;

bool bm_init_shard(bm_cache_shard_t *shard);

bool shard_enqueue_msg(bm_cache_shard_t *shard, bm_cache_queue_msg_t *msg);

#endif