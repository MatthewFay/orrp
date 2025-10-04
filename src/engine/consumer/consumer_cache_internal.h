#ifndef CONSUMER_CACHE_INTERNAL_H
#define CONSUMER_CACHE_INTERNAL_H

#include "ck_ht.h"
#include "consumer_cache_entry.h"
#include "uv.h" // IWYU pragma: keep
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

/**
Consumer cache internal Api -
just for use by Consumer thread */

typedef struct consumer_cache_Config_s {
} consumer_cache_config_t;

typedef struct consumer_cache_s {
  consumer_cache_config_t config;
  ck_ht_t table;

  uint32_t n_entries;
  consumer_cache_entry_t
      *lru_head; // Head of the LRU list (most recently used).
  consumer_cache_entry_t
      *lru_tail; // Tail of the LRU list (least recently used).

  consumer_cache_entry_t *dirty_head;
  consumer_cache_entry_t *dirty_tail;
  uint32_t num_dirty_entries;

  ck_epoch_t epoch;
} consumer_cache_t;

// Consumer ??
// Dirty list snapshot for flushing - copied data for safety purposes
typedef struct consumer_cache_dirty_snapshot_s {
  consumer_cache_t *consumer_cache;
  // consumer_cache_dirty_copy_t *dirty_copies;
  uint32_t entry_count;
} consumer_cache_dirty_snapshot_t;

bool consumer_cache_init(consumer_cache_t *consumer_cache);
bool consumer_cache_shutdown(consumer_cache_t *consumer_cache);

bool consumer_cache_get_entry(consumer_cache_t *consumer_cache,
                              const char *ser_db_key,
                              consumer_cache_entry_t **entry_out);

bool consumer_cache_add_entry(consumer_cache_t *consumer_cache,
                              const char *ser_db_key,
                              consumer_cache_entry_t *entry, bool dirty);

consumer_cache_dirty_snapshot_t *
shard_get_dirty_snapshot(consumer_cache_t *consumer_cache);

void shard_clear_dirty_list(consumer_cache_t *consumer_cache);

void shard_free_dirty_snapshot(consumer_cache_dirty_snapshot_t *snapshot);

#endif