#ifndef BM_CACHE_QUEUE_CONSUMER_H
#define BM_CACHE_QUEUE_CONSUMER_H

#include "cache_shard.h"
#include "uv.h" // IWYU pragma: keep

typedef struct bm_cache_consumer_config_s {
  bm_cache_shard_t *shards; // Array of shards to process
  uint32_t shard_start;     // Starting shard index
  uint32_t shard_count;     // Number of shards to handle
  uint32_t consumer_id;     // Thread identifier
} bm_cache_consumer_config_t;

typedef struct bm_cache_consumer_s {
  bm_cache_consumer_config_t config;
  uv_thread_t thread;
  volatile bool should_stop;
  uint64_t messages_processed; // Stats
} bm_cache_consumer_t;

// Public API
bool bm_cache_consumer_start(bm_cache_consumer_t *consumer,
                             const bm_cache_consumer_config_t *config);
bool bm_cache_consumer_stop(bm_cache_consumer_t *consumer);
void bm_cache_consumer_get_stats(bm_cache_consumer_t *consumer,
                                 uint64_t *processed);

#endif