#include "cache_queue_consumer.h"
#include "uv.h"

static void _consumer_thread_func(void *arg) {
  (void)arg;
  bm_cache_consumer_t *consumer = (bm_cache_consumer_t *)arg;
  const bm_cache_consumer_config_t *config = &consumer->config;

  while (!consumer->should_stop) {
    bool processed_any = false;

    for (uint32_t i = 0; i < config->shard_count; i++) {
      uint32_t shard_idx = config->shard_start + i;
      bm_cache_shard_t *shard = &config->shards[shard_idx];

      bm_cache_queue_msg_t msg;
      if (ck_ring_dequeue_spsc(&shard->ring, shard->ring_buffer, &msg)) {
        // process_cache_message(shard, &msg);
        consumer->messages_processed++;
        processed_any = true;
      }
    }

    // If no work, yield briefly to avoid spinning
    if (!processed_any) {
      uv_sleep(1);
    }
  }
}

bool bm_cache_consumer_start(bm_cache_consumer_t *consumer,
                          const bm_cache_consumer_config_t *config) {
  consumer->config = *config;
  consumer->should_stop = false;
  consumer->messages_processed = 0;

  return uv_thread_create(&consumer->thread, _consumer_thread_func, consumer);
}

bool bm_cache_consumer_stop(bm_cache_consumer_t *consumer) {
  consumer->should_stop = true;
  if (uv_thread_join(&consumer->thread) != 0) {
    return false;
  }
  return true;
}