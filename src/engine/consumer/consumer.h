#ifndef CONSUMER_H
#define CONSUMER_H

/**
Operation consumers - consumers from Operation queues
sends messages to engine writer for persistence
*/

#include "ck_epoch.h"
#include "consumer_cache_internal.h"
#include "engine/context/context.h"
#include "engine/engine_writer/engine_writer.h"
#include "engine/op_queue/op_queue.h"
#include "uv.h" // IWYU pragma: keep
#include <stdint.h>

typedef struct consumer_config_s {
  eng_context_t *eng_context;
  eng_writer_t *writer;
  uint32_t flush_every_n;
  op_queue_t *op_queues;
  uint32_t op_queue_consume_start; // Starting op queue index to consume
  uint32_t op_queue_consume_count; // Number of op queues to consume from
  uint32_t op_queue_total_count;   // Total count of op queues
  uint32_t consumer_id;            // Thread identifier
} consumer_config_t;

typedef struct consumer_s {
  consumer_config_t config;
  uv_thread_t thread;
  volatile bool should_stop;
  uint64_t messages_processed; // Stats
  ck_epoch_record_t consumer_cache_thread_epoch_record;
  consumer_cache_t cache;
} consumer_t;

// Public API
bool consumer_start(consumer_t *consumer, const consumer_config_t *config);
bool consumer_stop(consumer_t *consumer);
void consumer_get_stats(consumer_t *consumer, uint64_t *processed_out);

#endif