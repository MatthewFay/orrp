#ifndef consumer_flush_h
#define consumer_flush_h

#include "engine/consumer/consumer_cache_entry.h"
#include "engine/engine_writer/engine_writer_queue_msg.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct {
  bool success;
  eng_writer_msg_t *msg; // NULL on failure
  uint32_t entries_prepared;
  uint32_t entries_skipped;
  const char *err_msg;
} consumer_flush_result_t;

// Prepare flush message from dirty entries (doesn't enqueue or clear)
consumer_flush_result_t
consumer_flush_prepare(consumer_cache_entry_t *dirty_head,
                       uint32_t num_dirty_entries);

void consumer_flush_clear_result(consumer_flush_result_t r);

#endif