#include "consumer.h"
#include "consumer_batch.h"
#include "consumer_cache_entry.h"
#include "consumer_ebr.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/consumer/consumer_cache_internal.h"
#include "engine/consumer/consumer_flush.h"
#include "engine/consumer/consumer_ops.h"
#include "engine/consumer/consumer_processor.h"
#include "engine/consumer/consumer_validate.h"
#include "engine/container/container.h"
#include "engine/container/container_types.h"
#include "engine/engine_writer/engine_writer_queue.h"
#include "engine/op/op.h"
#include "engine/op_queue/op_queue.h"
#include "engine/op_queue/op_queue_msg.h"
#include "lmdb.h"
#include "log/log.h"
#include "sched.h"
#include "uthash.h"
#include "uv.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

LOG_INIT(consumer);

// spin count before sleeping
#define CONSUMER_SPIN_LIMIT 100
#define CONSUMER_MAX_SLEEP_MS 64

#define MAX_BATCH_SIZE_PER_OP_Queue 128
#define MIN_RECLAIM_BATCH_SIZE 100
// TODO: writer enqueue re-tries
#define MAX_WRITER_ENQUEUE_ATTEMPTS 3

#define CONSUMER_CACHE_CAPACITY 65536

static consumer_cache_entry_t *
_get_or_Create_cache_entry(eng_container_t *dc, consumer_cache_t *cache,
                           consumer_batch_db_key_t *key, op_queue_msg_t *msg,
                           MDB_txn *txn, bool *was_cached_out) {
  *was_cached_out = false;

  consumer_cache_entry_t *cached_entry = NULL;
  if (consumer_cache_get_entry(cache, key->ser_db_key, &cached_entry)) {
    *was_cached_out = true;
    LOG_DEBUG("Cache hit for key: %s", key->ser_db_key);
    return cached_entry;
  }

  LOG_DEBUG("Cache miss for key: %s", key->ser_db_key);

  COP_RESULT_T cop_r = cops_create_entry_from_op_msg(dc, msg, txn);
  if (cop_r.success) {
    return cop_r.entry;
  }
  LOG_ERROR("Error creating cache entry: %s", cop_r.err_msg);
  return NULL;
}

// Process all messages for a container
static bool _process_op_msgs(consumer_t *consumer, eng_container_t *dc,
                             consumer_batch_db_key_t *keys, MDB_txn *txn) {
  consumer_batch_db_key_t *key, *tmp;
  op_queue_msg_t *msg;
  uint32_t keys_processed = 0;
  uint32_t ops_applied = 0;

  HASH_ITER(hh, keys, key, tmp) {
    bool was_cached;
    consumer_batch_msg_node_t *b_entry = key->head;

    consumer_cache_entry_t *cache_entry = _get_or_Create_cache_entry(
        dc, &consumer->cache, key, b_entry->msg, txn, &was_cached);
    if (!cache_entry) {
      LOG_ERROR("Failed to get or create cache entry for key: %s",
                key->ser_db_key);
      return false;
    }

    bitmap_t *old_bm;
    bitmap_t *bm = atomic_load(&cache_entry->bitmap);
    if (was_cached) {
      // if cached, create a copy because other threads could be using it
      old_bm = bm;
      bm = bitmap_copy(bm);
      if (!bm) {
        LOG_ERROR("Failed to copy bitmap for key: %s", key->ser_db_key);
        return false;
      }
    }

    bool dirty = false;
    uint32_t batch_ops = 0;

    while (b_entry) {
      msg = b_entry->msg;

      switch (msg->op->op_type) {
      case OP_CACHE:
        // Will be cached, no work to do
        LOG_DEBUG("BM_CACHE op for key: %s", key->ser_db_key);
        break;
      case OP_ADD_VALUE:
        bitmap_add(bm, msg->op->value.int32_value);
        dirty = true;
        batch_ops++;
        break;
      default:
        LOG_WARN("Unknown op type %d for key: %s", msg->op->op_type,
                 key->ser_db_key);
        break;
      }
      b_entry = b_entry->next;
    }

    if (dirty) {
      bm->version++;
      atomic_store(&cache_entry->bitmap, bm);
      if (was_cached) {
        consumer_ebr_retire(&consumer->consumer_epoch_record,
                            &old_bm->epoch_entry);
      }
      LOG_DEBUG("Applied %u ops to key %s, version now %llx", batch_ops,
                key->ser_db_key, bm->version);
      ops_applied += batch_ops;
    } else {
      bitmap_free(bm);
    }

    if (dirty) {
      consumer_cache_add_entry_to_dirty_list(&consumer->cache, cache_entry);
    }

    if (was_cached) {
      keys_processed++;
      continue;
    }

    if (consumer->cache.n_entries >= CONSUMER_CACHE_CAPACITY) {
      consumer_cache_entry_t *victim =
          consumer_cache_evict_lru(&consumer->cache);
      if (victim) {
        LOG_DEBUG("Evicted LRU cache entry: %s", victim->ser_db_key);
        consumer_ebr_retire(&consumer->consumer_epoch_record,
                            &victim->bitmap->epoch_entry);
        consumer_cache_free_entry(victim);
      } else {
        LOG_WARN("Cache at capacity but failed to evict LRU entry");
      }
    }

    if (!consumer_cache_add_entry(&consumer->cache, key->ser_db_key,
                                  cache_entry)) {
      LOG_ERROR("Failed to add cache entry for key: %s", key->ser_db_key);
    }

    keys_processed++;
  }

  LOG_DEBUG("Processed %u keys, applied %u ops", keys_processed, ops_applied);
  return true;
}

// returns number of msgs processed, or -1 on error
static int _process_batch(consumer_t *consumer,
                          consumer_batch_container_t *batch) {
  if (!batch || !batch->container_name || !batch->db_keys) {
    LOG_WARN("Invalid batch: container=%p, keys=%p",
             batch ? batch->container_name : NULL,
             batch ? batch->db_keys : NULL);
    return -1;
  }

  container_result_t cr;
  if (batch->container_type == CONTAINER_TYPE_SYSTEM) {
    cr = container_get_system();
  } else {
    cr = container_get_or_create_user(batch->container_name);
  }
  if (!cr.success) {
    LOG_ERROR("Failed to get container %s from cache", batch->container_name);
    return -1;
  }

  eng_container_t *dc = cr.container;

  MDB_txn *txn = db_create_txn(dc->env, true);
  if (!txn) {
    LOG_ERROR("Failed to create transaction for container: %s",
              batch->container_name);
    container_release(dc);
    return -1;
  }

  consumer_process_result_t result = consumer_process_container_batch(
      &consumer->cache, dc, txn, &consumer->consumer_epoch_record, batch);

  db_abort_txn(txn);
  container_release(dc);

  if (result.success) {
    LOG_DEBUG(
        "Finished processing batch: %s, %u msgs processed, %u msgs failed",
        batch->container_name, result.msgs_processed, result.msgs_failed);
    return result.msgs_processed;
  }
  LOG_ERROR("Error processing batch %s: %s", batch->container_name,
            result.err_msg);
  return -1;
}

static void _process_batches(consumer_t *consumer,
                             consumer_batch_container_t *container_table) {
  consumer_batch_container_t *batch, *tmp;
  uint32_t batches_processed = 0;
  uint32_t batches_failed = 0;

  HASH_ITER(hh, container_table, batch, tmp) {
    if (_process_batch(consumer, batch) >= 0) {
      batches_processed++;
    } else {
      batches_failed++;
    }
  }

  if (batches_failed > 0) {
    LOG_WARN("Batch processing: %u succeeded, %u failed", batches_processed,
             batches_failed);
  }
}

static bool _validate_op_msg(op_queue_msg_t *msg) {
  if (!msg || !msg->ser_db_key || strlen(msg->ser_db_key) == 0 || !msg->op)
    return false;
  return consumer_validate_op(msg->op);
}

static void _flush_dirty(consumer_t *c) {
  uint32_t num_dirty_entries = c->cache.num_dirty_entries;
  if (num_dirty_entries < 1 || !c->cache.dirty_head) {
    return;
  }

  LOG_DEBUG("Flushing %u dirty cache entries to writer", num_dirty_entries);

  consumer_flush_result_t fr =
      consumer_flush_prepare(c->cache.dirty_head, num_dirty_entries);
  if (!fr.success || !fr.msg) {
    LOG_ERROR("Flush error: %s", fr.err_msg);
    return;
  }

  if (fr.entries_skipped > 0) {
    LOG_WARN("Flush entries skipped: %u", fr.entries_skipped);
  }

  if (fr.entries_prepared > 0) {
    if (!eng_writer_queue_enqueue(&c->config.writer->queue, fr.msg)) {
      LOG_ERROR("Failed to enqueue flush message with %u entries to writer",
                fr.entries_prepared);
    } else {
      LOG_INFO("Flushed %u entries to writer (%u skipped)", fr.entries_prepared,
               fr.entries_skipped);
    }
  } else {
    LOG_WARN("Flush prepared but no entries successfully copied (%u skipped)",
             fr.entries_skipped);
  }

  consumer_flush_clear_result(fr);
  consumer_cache_clear_dirty_list(&c->cache);
}

static void _reclamation(consumer_t *consumer) {
  uint32_t pending = consumer->consumer_epoch_record.n_pending;
  if (pending >= MIN_RECLAIM_BATCH_SIZE) {
    LOG_DEBUG("Running EBR reclamation with %u pending entries", pending);
    consumer_ebr_reclaim(&consumer->consumer_epoch_record);
  }
}

static void _consumer_thread_func(void *arg) {
  consumer_t *consumer = (consumer_t *)arg;
  const consumer_config_t *config = &consumer->config;
  consumer_cache_config_t cache_config = {.capacity = CONSUMER_CACHE_CAPACITY};

  log_init_consumer();
  if (!LOG_CATEGORY) {
    fprintf(stderr,
            "FATAL: Failed to initialize logging for consumer thread\n");
    return;
  }

  LOG_INFO("Consumer thread started");

  consumer_cache_init(&consumer->cache, &cache_config);
  consumer_ebr_register(&consumer->epoch, &consumer->consumer_epoch_record);

  consumer_batch_container_t *container_table = NULL;
  op_queue_msg_t *msg = NULL;
  bool batched_any = false;
  uint32_t msgs_batched = 0;
  uint32_t cycle = 0;
  int backoff = 1;
  int spin_count = 0;
  uint64_t total_cycles = 0;
  uint64_t active_cycles = 0;

  while (!consumer->should_stop) {
    batched_any = false;
    msgs_batched = 0;
    cycle++;
    total_cycles++;

    for (uint32_t i = 0; i < consumer->config.op_queue_consume_count; i++) {
      uint32_t op_queue_idx = consumer->config.op_queue_consume_start + i;
      op_queue_t *queue = &consumer->config.op_queues[op_queue_idx];

      for (int j = 0; j < MAX_BATCH_SIZE_PER_OP_Queue; j++) {
        if (!op_queue_dequeue(queue, &msg)) {
          break; // No more messages in this shard
        }
        if (!_validate_op_msg(msg)) {
          LOG_ERROR("Invalid op msg");
          continue;
        }

        if (!consumer_batch_add_msg(&container_table, msg)) {
          LOG_ERROR("Failed to add message to existing batch for container: %s",
                    msg->op->db_key.container_name);
          continue;
        }

        batched_any = true;
        msgs_batched++;
      }
    }

    if (batched_any) {
      LOG_DEBUG("Batched %u messages from queues", msgs_batched);
      backoff = 1;
      spin_count = 0;
      active_cycles++;
      _process_batches(consumer, container_table);
      consumer_batch_free_table(container_table);
      container_table = NULL;
    } else {
      consumer_batch_free_table(container_table);

      if (spin_count < CONSUMER_SPIN_LIMIT) {
        sched_yield();
        spin_count++;
      } else {
        uv_sleep(backoff);
        backoff = backoff < CONSUMER_MAX_SLEEP_MS ? backoff * 2
                                                  : CONSUMER_MAX_SLEEP_MS;
      }
    }

    if (cycle == config->flush_every_n) {
      cycle = 0;
      _flush_dirty(consumer);
      _reclamation(consumer);

      // Periodic stats
      if (total_cycles % 100000 == 0) {
        double active_pct = (active_cycles * 100.0) / total_cycles;
        LOG_INFO("Consumer stats: cycles=%llu, active=%.1f%%, cache_entries=%u",
                 total_cycles, active_pct, consumer->cache.n_entries);
      }
    }
  }

  consumer_ebr_unregister(&consumer->consumer_epoch_record);
  consumer_cache_destroy(&consumer->cache);

  LOG_INFO("Consumer thread exiting [total_cycles=%llu]", total_cycles);
}

consumer_result_t consumer_start(consumer_t *consumer,
                                 const consumer_config_t *config) {
  if (!consumer || !config) {
    return (consumer_result_t){.success = false,
                               .msg = "Invalid arguments to consumer_start"};
  }

  consumer->config = *config;
  consumer->should_stop = false;
  consumer->messages_processed = 0;

  if (uv_thread_create(&consumer->thread, _consumer_thread_func, consumer) !=
      0) {
    return (consumer_result_t){.success = false,
                               .msg = "Failed to create consumer thread"};
  }

  return (consumer_result_t){.success = true};
}

consumer_result_t consumer_stop(consumer_t *consumer) {
  if (!consumer) {
    return (consumer_result_t){.success = false,
                               .msg = "Invalid consumer in consumer_stop"};
  }

  consumer->should_stop = true;

  if (uv_thread_join(&consumer->thread) != 0) {
    return (consumer_result_t){.success = false,
                               .msg = "Failed to join consumer thread"};
  }

  return (consumer_result_t){.success = true};
}

bool consumer_get_stats(consumer_t *consumer, uint64_t *processed_out) {
  if (!consumer || !processed_out) {
    LOG_WARN("Invalid arguments to consumer_get_stats");
    return false;
  }
  *processed_out = consumer->messages_processed;
  return true;
}