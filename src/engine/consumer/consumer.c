#include "consumer.h"
#include "consumer_cache_entry.h"
#include "consumer_ebr.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/consumer/consumer_cache_internal.h"
#include "engine/container/container.h"
#include "engine/dc_cache/dc_cache.h"
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

typedef struct op_queue_msg_batch_entry_s {
  op_queue_msg_t *msg;
  struct op_queue_msg_batch_entry_s *next;
} op_queue_msg_batch_entry_t;

typedef struct {
  UT_hash_handle hh;
  const char *ser_db_key; // hash key
  op_queue_msg_batch_entry_t *head;
  op_queue_msg_batch_entry_t *tail;
} op_queue_msg_batch_key_t;

typedef struct {
  UT_hash_handle hh;
  const char *container_name;     // outer hash key
  op_queue_msg_batch_key_t *keys; // inner hash table
} op_queue_msg_batch_t;

static consumer_cache_entry_t *
_get_or_Create_cache_entry(consumer_cache_t *cache,
                           op_queue_msg_batch_key_t *key, op_queue_msg_t *msg,
                           MDB_dbi db, MDB_txn *txn, bool *was_cached_out) {
  *was_cached_out = false;

  db_get_result_t r;
  consumer_cache_entry_t *cached_entry = NULL;
  if (!consumer_cache_get_entry(cache, key->ser_db_key, &cached_entry)) {
    LOG_ERROR("Failed to lookup cache entry for key: %s", key->ser_db_key);
    return NULL;
  }
  if (cached_entry) {
    *was_cached_out = true;
    LOG_DEBUG("Cache hit for key: %s", key->ser_db_key);
    return cached_entry;
  }

  LOG_DEBUG("Cache miss for key: %s", key->ser_db_key);

  if (!db_get(db, txn, &msg->op->db_key.db_key, &r)) {
    LOG_ERROR("Failed to get DB value for key: %s", key->ser_db_key);
    return NULL;
  }

  consumer_cache_entry_t *new_cache_entry =
      consumer_cache_create_entry(&msg->op->db_key, key->ser_db_key);
  if (!new_cache_entry) {
    LOG_ERROR("Failed to create cache entry for key: %s", key->ser_db_key);
    return NULL;
  }

  if (r.status == DB_GET_OK) {
    bitmap_t *db_bm = bitmap_deserialize(r.value, r.value_len);
    if (!db_bm) {
      LOG_ERROR("Failed to deserialize bitmap for key: %s", key->ser_db_key);
      consumer_cache_free_entry(new_cache_entry);
      return NULL;
    }
    atomic_store(&new_cache_entry->bitmap, db_bm);
    LOG_DEBUG("Loaded existing bitmap from DB for key: %s", key->ser_db_key);
    return new_cache_entry;
  }

  bitmap_t *new_BITMAP = bitmap_create();
  if (!new_BITMAP) {
    LOG_ERROR("Failed to create new bitmap for key: %s", key->ser_db_key);
    consumer_cache_free_entry(new_cache_entry);
    return NULL;
  }
  atomic_store(&new_cache_entry->bitmap, new_BITMAP);
  LOG_DEBUG("Created new bitmap for key: %s", key->ser_db_key);
  return new_cache_entry;
}

// Process all messages for a container
static bool _process_cache_msgs(consumer_t *consumer, eng_container_t *dc,
                                op_queue_msg_batch_key_t *keys, MDB_txn *txn) {
  op_queue_msg_batch_key_t *key, *tmp;
  op_queue_msg_t *msg;
  uint32_t keys_processed = 0;
  uint32_t ops_applied = 0;

  HASH_ITER(hh, keys, key, tmp) {
    bool was_cached;
    op_queue_msg_batch_entry_t *b_entry = key->head;
    MDB_dbi db;

    if (!eng_container_get_db_handle(dc, b_entry->msg->op->db_key.user_db_type,
                                     &db)) {
      LOG_ERROR("Failed to get DB handle for key: %s", key->ser_db_key);
      return false;
    }

    consumer_cache_entry_t *cache_entry = _get_or_Create_cache_entry(
        &consumer->cache, key, b_entry->msg, db, txn, &was_cached);
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
      case BM_CACHE:
        // Will be cached, no work to do
        LOG_DEBUG("BM_CACHE op for key: %s", key->ser_db_key);
        break;
      case BM_ADD_VALUE:
        bitmap_add(bm, msg->op->data.int32_value);
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
        consumer_ebr_retire(&consumer->consumer_cache_thread_epoch_record,
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
        consumer_ebr_retire(&consumer->consumer_cache_thread_epoch_record,
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
static int _process_batch(consumer_t *consumer, op_queue_msg_batch_t *batch) {
  if (!batch || !batch->container_name || !batch->keys) {
    LOG_WARN("Invalid batch: container=%p, keys=%p",
             batch ? batch->container_name : NULL, batch ? batch->keys : NULL);
    return -1;
  }

  int n = 0;
  op_queue_msg_batch_key_t *keys = batch->keys;

  eng_container_t *dc = eng_dc_cache_get(batch->container_name);
  if (!dc) {
    LOG_ERROR("Failed to get container from cache: %s", batch->container_name);
    return -1;
  }

  MDB_txn *txn = db_create_txn(dc->env, true);
  if (!txn) {
    LOG_ERROR("Failed to create transaction for container: %s",
              batch->container_name);
    eng_dc_cache_release_container(dc);
    return -1;
  }

  if (!_process_cache_msgs(consumer, dc, keys, txn)) {
    LOG_ERROR("Failed to process cache messages for container: %s",
              batch->container_name);
    db_abort_txn(txn);
    eng_dc_cache_release_container(dc);
    return -1;
  }

  db_abort_txn(txn);
  eng_dc_cache_release_container(dc);

  LOG_DEBUG("Processed batch for container: %s", batch->container_name);
  return n;
}

static void _process_batches(consumer_t *consumer,
                             op_queue_msg_batch_t *batch_hash) {
  op_queue_msg_batch_t *batch, *tmp;
  uint32_t batches_processed = 0;
  uint32_t batches_failed = 0;

  HASH_ITER(hh, batch_hash, batch, tmp) {
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

static op_queue_msg_batch_t *_create_batch(const char *container_name) {
  op_queue_msg_batch_t *batch = calloc(1, sizeof(op_queue_msg_batch_t));
  if (!batch) {
    LOG_ERROR("Failed to allocate batch for container: %s", container_name);
    return NULL;
  }
  batch->container_name = container_name;
  batch->keys = NULL;
  return batch;
}

static op_queue_msg_batch_entry_t *_create_batch_entry(op_queue_msg_t *msg) {
  op_queue_msg_batch_entry_t *m = calloc(1, sizeof(op_queue_msg_batch_entry_t));
  if (!m) {
    LOG_ERROR("Failed to allocate batch entry");
    return NULL;
  }
  m->msg = msg;
  m->next = NULL;
  return m;
}

static bool _add_msg_to_batch(op_queue_msg_batch_t *batch,
                              op_queue_msg_t *msg) {
  bool new_key = false;
  op_queue_msg_batch_key_t *key = NULL;
  HASH_FIND_STR(batch->keys, msg->ser_db_key, key);
  if (!key) {
    new_key = true;
    key = calloc(1, sizeof(op_queue_msg_batch_key_t));
    if (!key) {
      LOG_ERROR("Failed to allocate batch key for: %s", msg->ser_db_key);
      return false;
    }
    key->ser_db_key = msg->ser_db_key;
    key->head = NULL;
    key->tail = NULL;
    HASH_ADD_KEYPTR(hh, batch->keys, msg->ser_db_key, strlen(msg->ser_db_key),
                    key);
  }

  op_queue_msg_batch_entry_t *m = _create_batch_entry(msg);
  if (!m) {
    if (new_key)
      free(key);
    return false;
  }

  if (!key->head) {
    key->head = m;
    key->tail = m;
    return true;
  }
  key->tail->next = m;
  key->tail = m;

  return true;
}

// Also free's consumed `op` msgs
static void _free_batch_hash(op_queue_msg_batch_t *batch_hash) {
  if (!batch_hash)
    return;
  op_queue_msg_batch_t *b, *tmp;
  op_queue_msg_batch_key_t *k, *tmp_k;
  op_queue_msg_batch_entry_t *b_entry, *b_entry_tmp;
  HASH_ITER(hh, batch_hash, b, tmp) {
    op_queue_msg_batch_key_t *keys = b->keys;
    HASH_ITER(hh, keys, k, tmp_k) {
      b_entry = k->head;
      while (b_entry) {
        b_entry_tmp = b_entry->next;
        op_queue_msg_free(b_entry->msg);
        free(b_entry);
        b_entry = b_entry_tmp;
      }
      HASH_DEL(keys, k);
      free(k);
    }
    HASH_DEL(batch_hash, b);
    free(b);
  }
}

static bool _batch_by_container(const consumer_t *consumer,
                                op_queue_msg_batch_t **batch_hash_out,
                                bool *batched_any_out) {
  op_queue_msg_t *msg;
  op_queue_msg_batch_t *batch = NULL;
  uint32_t msgs_batched = 0;

  for (uint32_t i = 0; i < consumer->config.op_queue_consume_count; i++) {
    uint32_t op_queue_idx = consumer->config.op_queue_consume_start + i;
    op_queue_t *queue = &consumer->config.op_queues[op_queue_idx];

    for (int j = 0; j < MAX_BATCH_SIZE_PER_OP_Queue; j++) {
      if (!op_queue_dequeue(queue, &msg)) {
        break; // No more messages in this shard
      }
      batch = NULL;
      HASH_FIND_STR(*batch_hash_out, msg->op->db_key.container_name, batch);
      if (batch) {
        if (!_add_msg_to_batch(batch, msg)) {
          LOG_ERROR("Failed to add message to existing batch for container: %s",
                    msg->op->db_key.container_name);
          _free_batch_hash(*batch_hash_out);
          return false;
        }
      } else {
        batch = _create_batch(msg->op->db_key.container_name);
        if (!batch || !_add_msg_to_batch(batch, msg)) {
          LOG_ERROR("Failed to create batch for container: %s",
                    msg->op->db_key.container_name);
          _free_batch_hash(*batch_hash_out);
          return false;
        }
        HASH_ADD_KEYPTR(hh, *batch_hash_out, batch->container_name,
                        strlen(batch->container_name), batch);
      }
      *batched_any_out = true;
      msgs_batched++;
    }
  }

  if (msgs_batched > 0) {
    LOG_DEBUG("Batched %u messages from queues", msgs_batched);
  }

  return true;
}

static void _flush(consumer_t *c) {
  uint32_t count = c->cache.num_dirty_entries;
  if (count < 1 || !c->cache.dirty_head) {
    return;
  }

  LOG_DEBUG("Flushing %u dirty cache entries to writer", count);

  eng_writer_msg_t *msg = malloc(sizeof(eng_writer_msg_t));
  if (!msg) {
    LOG_ERROR("Failed to allocate writer message for flush");
    return;
  }

  msg->entries = malloc(sizeof(eng_writer_entry_t) * count);
  if (!msg->entries) {
    LOG_ERROR("Failed to allocate writer entries array for %u entries", count);
    free(msg);
    return;
  }

  msg->count = 0;
  uint32_t skipped = 0;

  for (consumer_cache_entry_t *cache_entry = c->cache.dirty_head; cache_entry;
       cache_entry = cache_entry->dirty_next) {
    bitmap_t *bm = atomic_load(&cache_entry->bitmap);
    if (!bm) {
      skipped++;
      continue;
    }
    eng_writer_entry_t *entry = &msg->entries[msg->count];
    entry->bitmap_copy = bitmap_copy(bm);
    if (!entry->bitmap_copy) {
      LOG_WARN("Failed to copy bitmap for flush: %s", cache_entry->ser_db_key);
      skipped++;
      continue;
    }
    entry->flush_version_ptr = &cache_entry->flush_version;
    entry->db_key = cache_entry->db_key;
    entry->db_key.container_name = strdup(cache_entry->db_key.container_name);
    if (!entry->db_key.container_name) {
      LOG_WARN("Failed to duplicate container name for flush");
      bitmap_free(entry->bitmap_copy);
      skipped++;
      continue;
    }
    if (cache_entry->db_key.db_key.type == DB_KEY_STRING) {
      entry->db_key.db_key.key.s = strdup(cache_entry->db_key.db_key.key.s);
      if (!entry->db_key.db_key.key.s) {
        LOG_WARN("Failed to duplicate DB key string for flush");
        free((void *)entry->db_key.container_name);
        bitmap_free(entry->bitmap_copy);
        skipped++;
        continue;
      }
    }
    msg->count++;
  }

  if (msg->count > 0) {
    if (!eng_writer_queue_enqueue(&c->config.writer->queue, msg)) {
      LOG_ERROR("Failed to enqueue flush message with %u entries to writer",
                msg->count);
      // TODO: cleanup allocated resources
    } else {
      LOG_INFO("Flushed %u entries to writer (%u skipped)", msg->count,
               skipped);
    }
  } else {
    LOG_WARN("Flush prepared but no entries successfully copied (%u skipped)",
             skipped);
    free(msg->entries);
    free(msg);
  }

  consumer_cache_clear_dirty_list(&c->cache);
}

static void _reclamation(consumer_t *consumer) {
  uint32_t pending = consumer->consumer_cache_thread_epoch_record.n_pending;
  if (pending >= MIN_RECLAIM_BATCH_SIZE) {
    LOG_DEBUG("Running EBR reclamation with %u pending entries", pending);
    consumer_ebr_reclaim(&consumer->consumer_cache_thread_epoch_record);
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
  consumer_ebr_register(&consumer->epoch,
                        &consumer->consumer_cache_thread_epoch_record);

  op_queue_msg_batch_t *batch_hash = NULL;
  bool batched_any = false;
  uint32_t cycle = 0;
  int backoff = 1;
  int spin_count = 0;
  uint64_t total_cycles = 0;
  uint64_t active_cycles = 0;

  while (!consumer->should_stop) {
    batched_any = false;
    cycle++;
    total_cycles++;

    if (!_batch_by_container(consumer, &batch_hash, &batched_any)) {
      LOG_ERROR("Failed to batch messages by container");
    }

    if (batched_any) {
      backoff = 1;
      spin_count = 0;
      active_cycles++;
      _process_batches(consumer, batch_hash);
      _free_batch_hash(batch_hash);
      batch_hash = NULL;
    } else {
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
      _flush(consumer);
      _reclamation(consumer);

      // Periodic stats
      if (total_cycles % 100000 == 0) {
        double active_pct = (active_cycles * 100.0) / total_cycles;
        LOG_INFO("Consumer stats: cycles=%llu, active=%.1f%%, cache_entries=%u",
                 total_cycles, active_pct, consumer->cache.n_entries);
      }
    }
  }

  consumer_ebr_unregister(&consumer->consumer_cache_thread_epoch_record);
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