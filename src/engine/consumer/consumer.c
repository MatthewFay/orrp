#include "consumer.h"
#include "consumer_batch.h"
#include "consumer_cache_entry.h"
#include "consumer_ebr.h"
#include "consumer_schema.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/consumer/consumer_cache_internal.h"
#include "engine/consumer/consumer_flush.h"
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

#define MAX_BATCH_SIZE_PER_OP_QUEUE 128
#define MIN_RECLAIM_BATCH_SIZE 100
// TODO: writer enqueue re-tries
#define MAX_WRITER_ENQUEUE_ATTEMPTS 3

#define CONSUMER_CACHE_CAPACITY 65536

typedef enum {
  CONSUMER_PROCESS_SUCCESS,         // All succeeded
  CONSUMER_PROCESS_PARTIAL_FAILURE, // Some failed, some succeeded
  CONSUMER_PROCESS_FAILURE          // All failed (or critical error)
} consumer_process_status_t;

typedef struct {
  consumer_process_status_t status;
  uint32_t msgs_processed;
  uint32_t msgs_failed;
} consumer_process_result_t;

static consumer_cache_entry_t *_create_bm_entry(db_get_result_t *r,
                                                consumer_batch_db_key_t *key,
                                                op_queue_msg_t *msg) {
  bitmap_t *bm = NULL;
  consumer_cache_bitmap_t *cc_bm = calloc(1, sizeof(consumer_cache_bitmap_t));
  if (!cc_bm) {
    return NULL;
  }
  if (r->status == DB_GET_OK) {
    bm = bitmap_deserialize(r->value, r->value_len);
    if (!bm) {
      LOG_ACTION_ERROR(ACT_DESERIALIZATION_FAILED, "val_type=bitmap key=\"%s\"",
                       key->ser_db_key);
      free(cc_bm);
      return NULL;
    }
    LOG_ACTION_DEBUG(ACT_DB_READ,
                     "context=\"bitmap_load\" key=\"%s\" status=existing",
                     key->ser_db_key);
  } else {
    bm = bitmap_create();
    if (!bm) {
      LOG_ACTION_ERROR(ACT_MEMORY_ALLOC_FAILED,
                       "context=\"bitmap_create\" key=\"%s\"", key->ser_db_key);
      free(cc_bm);
      return NULL;
    }
    LOG_ACTION_DEBUG(ACT_CACHE_ENTRY_CREATED,
                     "context=\"bitmap_new\" key=\"%s\"", key->ser_db_key);
  }
  cc_bm->bitmap = bm;
  consumer_cache_entry_t *e = consumer_cache_create_entry_bitmap(
      &msg->op->db_key, msg->ser_db_key, cc_bm);
  if (!e) {
    LOG_ACTION_ERROR(ACT_CACHE_ENTRY_CREATE_FAILED,
                     "val_type=bitmap key=\"%s\"", msg->ser_db_key);
    bitmap_free(bm);
    free(cc_bm);
  }
  return e;
}

static consumer_cache_entry_t *_create_str_entry(db_get_result_t *r,
                                                 consumer_batch_db_key_t *key,
                                                 op_queue_msg_t *msg) {
  consumer_cache_str_t *cc_str = calloc(1, sizeof(consumer_cache_str_t));
  if (!cc_str) {
    return NULL;
  }
  if (r->status == DB_GET_OK) {
    cc_str->str = strdup(r->value);
    if (!cc_str->str) {
      LOG_ACTION_ERROR(ACT_MEMORY_ALLOC_FAILED,
                       "context=\"str_dup\" key=\"%s\"", key->ser_db_key);
      free(cc_str);
      return NULL;
    }
    LOG_ACTION_DEBUG(ACT_DB_READ,
                     "context=\"string_load\" key=\"%s\" status=existing",
                     key->ser_db_key);
  }
  consumer_cache_entry_t *e = consumer_cache_create_entry_str(
      &msg->op->db_key, msg->ser_db_key, cc_str);
  if (!e) {
    LOG_ACTION_ERROR(ACT_CACHE_ENTRY_CREATE_FAILED, "val_type=str key=\"%s\"",
                     msg->ser_db_key);
    free(cc_str->str);
    free(cc_str);
  }
  return e;
}

static consumer_cache_entry_t *_create_int32_entry(db_get_result_t *r,
                                                   consumer_batch_db_key_t *key,
                                                   op_queue_msg_t *msg) {
  uint32_t val = r->status == DB_GET_OK ? *(uint32_t *)r->value : 0;
  consumer_cache_entry_t *e =
      consumer_cache_create_entry_int32(&msg->op->db_key, msg->ser_db_key, val);
  if (!e) {
    LOG_ACTION_ERROR(ACT_CACHE_ENTRY_CREATE_FAILED, "val_type=int32 key=\"%s\"",
                     key->ser_db_key);
  }
  return e;
}

static consumer_cache_entry_t *
_get_or_Create_cache_entry(eng_container_t *dc, consumer_cache_t *cache,
                           consumer_batch_db_key_t *key, op_queue_msg_t *msg,
                           MDB_txn *txn, bool *was_cached_out) {
  db_get_result_t r;
  MDB_dbi db;
  consumer_cache_entry_t *cached_entry = NULL;

  *was_cached_out = false;

  if (consumer_cache_get_entry(cache, key->ser_db_key, &cached_entry)) {
    *was_cached_out = true;
    LOG_ACTION_DEBUG(ACT_CACHE_HIT, "key=\"%s\"", key->ser_db_key);
    return cached_entry;
  }
  LOG_ACTION_DEBUG(ACT_CACHE_MISS, "key=\"%s\"", key->ser_db_key);

  if (msg->op->db_key.dc_type == CONTAINER_TYPE_USER) {
    if (!container_get_user_db_handle(dc, msg->op->db_key.user_db_type, &db)) {
      LOG_ACTION_ERROR(ACT_DB_HANDLE_FAILED, "container_type=user db_type=%d",
                       msg->op->db_key.user_db_type);
      return NULL;
    }
  } else {
    if (!container_get_system_db_handle(dc, msg->op->db_key.sys_db_type, &db)) {
      LOG_ACTION_ERROR(ACT_DB_HANDLE_FAILED, "container_type=system db_type=%d",
                       msg->op->db_key.sys_db_type);
      return NULL;
    }
  }

  if (!db_get(db, txn, &msg->op->db_key.db_key, &r)) {
    LOG_ACTION_ERROR(ACT_DB_READ_FAILED, "key=\"%s\"", key->ser_db_key);
    return NULL;
  }

  consumer_cache_entry_val_type_t val_type =
      consumer_schema_get_value_type(&msg->op->db_key);

  consumer_cache_entry_t *entry = NULL;

  switch (val_type) {
  case CONSUMER_CACHE_ENTRY_VAL_BM:
    entry = _create_bm_entry(&r, key, msg);
    break;
  case CONSUMER_CACHE_ENTRY_VAL_INT32:
    entry = _create_int32_entry(&r, key, msg);
    break;
  case CONSUMER_CACHE_ENTRY_VAL_STR:
    entry = _create_str_entry(&r, key, msg);
    break;
  default:
    break;
  }

  db_get_result_clear(&r);
  return entry;
}

static void _fail_all_batch_db_key_msgs(consumer_process_result_t *result,
                                        consumer_batch_db_key_t *batch_db_key) {
  result->msgs_failed += batch_db_key->count;
}

static bool _try_evict(consumer_t *consumer) {
  if (consumer->cache.n_entries >= CONSUMER_CACHE_CAPACITY) {
    consumer_cache_entry_t *victim = consumer_cache_evict_lru(&consumer->cache);
    if (victim) {
      LOG_ACTION_DEBUG(ACT_CACHE_ENTRY_EVICTED, "key=\"%s\"",
                       victim->ser_db_key);
      if (victim->val_type == CONSUMER_CACHE_ENTRY_VAL_BM) {
        consumer_ebr_retire_bitmap(&consumer->consumer_epoch_record,
                                   &victim->val.cc_bitmap->epoch_entry);
      } else if (victim->val_type == CONSUMER_CACHE_ENTRY_VAL_STR) {
        consumer_ebr_retire_str(&consumer->consumer_epoch_record,
                                &victim->val.cc_bitmap->epoch_entry);
      }
      consumer_cache_free_entry(victim);
      return true;
    } else {
      LOG_ACTION_WARN(ACT_CACHE_ENTRY_EVICT_FAILED, "n_entries=%d capacity=%d",
                      consumer->cache.n_entries, CONSUMER_CACHE_CAPACITY);
    }
  }
  return false;
}

static void _process_int32_ops(consumer_t *consumer,
                               consumer_process_result_t *result,
                               consumer_cache_entry_t *cache_entry,
                               consumer_batch_db_key_t *batch_db_key,
                               bool was_cached) {
  uint32_t current_val = atomic_load(&cache_entry->val.int32);
  uint32_t new_val = current_val;
  uint32_t msgs_processed = 0;
  uint32_t msgs_failed = 0;
  bool dirty = false;

  // Apply all operations
  consumer_batch_msg_node_t *batch_node = batch_db_key->head;
  while (batch_node) {
    op_queue_msg_t *msg = batch_node->msg;

    switch (msg->op->op_type) {
    case OP_TYPE_CACHE:
      msgs_processed++;
      break;

    case OP_TYPE_ADD_VALUE:
      new_val += msg->op->value.int32;
      dirty = true;
      msgs_processed++;
      break;

    case OP_TYPE_PUT:
      new_val = msg->op->value.int32;
      dirty = true;
      msgs_processed++;
      break;

    case OP_TYPE_COND_PUT:
      if (msg->op->cond_type == COND_PUT_IF_EXISTING_LESS_THAN) {
        if (new_val < msg->op->value.int32) {
          new_val = msg->op->value.int32;
          dirty = true;
        }
      }
      msgs_processed++;
      break;

    default:
      LOG_ACTION_ERROR(ACT_OP_REJECTED, "op_type=%d key=\"%s\"",
                       msg->op->op_type, batch_db_key->ser_db_key);
      msgs_failed++;
      break;
    }
    batch_node = batch_node->next;
  }

  // If we modified the value, commit it
  if (dirty) {
    atomic_store(&cache_entry->val.int32, new_val);
    cache_entry->version++;

    consumer_cache_add_entry_to_dirty_list(&consumer->cache, cache_entry);

    LOG_ACTION_DEBUG(ACT_OP_APPLIED,
                     "context=\"int32_ops\" count=%u key=\"%s\" version=%llu "
                     "old_val=%u new_val=%u",
                     msgs_processed, batch_db_key->ser_db_key,
                     cache_entry->version, current_val, new_val);
  }

  result->msgs_processed += msgs_processed;
  result->msgs_failed += msgs_failed;

  // Add to cache if new entry
  if (!was_cached) {
    _try_evict(consumer);

    if (!consumer_cache_add_entry(&consumer->cache, batch_db_key->ser_db_key,
                                  cache_entry)) {
      LOG_ACTION_ERROR(ACT_CACHE_ENTRY_ADD_FAILED, "key=\"%s\"",
                       batch_db_key->ser_db_key);
      consumer_cache_free_entry(cache_entry);

      return _fail_all_batch_db_key_msgs(result, batch_db_key);
    }
  }
}

static void _process_str_ops(consumer_t *consumer,
                             consumer_process_result_t *result,
                             consumer_cache_entry_t *cache_entry,
                             consumer_batch_db_key_t *batch_db_key,
                             bool was_cached) {
  consumer_cache_str_t *old_cc_str = atomic_load(&cache_entry->val.cc_str);
  char *last_str = NULL;
  uint32_t msgs_processed = 0;
  uint32_t msgs_failed = 0;

  // Find the last PUT (last write wins)
  consumer_batch_msg_node_t *batch_node = batch_db_key->head;
  while (batch_node) {
    op_queue_msg_t *msg = batch_node->msg;

    switch (msg->op->op_type) {
    case OP_TYPE_CACHE:
      msgs_processed++;
      break;
    case OP_TYPE_PUT:
      last_str = msg->op->value.str; // Don't copy yet, just track last one
      msgs_processed++;
      break;
    default:
      LOG_ACTION_ERROR(ACT_OP_REJECTED, "op_type=%d key=\"%s\"",
                       msg->op->op_type, batch_db_key->ser_db_key);
      msgs_failed++;
      break;
    }
    batch_node = batch_node->next;
  }

  // If we have a PUT to apply, do it once
  if (last_str) {
    consumer_cache_str_t *new_cc_str = calloc(1, sizeof(consumer_cache_str_t));
    if (!new_cc_str) {
      return _fail_all_batch_db_key_msgs(result, batch_db_key);
    }

    new_cc_str->str = strdup(last_str);
    if (!new_cc_str->str) {
      free(new_cc_str);
      return _fail_all_batch_db_key_msgs(result, batch_db_key);
    }

    atomic_store(&cache_entry->val.cc_str, new_cc_str);
    cache_entry->version++;

    if (was_cached && old_cc_str) {
      consumer_ebr_retire_str(&consumer->consumer_epoch_record,
                              &old_cc_str->epoch_entry);
    }

    consumer_cache_add_entry_to_dirty_list(&consumer->cache, cache_entry);

    LOG_ACTION_DEBUG(
        ACT_OP_APPLIED, "context=\"str_ops\" count=%u key=\"%s\" version=%llu",
        msgs_processed, batch_db_key->ser_db_key, cache_entry->version);
  }

  result->msgs_processed += msgs_processed;
  result->msgs_failed += msgs_failed;

  // Add to cache if new entry
  if (!was_cached) {
    _try_evict(consumer);

    if (!consumer_cache_add_entry(&consumer->cache, batch_db_key->ser_db_key,
                                  cache_entry)) {
      LOG_ACTION_ERROR(ACT_CACHE_ENTRY_ADD_FAILED, "key=\"%s\"",
                       batch_db_key->ser_db_key);

      consumer_cache_str_t *failed_str = atomic_load(&cache_entry->val.cc_str);
      if (failed_str) {
        free(failed_str->str);
        free(failed_str);
      }
      consumer_cache_free_entry(cache_entry);

      return _fail_all_batch_db_key_msgs(result, batch_db_key);
    }
  }
}

static void _process_bitmap_ops(consumer_t *consumer,
                                consumer_process_result_t *result,
                                consumer_cache_entry_t *cache_entry,
                                consumer_batch_db_key_t *batch_db_key,
                                bool was_cached) {
  op_queue_msg_t *msg;
  consumer_cache_bitmap_t *cc_bm = atomic_load(&cache_entry->val.cc_bitmap);
  bitmap_t *bm_copy = NULL;

  if (was_cached) {
    // if cached, create a copy because other threads could be using it
    bm_copy = bitmap_copy(cc_bm->bitmap);
    if (!bm_copy) {
      LOG_ACTION_ERROR(ACT_BITMAP_COPY_FAILED, "key=\"%s\"",
                       batch_db_key->ser_db_key);
      return _fail_all_batch_db_key_msgs(result, batch_db_key);
    }
  }

  consumer_batch_msg_node_t *batch_node = batch_db_key->head;
  bool dirty = false;
  uint32_t msgs_processed = 0;
  uint32_t msgs_failed = 0;
  while (batch_node) {
    msg = batch_node->msg;

    switch (msg->op->op_type) {
    case OP_TYPE_CACHE:
      // Will be cached, no work to do
      msgs_processed++;
      break;
    case OP_TYPE_ADD_VALUE:
      bitmap_add(bm_copy, msg->op->value.int32);
      dirty = true;
      msgs_processed++;
      break;
    default:
      LOG_ACTION_ERROR(ACT_OP_REJECTED, "op_type=%d key=\"%s\"",
                       msg->op->op_type, batch_db_key->ser_db_key);
      msgs_failed++;
      break;
    }
    batch_node = batch_node->next;
  }

  consumer_cache_bitmap_t *new_cc_bm;
  if (dirty) {
    new_cc_bm = calloc(1, sizeof(consumer_cache_bitmap_t));
    if (!new_cc_bm) {
      bitmap_free(bm_copy);
      return _fail_all_batch_db_key_msgs(result, batch_db_key);
    }
    new_cc_bm->bitmap = bm_copy;
    atomic_store(&cache_entry->val.cc_bitmap, new_cc_bm);
    cache_entry->version++;

    if (was_cached) {
      consumer_ebr_retire_bitmap(&consumer->consumer_epoch_record,
                                 &cc_bm->epoch_entry);
    }
    LOG_ACTION_DEBUG(ACT_OP_APPLIED,
                     "context=\"bitmap_ops\" count=%u key=\"%s\" version=%llu",
                     msgs_processed, batch_db_key->ser_db_key,
                     cache_entry->version);
    result->msgs_processed += msgs_processed;
    result->msgs_failed += msgs_failed;
  } else {
    bitmap_free(bm_copy);
  }

  if (dirty) {
    consumer_cache_add_entry_to_dirty_list(&consumer->cache, cache_entry);
  }

  if (was_cached) {
    return;
  }

  _try_evict(consumer);

  if (!consumer_cache_add_entry(&consumer->cache, batch_db_key->ser_db_key,
                                cache_entry)) {
    LOG_ACTION_ERROR(ACT_CACHE_ENTRY_ADD_FAILED, "key=\"%s\"",
                     batch_db_key->ser_db_key);
    bitmap_free(new_cc_bm->bitmap);
    free(new_cc_bm);
    consumer_cache_free_entry(cache_entry);

    return _fail_all_batch_db_key_msgs(result, batch_db_key);
  }
}

// Process all messages for a container
static void _process_op_msgs(consumer_t *consumer, eng_container_t *dc,
                             consumer_batch_db_key_t *key, MDB_txn *txn,
                             consumer_process_result_t *result) {
  consumer_batch_msg_node_t *batch_node = key->head;
  bool was_cached;

  consumer_cache_entry_t *cache_entry = _get_or_Create_cache_entry(
      dc, &consumer->cache, key, batch_node->msg, txn, &was_cached);
  if (!cache_entry) {
    LOG_ACTION_ERROR(ACT_CACHE_ENTRY_CREATE_FAILED, "key=\"%s\"",
                     key->ser_db_key);
    return _fail_all_batch_db_key_msgs(result, key);
  }

  switch (cache_entry->val_type) {
  case CONSUMER_CACHE_ENTRY_VAL_BM:
    _process_bitmap_ops(consumer, result, cache_entry, key, was_cached);
    break;
  case CONSUMER_CACHE_ENTRY_VAL_STR:
    _process_str_ops(consumer, result, cache_entry, key, was_cached);
    break;
  case CONSUMER_CACHE_ENTRY_VAL_INT32:
    _process_int32_ops(consumer, result, cache_entry, key, was_cached);
    break;
  default:
    return;
  }
}

static consumer_process_result_t
_process_container_batch(consumer_t *consumer, eng_container_t *dc,
                         MDB_txn *txn, consumer_batch_container_t *batch) {
  consumer_process_result_t result = {0};
  consumer_batch_db_key_t *batch_db_keys = batch->db_keys;
  consumer_batch_db_key_t *batch_db_key, *tmp;

  HASH_ITER(hh, batch_db_keys, batch_db_key, tmp) {
    _process_op_msgs(consumer, dc, batch_db_key, txn, &result);
  }

  if (result.msgs_processed == 0) {
    result.status = CONSUMER_PROCESS_FAILURE;
  } else if (result.msgs_failed > 0) {
    result.status = CONSUMER_PROCESS_PARTIAL_FAILURE;
  } else {
    result.status = CONSUMER_PROCESS_SUCCESS;
  }
  return result;
}

// returns number of msgs processed, or -1 on total failure
static int _process_batch(consumer_t *consumer,
                          consumer_batch_container_t *batch) {
  if (!batch || !batch->container_name || !batch->db_keys) {
    LOG_ACTION_WARN(ACT_BATCH_INVALID, "container=%p db_keys=%p",
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
    LOG_ACTION_ERROR(ACT_CONTAINER_OPEN_FAILED, "container=\"%s\"",
                     batch->container_name);
    return -1;
  }

  eng_container_t *dc = cr.container;

  MDB_txn *txn = db_create_txn(dc->env, true);
  if (!txn) {
    LOG_ACTION_ERROR(ACT_TXN_BEGIN, "err=\"failed\" container=\"%s\"",
                     batch->container_name);
    container_release(dc);
    return -1;
  }

  consumer_process_result_t result =
      _process_container_batch(consumer, dc, txn, batch);

  db_abort_txn(txn);
  container_release(dc);

  switch (result.status) {
  case CONSUMER_PROCESS_SUCCESS:
    LOG_ACTION_DEBUG(
        ACT_BATCH_PROCESSED,
        "container=\"%s\" msgs_processed=%u msgs_failed=%u status=success",
        batch->container_name, result.msgs_processed, result.msgs_failed);
    return result.msgs_processed;
  case CONSUMER_PROCESS_PARTIAL_FAILURE:
    LOG_ACTION_ERROR(ACT_BATCH_PROCESSED,
                     "container=\"%s\" msgs_processed=%u msgs_failed=%u "
                     "status=partial_failure",
                     batch->container_name, result.msgs_processed,
                     result.msgs_failed);
    return result.msgs_processed;
  case CONSUMER_PROCESS_FAILURE:
    LOG_ACTION_ERROR(ACT_BATCH_PROCESS_FAILED, "container=\"%s\"",
                     batch->container_name);
    return -1;
  default:
    LOG_ACTION_ERROR(ACT_BATCH_PROCESS_FAILED,
                     "container=\"%s\" err=\"unknown_status\"",
                     batch->container_name);
    return -1;
  }
}

static void _process_batches(consumer_t *consumer,
                             consumer_batch_container_t *container_table) {
  consumer_batch_container_t *batch, *tmp;
  uint32_t batches_processed = 0;
  uint32_t batches_failed = 0;

  HASH_ITER(hh, container_table, batch, tmp) {
    if (_process_batch(consumer, batch) > 0) {
      batches_processed++;
    } else {
      batches_failed++;
    }
  }

  LOG_ACTION_DEBUG(ACT_PERF_BATCH_COMPLETE,
                   "batches_processed=%u batches_failed=%u", batches_processed,
                   batches_failed);

  if (batches_failed > 0) {
    LOG_ACTION_ERROR(ACT_BATCH_PROCESS_FAILED,
                     "batches_processed=%u batches_failed=%u",
                     batches_processed, batches_failed);
  }
}

static void _flush_dirty(consumer_t *c) {
  uint32_t num_dirty_entries = c->cache.num_dirty_entries;
  if (num_dirty_entries < 1 || !c->cache.dirty_head) {
    return;
  }

  LOG_ACTION_DEBUG(ACT_FLUSH_STARTING, "num_dirty=%u", num_dirty_entries);

  consumer_flush_result_t fr =
      consumer_flush_prepare(c->cache.dirty_head, num_dirty_entries);
  if (!fr.success || !fr.msg) {
    LOG_ACTION_ERROR(ACT_FLUSH_FAILED, "err=\"%s\"", fr.err_msg);
    return;
  }

  if (fr.entries_skipped > 0) {
    LOG_ACTION_WARN(ACT_FLUSH_ENTRIES_SKIPPED, "count=%u", fr.entries_skipped);
  }

  if (fr.entries_prepared > 0) {
    if (!eng_writer_queue_enqueue(&c->config.writer->queue, fr.msg)) {
      LOG_ACTION_ERROR(ACT_FLUSH_FAILED,
                       "context=\"enqueue\" entries_prepared=%u",
                       fr.entries_prepared);
      /* Failed to enqueue - free the message we prepared */
      consumer_flush_clear_result(fr);
    } else {
      LOG_ACTION_INFO(ACT_PERF_FLUSH_COMPLETE,
                      "entries_flushed=%u entries_skipped=%u",
                      fr.entries_prepared, fr.entries_skipped);
      /* enqueued - ownership transferred to writer; do not free */
    }
  } else {
    LOG_ACTION_WARN(ACT_FLUSH_FAILED,
                    "context=\"no_entries\" entries_skipped=%u",
                    fr.entries_skipped);
  }
  consumer_cache_clear_dirty_list(&c->cache);
}

static void _reclamation(consumer_t *consumer) {
  uint32_t pending = consumer->consumer_epoch_record.n_pending;
  if (pending >= MIN_RECLAIM_BATCH_SIZE) {
    LOG_ACTION_DEBUG(ACT_EBR_RECLAIM, "pending=%u", pending);
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

  LOG_ACTION_INFO(ACT_THREAD_STARTED, "thread_type=consumer consumer_id=%d",
                  config->consumer_id);

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

  const uint32_t max_msgs =
      MAX_BATCH_SIZE_PER_OP_QUEUE * config->op_queue_consume_count;
  op_queue_msg_t *op_msgs[max_msgs];
  uint32_t op_msg_count = 0;

  while (!consumer->should_stop) {
    batched_any = false;
    msgs_batched = 0;
    op_msg_count = 0; // Reset message tracking for this cycle
    cycle++;
    total_cycles++;

    for (uint32_t i = 0; i < consumer->config.op_queue_consume_count; i++) {
      uint32_t op_queue_idx = consumer->config.op_queue_consume_start + i;
      op_queue_t *queue = &consumer->config.op_queues[op_queue_idx];

      for (int j = 0; j < MAX_BATCH_SIZE_PER_OP_QUEUE; j++) {
        if (!op_queue_dequeue(queue, &msg)) {
          break; // No more messages in this shard
        }

        // Track this message for cleanup
        op_msgs[op_msg_count++] = msg;

        schema_validation_result_t result = consumer_schema_validate_msg(msg);
        if (!result.valid) {
          const char *msg_key =
              (msg && msg->ser_db_key) ? msg->ser_db_key : "unknown";
          LOG_ACTION_ERROR(ACT_OP_VALIDATION_FAILED, "key=\"%s\" err=\"%s\"",
                           msg_key, result.error_msg);
          continue;
        }

        if (!consumer_batch_add_msg(&container_table, msg)) {
          LOG_ACTION_ERROR(ACT_BATCH_ADD_FAILED, "container=\"%s\"",
                           msg->op->db_key.container_name);
          continue;
        }

        batched_any = true;
        msgs_batched++;
      }
    }

    if (batched_any) {
      LOG_ACTION_DEBUG(ACT_BATCH_CREATED, "msgs_batched=%u", msgs_batched);
      backoff = 1;
      spin_count = 0;
      active_cycles++;
      _process_batches(consumer, container_table);
      consumer_batch_free_table(container_table);
      container_table = NULL;
    } else {
      consumer_batch_free_table(container_table);
      container_table = NULL;

      if (spin_count < CONSUMER_SPIN_LIMIT) {
        sched_yield();
        spin_count++;
      } else {
        uv_sleep(backoff);
        backoff = backoff < CONSUMER_MAX_SLEEP_MS ? backoff * 2
                                                  : CONSUMER_MAX_SLEEP_MS;
      }
    }

    for (uint32_t i = 0; i < op_msg_count; i++) {
      op_queue_msg_free(op_msgs[i]);
    }

    if (cycle == config->flush_every_n) {
      cycle = 0;
      _flush_dirty(consumer);
      _reclamation(consumer);

      // Periodic stats
      if (total_cycles % 100000 == 0) {
        double active_pct = (active_cycles * 100.0) / total_cycles;
        LOG_ACTION_INFO(ACT_CONSUMER_STATS,
                        "total_cycles=%llu active_pct=%.1f cache_entries=%u",
                        total_cycles, active_pct, consumer->cache.n_entries);
      }
    }
  }

  consumer_ebr_unregister(&consumer->consumer_epoch_record);
  consumer_cache_destroy(&consumer->cache);

  LOG_ACTION_INFO(ACT_THREAD_STOPPED, "thread_type=consumer total_cycles=%llu",
                  total_cycles);
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
    LOG_ACTION_WARN(ACT_INVALID_ARGS, "function=consumer_get_stats");
    return false;
  }
  *processed_out = consumer->messages_processed;
  return true;
}