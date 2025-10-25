#include "consumer_processor.h"
#include "core/db.h"
#include "engine/consumer/consumer.h"
#include "engine/consumer/consumer_cache_entry.h"
#include "engine/container/container.h"

static consumer_cache_entry_val_type_t _map_op_type_to_cache_type() {}

static consumer_cache_entry_t *
_get_or_Create_cache_entry(eng_container_t *dc, consumer_cache_t *cache,
                           consumer_batch_db_key_t *batch_db_key,
                           op_queue_msg_t *msg, MDB_txn *txn,
                           bool *was_cached_out) {
  db_get_result_t r;

  MDB_dbi db;
  *was_cached_out = false;
  consumer_cache_entry_t *cached_entry = NULL;

  if (consumer_cache_get_entry(cache, batch_db_key->ser_db_key,
                               &cached_entry)) {
    *was_cached_out = true;
    return cached_entry;
  }

  if (msg->op->db_key.dc_type == CONTAINER_TYPE_USER) {
    if (!container_get_user_db_handle(dc, msg->op->db_key.user_db_type, &db)) {
      return NULL;
    }
  } else {
    if (!container_get_system_db_handle(dc, msg->op->db_key.sys_db_type, &db)) {
      return NULL;
    }
  }

  if (!db_get(db, txn, &msg->op->db_key.db_key, &r)) {
    return NULL;
  }

  consumer_cache_entry_t *new_cache_entry =
      consumer_cache_create_entry(&msg->op->db_key, msg->ser_db_key);
  if (!new_cache_entry) {
    return (COP_RESULT_T){.success = false,
                          .err_msg = "Failed to create cache entry for key"};
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

static void _fail_all_batch_db_key_msgs(consumer_process_result_t *result,
                                        consumer_batch_db_key_t *batch_db_key) {
  result->msgs_failed += batch_db_key->count;
}

static void _process_op_msgs(consumer_cache_t *cache,
                             consumer_batch_db_key_t *batch_db_key,
                             eng_container_t *dc, MDB_txn *txn,
                             ck_epoch_record_t *consumer_epoch_record,
                             consumer_process_result_t *result) {
  bool was_cached = false;
  consumer_batch_msg_node_t *node = batch_db_key->head;
  if (!node || !node->msg) {
    return _fail_all_batch_db_key_msgs(result, batch_db_key);
  }

  consumer_cache_entry_t *cache_entry = _get_or_Create_cache_entry(
      dc, cache, batch_db_key, node->msg, txn, &was_cached);
  if (!cache_entry) {
    return _fail_all_batch_db_key_msgs(result, batch_db_key);
  }

  //  db_get_result_clear(&r);

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

  while (node) {
    msg = node->msg;

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
    node = node->next;
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
    consumer_cache_entry_t *victim = consumer_cache_evict_lru(&consumer->cache);
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

static void _finalize_result(consumer_process_result_t *result) {
  if (result->msgs_processed == 0) {
    result->status = CONSUMER_PROCESS_FAILURE;
  } else if (result->msgs_failed > 0) {
    result->status = CONSUMER_PROCESS_PARTIAL_FAILURE;
  } else {
    result->status = CONSUMER_PROCESS_SUCCESS;
  }

  if (!result->err_msg && result->msgs_processed == 0) {
    result->err_msg = "All messages failed to process.";
  }
}

consumer_process_result_t
consumer_process_container_batch(consumer_cache_t *cache, eng_container_t *dc,
                                 MDB_txn *txn,
                                 ck_epoch_record_t *consumer_epoch_record,
                                 consumer_batch_container_t *batch) {
  if (!cache || !dc || !txn || !consumer_epoch_record) {
    return (consumer_process_result_t){.status = CONSUMER_PROCESS_FAILURE,
                                       .err_msg = "Invalid process args!"

    };
  }
  if (!batch || !batch->db_keys || !batch->container_name) {
    return (consumer_process_result_t){.status = CONSUMER_PROCESS_FAILURE,
                                       .err_msg = "Invalid batch!"

    };
  }

  consumer_process_result_t result = {0};
  consumer_batch_db_key_t *batch_db_keys = batch->db_keys;
  consumer_batch_db_key_t *batch_db_key, *tmp;

  HASH_ITER(hh, batch_db_keys, batch_db_key, tmp) {
    _process_op_msgs(cache, batch_db_key, dc, txn, consumer_epoch_record,
                     &result);
  }

  _finalize_result(&result);
  return result;
}