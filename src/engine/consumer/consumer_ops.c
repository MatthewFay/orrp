#include "consumer_ops.h"
#include "core/db.h"
#include "engine/consumer/consumer_cache_entry.h"
#include "engine/consumer/consumer_cache_internal.h"
#include "engine/container/container.h"
#include "engine/container/container_types.h"

// can be multiple cache entries per op

// static consumer_cache_entry_val_type_t _

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

COP_RESULT_T cops_create_entry_from_op_msg(eng_container_t *dc,
                                           op_queue_msg_t *msg, MDB_txn *txn) {
  db_get_result_t r;
  MDB_dbi db;

  if (!dc || !msg || !txn) {
    return (COP_RESULT_T){.success = false,
                          .err_msg = "Create entry from op msg: Invalid args"};
  }

  if (msg->op->db_key.dc_type == CONTAINER_TYPE_USER) {
    if (!container_get_user_db_handle(dc, msg->op->db_key.user_db_type, &db)) {
      return (COP_RESULT_T){.success = false,
                            .err_msg = "Failed to get DB handle for key"};
    }

  } else {
    if (!container_get_system_db_handle(dc, msg->op->db_key.sys_db_type, &db)) {
      return (COP_RESULT_T){.success = false,
                            .err_msg = "Failed to get DB handle for key"};
    }
  }

  if (!db_get(db, txn, &msg->op->db_key.db_key, &r)) {
    return (COP_RESULT_T){.success = false,
                          .err_msg = "Failed to get DB value for key"};
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

static bool _process_op_msgs(consumer_batch_db_key_t *key) {
  bool was_cached;
  consumer_batch_msg_node_t *b_entry = key->head;
}

COP_RESULT_T process_batch(consumer_batch_container_t *batch) {
  consumer_batch_db_key_t *key, *tmp;
  op_queue_msg_t *msg;
  uint32_t keys_processed = 0;
  uint32_t ops_applied = 0;

  if (!batch || !batch->container_name || !batch->keys) {
    return (COP_RESULT_T){.success = false, .err_msg = "Invalid batch"};
  }
  container_result_t cr;
  if (batch->container_type == CONTAINER_TYPE_SYSTEM) {
    cr = container_get_system();
  } else {
    cr = container_get_or_create_user(batch->container_name);
  }
  if (!cr.success) {
    return (COP_RESULT_T){.success = false,
                          .err_msg = "Failed to get container from cache"};
  }
  MDB_txn *txn = db_create_txn(cr.container->env, true);
  if (!txn) {
    container_release(cr.container);
    return (COP_RESULT_T){.success = false,
                          .err_msg = "Failed to create transaction"};
  }
  consumer_batch_db_key_t *keys = batch->keys;

  HASH_ITER(hh, keys, key, tmp) {}

  db_abort_txn(txn);
  container_release(cr.container);
}