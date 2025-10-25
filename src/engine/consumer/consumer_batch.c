#include "consumer_batch.h"

void consumer_batch_free_table(consumer_batch_container_t *container_table) {
  if (!container_table)
    return;
  consumer_batch_container_t *b, *tmp;
  consumer_batch_db_key_t *k, *tmp_k;
  consumer_batch_msg_node_t *b_entry, *b_entry_tmp;
  HASH_ITER(hh, container_table, b, tmp) {
    consumer_batch_db_key_t *keys = b->db_keys;
    HASH_ITER(hh, keys, k, tmp_k) {
      b_entry = k->head;
      while (b_entry) {
        b_entry_tmp = b_entry->next;
        op_queue_msg_free(b_entry->msg, true);
        free(b_entry);
        b_entry = b_entry_tmp;
      }
      HASH_DEL(keys, k);
      free(k);
    }
    HASH_DEL(container_table, b);
    free(b);
  }
}

static consumer_batch_container_t *_create_batch(const char *container_name,
                                                 eng_dc_type_t container_type) {
  consumer_batch_container_t *batch =
      calloc(1, sizeof(consumer_batch_container_t));
  if (!batch) {
    return NULL;
  }
  batch->container_name = container_name;
  batch->container_type = container_type;
  batch->db_keys = NULL;
  return batch;
}

static consumer_batch_msg_node_t *_create_batch_entry(op_queue_msg_t *msg) {
  consumer_batch_msg_node_t *m = calloc(1, sizeof(consumer_batch_msg_node_t));
  if (!m) {
    return NULL;
  }
  m->msg = msg;
  m->next = NULL;
  return m;
}

static bool _add_msg_to_batch(consumer_batch_container_t *batch,
                              op_queue_msg_t *msg) {
  bool new_key = false;
  consumer_batch_db_key_t *key = NULL;
  HASH_FIND_STR(batch->db_keys, msg->ser_db_key, key);
  if (!key) {
    new_key = true;
    key = calloc(1, sizeof(consumer_batch_db_key_t));
    if (!key) {
      return false;
    }
    key->ser_db_key = msg->ser_db_key;
    key->head = NULL;
    key->tail = NULL;
    HASH_ADD_KEYPTR(hh, batch->db_keys, msg->ser_db_key,
                    strlen(msg->ser_db_key), key);
  }

  consumer_batch_msg_node_t *m = _create_batch_entry(msg);
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
  key->count++;

  return true;
}

bool consumer_batch_add_msg(consumer_batch_container_t **container_table,
                            op_queue_msg_t *msg) {
  consumer_batch_container_t *batch = NULL;

  if (!container_table || !msg)
    return false;

  HASH_FIND_STR(*container_table, msg->op->db_key.container_name, batch);
  if (batch && !_add_msg_to_batch(batch, msg)) {
    return false;
  } else {
    batch =
        _create_batch(msg->op->db_key.container_name, msg->op->db_key.dc_type);
    if (!batch || !_add_msg_to_batch(batch, msg)) {
      return false;
    }
    HASH_ADD_KEYPTR(hh, *container_table, batch->container_name,
                    strlen(batch->container_name), batch);
  }

  return true;
}