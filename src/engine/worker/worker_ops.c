#include "engine/worker/worker_ops.h"
#include "core/db.h"
#include "engine/container/container.h"
#include "engine/container/container_types.h"
#include "engine/eng_key_format/eng_key_format.h"
#include "engine/op/op.h"
#include "engine/op_queue/op_queue_msg.h"
#include "engine/worker/worker_types.h"
#include "uthash.h"
#include "worker_ops.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void worker_ops_clear(worker_ops_t *ops) {
  if (!ops) {
    return;
  }
  if (ops->ops) {
    free(ops->ops);
    ops->ops = NULL;
  }
  ops->num_ops = 0;
}

static bool _append_op(worker_ops_t *ops, char *key_buffer, op_t *op, int *i) {
  op_queue_msg_t *msg = op_queue_msg_create(key_buffer, op);
  if (!msg) {
    return false;
  }

  ops->ops[*i] = msg;
  (*i)++;
  return true;
}

static worker_ops_result_t
_create_incr_entity_id_op(uint32_t entity_id, worker_ops_t *ops, int *i) {
  char key_buffer[512];
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_SYSTEM;
  db_key.sys_db_type = SYS_DB_METADATA;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = strdup(SYS_NEXT_ENT_ID_KEY);

  db_key.container_name = strdup(SYS_CONTAINER_NAME);
  if (!db_key.container_name) {
    return WORKER_OPS_ERROR("Memory allocation failed", "container_name_dup");
  }

  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    free(db_key.container_name);
    return WORKER_OPS_ERROR("Key formatting failed", "db_key_into");
  }

  op_t *op = op_create_int32_val(&db_key, OP_TYPE_COND_PUT,
                                 COND_PUT_IF_EXISTING_LESS_THAN, entity_id);
  if (!op) {
    free(db_key.container_name);
    return WORKER_OPS_ERROR("Operation creation failed", "op_create");
  }

  if (!_append_op(ops, key_buffer, op, i)) {
    op_destroy(op);
    free(db_key.container_name);
    return WORKER_OPS_ERROR("Failed to append operation", "append_op");
  }

  return WORKER_OPS_SUCCESS();
}

static worker_ops_result_t _create_incr_event_id_op(char *container_name,
                                                    uint32_t event_id,
                                                    worker_ops_t *ops, int *i) {
  char key_buffer[512];

  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;

  db_key.container_name = strdup(container_name);
  if (!db_key.container_name) {
    return WORKER_OPS_ERROR("Memory allocation failed", "container_name_dup");
  }

  db_key.user_db_type = USER_DB_METADATA;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = strdup(USR_NEXT_EVENT_ID_KEY);

  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    container_free_db_key_contents(&db_key);
    return WORKER_OPS_ERROR("Key formatting failed", "db_key_into");
  }

  op_t *op = op_create_int32_val(&db_key, OP_TYPE_COND_PUT,
                                 COND_PUT_IF_EXISTING_LESS_THAN, event_id);
  if (!op) {
    container_free_db_key_contents(&db_key);
    return WORKER_OPS_ERROR("Operation creation failed", "op_create");
  }

  if (!_append_op(ops, key_buffer, op, i)) {
    op_destroy(op);
    return WORKER_OPS_ERROR("Failed to append operation", "append_op");
  }

  return WORKER_OPS_SUCCESS();
}

static worker_ops_result_t _create_ent_mapping_ops(char *ent_str_id,
                                                   uint32_t ent_int_id,
                                                   worker_ops_t *ops, int *i) {
  char key_buffer[512];
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_SYSTEM;
  db_key.sys_db_type = SYS_DB_ENT_ID_TO_INT;
  db_key.db_key.type = DB_KEY_STRING;

  db_key.db_key.key.s = strdup(ent_str_id);
  if (!db_key.db_key.key.s) {
    return WORKER_OPS_ERROR("Memory allocation failed", "ent_str_id_dup");
  }

  db_key.container_name = strdup(SYS_CONTAINER_NAME);
  if (!db_key.container_name) {
    free(db_key.db_key.key.s);
    return WORKER_OPS_ERROR("Memory allocation failed", "container_name_dup");
  }

  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    free(db_key.db_key.key.s);
    free(db_key.container_name);
    return WORKER_OPS_ERROR("Key formatting failed", "db_key_into");
  }

  op_t *ent_id_to_int_op =
      op_create_int32_val(&db_key, OP_TYPE_PUT, COND_PUT_NONE, ent_int_id);
  if (!ent_id_to_int_op) {
    free(db_key.db_key.key.s);
    free(db_key.container_name);
    return WORKER_OPS_ERROR("Operation creation failed", "op_create");
  }

  if (!_append_op(ops, key_buffer, ent_id_to_int_op, i)) {
    op_destroy(ent_id_to_int_op);
    free(db_key.db_key.key.s);
    free(db_key.container_name);
    return WORKER_OPS_ERROR("Failed to append operation", "append_op");
  }

  db_key.sys_db_type = SYS_DB_INT_TO_ENT_ID;
  db_key.db_key.type = DB_KEY_INTEGER;
  db_key.db_key.key.i = ent_int_id;

  db_key.container_name = strdup(SYS_CONTAINER_NAME);
  if (!db_key.container_name) {
    return WORKER_OPS_ERROR("Memory allocation failed", "container_name_dup");
  }

  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    free(db_key.container_name);
    return WORKER_OPS_ERROR("Key formatting failed", "db_key_into");
  }

  op_t *int_to_ent_id_op =
      op_create_str_val(&db_key, OP_TYPE_PUT, COND_PUT_NONE, ent_str_id);
  if (!int_to_ent_id_op) {
    free(db_key.container_name);
    return WORKER_OPS_ERROR("Operation creation failed", "op_create");
  }

  if (!_append_op(ops, key_buffer, int_to_ent_id_op, i)) {
    op_destroy(int_to_ent_id_op);
    free(db_key.container_name);
    return WORKER_OPS_ERROR("Failed to append operation", "append_op");
  }

  return WORKER_OPS_SUCCESS();
}

static worker_ops_result_t
_create_event_to_entity_op(char *container_name, uint32_t event_id,
                           uint32_t ent_int_id, worker_ops_t *ops, int *i) {
  char key_buffer[512] = {0};

  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;

  db_key.container_name = strdup(container_name);
  if (!db_key.container_name) {
    return WORKER_OPS_ERROR("Memory allocation failed", "container_name_dup");
  }

  db_key.user_db_type = USER_DB_EVENT_TO_ENTITY;
  db_key.db_key.type = DB_KEY_INTEGER;
  db_key.db_key.key.i = event_id;

  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    free(db_key.container_name);
    return WORKER_OPS_ERROR("Key formatting failed", "db_key_into");
  }

  op_t *event_to_entity_op =
      op_create_int32_val(&db_key, OP_TYPE_PUT, COND_PUT_NONE, ent_int_id);

  if (!event_to_entity_op) {
    free(db_key.container_name);
    return WORKER_OPS_ERROR("Operation creation failed", "op_create");
  }

  if (!_append_op(ops, key_buffer, event_to_entity_op, i)) {
    op_destroy(event_to_entity_op);
    free(db_key.container_name);
    return WORKER_OPS_ERROR("Failed to append operation", "append_op");
  }

  return WORKER_OPS_SUCCESS();
}

static worker_ops_result_t
_create_write_to_event_index_ops(char *container_name, uint32_t event_id,
                                 cmd_queue_msg_t *msg, worker_ops_t *ops,
                                 int *i) {
  char key_buffer[512];
  char ser_db_key[512];
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;
  db_key.user_db_type = USER_DB_INVERTED_EVENT_INDEX;
  db_key.db_key.type = DB_KEY_STRING;

  ast_node_t *custom_tag = msg->command->custom_tags_head;

  for (uint32_t ct_i = 0; ct_i < msg->command->num_custom_tags; ct_i++) {
    if (!custom_tag_into(key_buffer, sizeof(key_buffer), custom_tag)) {
      return WORKER_OPS_ERROR("Key formatting failed", "custom_tag_into");
    }

    db_key.db_key.key.s = strdup(key_buffer);
    if (!db_key.db_key.key.s) {
      return WORKER_OPS_ERROR("Memory allocation failed", "key_buffer_dup");
    }

    db_key.container_name = strdup(container_name);
    if (!db_key.container_name) {
      free(db_key.db_key.key.s);
      return WORKER_OPS_ERROR("Memory allocation failed", "container_name_dup");
    }

    if (!db_key_into(ser_db_key, sizeof(ser_db_key), &db_key)) {
      free(db_key.db_key.key.s);
      free(db_key.container_name);
      return WORKER_OPS_ERROR("Key formatting failed", "db_key_into");
    }

    op_t *o = op_create_int32_val(&db_key, OP_TYPE_ADD_VALUE, COND_PUT_NONE,
                                  event_id);
    if (!o) {
      free(db_key.db_key.key.s);
      free(db_key.container_name);
      return WORKER_OPS_ERROR("Operation creation failed", "op_create");
    }

    if (!_append_op(ops, ser_db_key, o, i)) {
      op_destroy(o);
      free(db_key.db_key.key.s);
      free(db_key.container_name);
      return WORKER_OPS_ERROR("Failed to append operation", "append_op");
    }

    custom_tag = custom_tag->next;
  }

  return WORKER_OPS_SUCCESS();
}

static worker_ops_result_t _create_tag_counter_ops(
    char *container_name, uint32_t entity_id, cmd_queue_msg_t *msg,
    worker_entity_tag_counter_t *tag_counters, worker_ops_t *ops, int *i) {
  char tag_buffer[512];
  char counter_store_key[512];
  char count_index_key[512];
  char ser_db_key[512];

  eng_container_db_key_t db_key;
  worker_entity_tag_counter_t *tag_counter = NULL;

  ast_node_t *custom_tag = msg->command->custom_tags_head;

  for (; custom_tag; custom_tag = custom_tag->next) {
    if (!custom_tag->tag.is_counter)
      continue;

    if (!(custom_tag_into(tag_buffer, sizeof(tag_buffer), custom_tag) &&
          tag_str_entity_id_into(counter_store_key, sizeof(counter_store_key),
                                 tag_buffer, entity_id))) {
      return WORKER_OPS_ERROR("Key formatting failed", "tag_counter_key");
    }

    HASH_FIND_STR(tag_counters, counter_store_key, tag_counter);
    if (!tag_counter) {
      // Skip this counter, not an error
      continue;
    }

    if (!tag_count_into(count_index_key, sizeof(count_index_key), tag_buffer,
                        tag_counter->count)) {
      return WORKER_OPS_ERROR("Key formatting failed", "counter_store_key");
    }

    db_key.dc_type = CONTAINER_TYPE_USER;

    db_key.container_name = strdup(container_name);
    if (!db_key.container_name) {
      return WORKER_OPS_ERROR("Memory allocation failed", "container_name_dup");
    }

    db_key.user_db_type = USER_DB_COUNTER_STORE;
    db_key.db_key.type = DB_KEY_STRING;

    db_key.db_key.key.s = strdup(counter_store_key);
    if (!db_key.db_key.key.s) {
      free(db_key.container_name);
      return WORKER_OPS_ERROR("Memory allocation failed",
                              "tag_counter_key_dup");
    }

    if (!db_key_into(ser_db_key, sizeof(ser_db_key), &db_key)) {
      free(db_key.db_key.key.s);
      free(db_key.container_name);
      return WORKER_OPS_ERROR("Key formatting failed", "db_key_into");
    }

    op_t *o =
        op_create_int32_val(&db_key, OP_TYPE_COND_PUT,
                            COND_PUT_IF_EXISTING_LESS_THAN, tag_counter->count);
    if (!o) {
      free(db_key.db_key.key.s);
      free(db_key.container_name);
      return WORKER_OPS_ERROR("Operation creation failed", "op_create");
    }

    if (!_append_op(ops, ser_db_key, o, i)) {
      op_destroy(o);
      free(db_key.db_key.key.s);
      free(db_key.container_name);
      return WORKER_OPS_ERROR("Failed to append operation", "append_op");
    }

    db_key.dc_type = CONTAINER_TYPE_USER;

    db_key.container_name = strdup(container_name);
    if (!db_key.container_name) {
      return WORKER_OPS_ERROR("Memory allocation failed", "container_name_dup");
    }

    db_key.user_db_type = USER_DB_COUNT_INDEX;
    db_key.db_key.type = DB_KEY_STRING;

    db_key.db_key.key.s = strdup(count_index_key);
    if (!db_key.db_key.key.s) {
      free(db_key.container_name);
      return WORKER_OPS_ERROR("Memory allocation failed",
                              "counter_store_key_dup");
    }

    if (!db_key_into(ser_db_key, sizeof(ser_db_key), &db_key)) {
      free(db_key.db_key.key.s);
      free(db_key.container_name);
      return WORKER_OPS_ERROR("Key formatting failed", "db_key_into");
    }

    op_t *o2 = op_create_int32_val(&db_key, OP_TYPE_ADD_VALUE, COND_PUT_NONE,
                                   entity_id);
    if (!o2) {
      free(db_key.db_key.key.s);
      free(db_key.container_name);
      return WORKER_OPS_ERROR("Operation creation failed", "op_create");
    }

    if (!_append_op(ops, ser_db_key, o2, i)) {
      op_destroy(o2);
      free(db_key.db_key.key.s);
      free(db_key.container_name);
      return WORKER_OPS_ERROR("Failed to append operation", "append_op");
    }
  }

  return WORKER_OPS_SUCCESS();
}

static worker_ops_result_t _create_container_entity_op(char *container_name,
                                                       uint32_t entity_id_int32,
                                                       worker_ops_t *ops,
                                                       int *i) {
  char key_buffer[512] = {0};

  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;

  db_key.container_name = strdup(container_name);
  if (!db_key.container_name) {
    return WORKER_OPS_ERROR("Memory allocation failed", "container_name_dup");
  }

  db_key.user_db_type = USER_DB_METADATA;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = strdup(USR_ENTITIES_KEY);
  if (!db_key.db_key.key.s) {
    return WORKER_OPS_ERROR("Memory allocation failed", "db_key_dup");
  }
  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    free(db_key.container_name);
    free(db_key.db_key.key.s);
    return WORKER_OPS_ERROR("Key formatting failed", "db_key_into");
  }

  op_t *container_entity_op = op_create_int32_val(
      &db_key, OP_TYPE_ADD_VALUE, COND_PUT_NONE, entity_id_int32);

  if (!container_entity_op) {
    free(db_key.container_name);
    free(db_key.db_key.key.s);
    return WORKER_OPS_ERROR("Operation creation failed", "op_create");
  }

  if (!_append_op(ops, key_buffer, container_entity_op, i)) {
    op_destroy(container_entity_op);
    free(db_key.container_name);
    free(db_key.db_key.key.s);

    return WORKER_OPS_ERROR("Failed to append operation", "append_op");
  }

  return WORKER_OPS_SUCCESS();
}

static worker_ops_result_t
_create_ops(cmd_queue_msg_t *msg, char *container_name, char *entity_id_str,
            uint32_t entity_id_int32, bool is_new_entity, uint32_t event_id,
            worker_entity_tag_counter_t *tag_counters, worker_ops_t *ops_out,
            bool is_new_container_ent) {
  worker_ops_result_t result;
  int ops_created = 0;
  uint32_t num_custom_tags = msg->command->num_custom_tags;
  uint32_t num_counter_tags = msg->command->num_counter_tags;

  if (num_counter_tags && tag_counters == NULL) {
    return WORKER_OPS_ERROR("Counter tags present but no counters provided",
                            "counter_tags_validation");
  }

  uint32_t num_ops =
      // _create_incr_entity_id_op        = 1 op
      // _create_ent_mapping_ops          = 2 ops
      (is_new_entity ? 3 : 0)
      // _create_incr_event_id_op         = 1 op
      // _create_event_to_entity_op       = 1 op
      + 2
      // _create_container_entity_op      = 1 op
      + (is_new_container_ent ? 1 : 0)
      // _create_write_to_event_index_ops = `num_custom_tags` ops
      + num_custom_tags
      // _create_tag_counter_ops          = (`num_counter_tags` * 2) ops
      + (num_counter_tags * 2);

  ops_out->ops = malloc(num_ops * sizeof(op_queue_msg_t *));
  if (!ops_out->ops) {
    return WORKER_OPS_ERROR("Memory allocation failed", "ops_array");
  }
  ops_out->num_ops = num_ops;

  if (is_new_entity) {
    result = _create_incr_entity_id_op(entity_id_int32, ops_out, &ops_created);
    if (!result.success)
      goto cleanup;

    result = _create_ent_mapping_ops(entity_id_str, entity_id_int32, ops_out,
                                     &ops_created);
    if (!result.success)
      goto cleanup;
  }

  result =
      _create_incr_event_id_op(container_name, event_id, ops_out, &ops_created);
  if (!result.success)
    goto cleanup;

  result = _create_event_to_entity_op(container_name, event_id, entity_id_int32,
                                      ops_out, &ops_created);
  if (!result.success)
    goto cleanup;

  if (is_new_container_ent) {
    result = _create_container_entity_op(container_name, entity_id_int32,
                                         ops_out, &ops_created);
    if (!result.success)
      goto cleanup;
  }

  result = _create_write_to_event_index_ops(container_name, event_id, msg,
                                            ops_out, &ops_created);
  if (!result.success)
    goto cleanup;

  if (num_counter_tags) {
    result = _create_tag_counter_ops(container_name, entity_id_int32, msg,
                                     tag_counters, ops_out, &ops_created);
    if (!result.success)
      goto cleanup;
  }

  return WORKER_OPS_SUCCESS();

cleanup:
  if (ops_out->ops) {
    for (int i = 0; i < ops_created; i++) {
      op_queue_msg_free(ops_out->ops[i]);
    }
    free(ops_out->ops);
    ops_out->ops = NULL;
    ops_out->num_ops = 0;
  }
  return result;
}

worker_ops_result_t worker_create_ops(cmd_queue_msg_t *msg,
                                      char *container_name, char *entity_id_str,
                                      uint32_t entity_id_int32,
                                      bool is_new_entity, uint32_t event_id,
                                      worker_entity_tag_counter_t *tag_counters,
                                      worker_ops_t *ops_out,
                                      bool is_new_container_ent) {
  if (!msg || !container_name || !entity_id_str || !ops_out) {
    return WORKER_OPS_ERROR("Invalid arguments", "worker_create_ops");
  }

  memset(ops_out, 0, sizeof(worker_ops_t));

  return _create_ops(msg, container_name, entity_id_str, entity_id_int32,
                     is_new_entity, event_id, tag_counters, ops_out,
                     is_new_container_ent);
}