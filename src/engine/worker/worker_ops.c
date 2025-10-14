#include "engine/worker/worker_ops.h"
#include "core/db.h"
#include "engine/eng_key_format/eng_key_format.h"
#include "engine/op/op.h"
#include "engine/op_queue/op_queue_msg.h"
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

static bool _create_incr_entity_id_op(uint32_t entity_id, worker_ops_t *ops,
                                      int *i) {
  char key_buffer[512];
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_SYSTEM;
  db_key.sys_db_type = SYS_DB_METADATA;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = SYS_NEXT_ENT_ID_KEY;
  db_key.container_name = SYS_CONTAINER_NAME;
  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    return false;
  }

  op_t *op = op_create_int32_val(&db_key, COND_PUT,
                                 COND_PUT_IF_EXISTING_LESS_THAN, entity_id);
  if (!op)
    return false;

  if (!_append_op(ops, key_buffer, op, i)) {
    op_destroy(op);
    return false;
  }
  return true;
}

static bool _create_incr_event_id_op(char *container_name, uint32_t event_id,
                                     worker_ops_t *ops, int *i) {
  char key_buffer[512];

  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;
  db_key.container_name = container_name;
  db_key.user_db_type = USER_DB_METADATA;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = USR_NEXT_EVENT_ID_KEY;
  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    return false;
  }
  op_t *op = op_create_int32_val(&db_key, COND_PUT,
                                 COND_PUT_IF_EXISTING_LESS_THAN, event_id);
  if (!op)
    return false;

  if (!_append_op(ops, key_buffer, op, i)) {
    op_destroy(op);
    return false;
  }
  return true;
}

static bool _create_ent_mapping_ops(char *ent_str_id, uint32_t ent_int_id,
                                    worker_ops_t *ops, int *i) {
  char key_buffer[512];
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_SYSTEM;
  db_key.sys_db_type = SYS_DB_ENT_ID_TO_INT;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = ent_str_id;
  db_key.container_name = SYS_CONTAINER_NAME;
  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    return false;
  }
  op_t *ent_id_to_int_op =
      op_create_int32_val(&db_key, PUT, COND_PUT_NONE, ent_int_id);
  if (!ent_id_to_int_op)
    return false;
  if (!_append_op(ops, key_buffer, ent_id_to_int_op, i)) {
    op_destroy(ent_id_to_int_op);
    return false;
  }

  db_key.sys_db_type = SYS_DB_INT_TO_ENT_ID;
  db_key.db_key.type = DB_KEY_INTEGER;
  db_key.db_key.key.i = ent_int_id;
  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    return false;
  }

  op_t *int_to_ent_id_op =
      op_create_str_val(&db_key, PUT, COND_PUT_NONE, ent_str_id);

  if (!_append_op(ops, key_buffer, int_to_ent_id_op, i)) {
    op_destroy(int_to_ent_id_op);
    return false;
  }
  return true;
}

static bool _create_event_to_entity_op(char *container_name, uint32_t event_id,
                                       uint32_t ent_int_id, worker_ops_t *ops,
                                       int *i) {
  char key_buffer[512] = {0};

  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;
  db_key.container_name = container_name;
  db_key.user_db_type = USER_DB_EVENT_TO_ENTITY;
  db_key.db_key.type = DB_KEY_INTEGER;
  db_key.db_key.key.i = event_id;
  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    return false;
  }

  op_t *event_to_entity_op =
      op_create_int32_val(&db_key, PUT, COND_PUT_NONE, ent_int_id);

  if (!event_to_entity_op) {
    return false;
  }
  if (!_append_op(ops, key_buffer, event_to_entity_op, i)) {
    op_destroy(event_to_entity_op);
    return false;
  }
  return true;
}

static bool _create_write_to_event_index_ops(char *container_Name,
                                             uint32_t event_id,
                                             cmd_queue_msg_t *msg,
                                             worker_ops_t *ops, int *i) {
  char key_buffer[512]; // TODO: pull 512 from shared config
  char ser_db_key[512];
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;
  db_key.container_name = container_Name;
  db_key.user_db_type = USER_DB_INVERTED_EVENT_INDEX;
  db_key.db_key.type = DB_KEY_STRING;

  ast_node_t *custom_tag = msg->command->custom_tags_head;

  for (uint32_t ct_i = 0; ct_i < msg->command->num_custom_tags; ct_i++) {
    if (!custom_tag_into(key_buffer, sizeof(key_buffer), custom_tag)) {
      return false;
    }
    db_key.db_key.key.s = key_buffer;
    if (!db_key_into(ser_db_key, sizeof(ser_db_key), &db_key)) {
      return false;
    }
    op_t *o =
        op_create_int32_val(&db_key, BM_ADD_VALUE, COND_PUT_NONE, event_id);
    if (!o) {
      // TODO: handle error
      return false;
    }
    if (!_append_op(ops, key_buffer, o, i)) {
      op_destroy(o);
      return false;
    }
    custom_tag = custom_tag->next;
  }

  return true;
}

static bool _create_tag_counter_ops(char *container_Name, uint32_t entity_id,
                                    cmd_queue_msg_t *msg, worker_ops_t *ops,
                                    int *i) {
  char tag_buffer[512]; // TODO: pull 512 from shared config
  char ser_db_key[512];
  eng_container_db_key_t db_key;

  ast_node_t *custom_tag = msg->command->custom_tags_head;

  for (uint32_t ct_i = 0; ct_i < msg->command->num_counter_tags; ct_i++) {
    if (!custom_tag_into(tag_buffer, sizeof(tag_buffer), custom_tag)) {
      return false;
    }
    db_key.dc_type = CONTAINER_TYPE_USER;
    db_key.container_name = container_Name;
    db_key.user_db_type = USER_DB_COUNT_INDEX;
    db_key.db_key.type = DB_KEY_STRING;

    if (!db_key_into(ser_db_key, sizeof(ser_db_key), &db_key)) {
      return false;
    }

    op_t *o = op_create_count_tag_increment(&db_key, tag_buffer, entity_id, 1);
    if (!o) {
      // TODO: handle error
      return false;
    }
    if (!_append_op(ops, ser_db_key, o, i)) {
      op_destroy(o);
      return false;
    }
    custom_tag = custom_tag->next;
  }

  return true;
}

static bool _create_ops(cmd_queue_msg_t *msg, char *container_name,
                        char *entity_id_str, uint32_t entity_id_int32,
                        bool is_new_entity, uint32_t event_id,
                        worker_ops_t *ops_out) {
  bool success = false;
  int ops_created = 0;

  uint32_t num_ops = (is_new_entity ? 3 : 0) + 2 +
                     msg->command->num_custom_tags +
                     msg->command->num_counter_tags;

  ops_out->ops = malloc(num_ops * sizeof(op_queue_msg_t *));
  if (!ops_out->ops) {
    goto cleanup;
  }
  ops_out->num_ops = num_ops;

  if (is_new_entity) {
    if (!_create_incr_entity_id_op(entity_id_int32, ops_out, &ops_created)) {
      goto cleanup;
    }
    if (!_create_ent_mapping_ops(entity_id_str, entity_id_int32, ops_out,
                                 &ops_created)) {
      goto cleanup;
    }
  }

  if (!_create_incr_event_id_op(container_name, event_id, ops_out,
                                &ops_created))
    goto cleanup;
  if (!_create_event_to_entity_op(container_name, event_id, entity_id_int32,
                                  ops_out, &ops_created))
    goto cleanup;
  if (!_create_write_to_event_index_ops(container_name, event_id, msg, ops_out,
                                        &ops_created)) {
    goto cleanup;
  }
  if (!_create_tag_counter_ops(container_name, entity_id_int32, msg, ops_out,
                               &ops_created))
    goto cleanup;

  success = true;

cleanup:
  if (!success) {
    if (ops_out->ops) {
      for (int i = 0; i < ops_created; i++) {
        op_queue_msg_free(ops_out->ops[i]);
      }
      free(ops_out->ops);
      ops_out->ops = NULL;
      ops_out->num_ops = 0;
    }
  }
  return success;
}

bool worker_create_ops(cmd_queue_msg_t *msg, char *container_name,
                       char *entity_id_str, uint32_t entity_id_int32,
                       bool is_new_entity, uint32_t event_id,
                       worker_ops_t *ops_out) {
  if (!msg || !container_name || !entity_id_str || !ops_out) {
    return false;
  }

  memset(ops_out, 0, sizeof(worker_ops_t));

  return _create_ops(msg, container_name, entity_id_str, entity_id_int32,
                     is_new_entity, event_id, ops_out);
  // If this returns false, ops_out is already cleaned up internally
}