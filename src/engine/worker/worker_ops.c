#include "worker_ops.h"
#include "core/db.h"
#include "engine/container/container.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Turn custom tag AST node into a string representation
static bool _custom_tag_into(char *out_buf, size_t size,
                             ast_node_t *custom_tag) {
  int r = snprintf(out_buf, size, "%s:%s", custom_tag->tag.custom_key,
                   custom_tag->tag.value->literal.string_value);
  if (r < 0 || (size_t)r >= size) {
    return false;
  }
  return true;
}

static bool _create_incr_entity_id_op(uint32_t entity_id, worker_ops_t *ops) {
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_SYSTEM;
  db_key.sys_db_type = SYS_DB_METADATA;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = SYS_NEXT_ENT_ID_KEY;
  ops->incr_entity_id_op = op_queue_create_msg_int32_val(
      db_key, COND_PUT, IF_EXISTING_LESS_THAN, entity_id);
  if (ops->incr_entity_id_op) {
    return true;
  }
  return false;
}

static bool _create_incr_event_id_op(uint32_t event_id, worker_ops_t *ops) {
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;
  db_key.user_db_type = USER_DB_METADATA;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = USR_NEXT_EVENT_ID_KEY;
  ops->incr_event_id_op = op_queue_create_msg_int32_val(
      db_key, COND_PUT, IF_EXISTING_LESS_THAN, event_id);
  if (ops->incr_event_id_op) {
    return true;
  }
  return false;
}

static bool _create_ent_mapping_ops(char *ent_str_id, uint32_t ent_int_id,
                                    worker_ops_t *ops) {
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_SYSTEM;
  db_key.sys_db_type = SYS_DB_ENT_ID_TO_INT;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = ent_str_id;
  ops->ent_id_to_int_op =
      op_queue_create_msg_int32_val(db_key, PUT, NULL, ent_int_id);

  db_key.sys_db_type = SYS_DB_INT_TO_ENT_ID;
  db_key.db_key.type = DB_KEY_INTEGER;
  db_key.db_key.key.i = ent_int_id;
  ops->int_to_ent_id_op =
      op_queue_create_msg_str_val(db_key, PUT, NULL, ent_str_id);
  if (ops->ent_id_to_int_op && ops->int_to_ent_id_op) {
    return true;
  }
  return false;
}

static bool _create_event_to_entity_op(uint32_t event_id, uint32_t ent_int_id,
                                       worker_ops_t *ops) {
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;
  db_key.user_db_type = USER_DB_EVENT_TO_ENTITY;
  db_key.db_key.type = DB_KEY_INTEGER;
  db_key.db_key.key.i = event_id;
  ops->event_to_entity_op =
      op_queue_create_msg_int32_val(db_key, PUT, NULL, ent_int_id);

  if (ops->event_to_entity_op) {
    return true;
  }
  return false;
}

static bool _create_write_to_event_index_ops(char *container_Name,
                                             uint32_t event_id,
                                             cmd_queue_msg_t *msg,
                                             worker_ops_t *ops) {
  char key_buffer[512]; // TODO: pull 512 from shared config
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;
  db_key.container_name = container_Name;
  db_key.user_db_type = USER_DB_INVERTED_EVENT_INDEX;
  db_key.db_key.type = DB_KEY_STRING;

  ast_node_t *custom_tag = msg->command->custom_tags_head;

  ops->write_to_event_index_ops.count = msg->command->num_custom_tags;
  ops->write_to_event_index_ops.ops =
      malloc(sizeof(op_queue_msg_t *) * ops->write_to_event_index_ops.count);

  for (uint32_t i = 0; i < ops->write_to_event_index_ops.count; i++) {
    if (!_custom_tag_into(key_buffer, sizeof(key_buffer), custom_tag)) {
      return false;
    }
    db_key.db_key.key.s = key_buffer;
    op_queue_msg_t *m =
        op_queue_create_msg_int32_val(db_key, BM_ADD_VALUE, NULL, event_id);
    if (!m) {
      // TODO: handle error
      return false;
    }
    ops->write_to_event_index_ops[i] = m;
    custom_tag = custom_tag->next;
  }

  return true;
}

bool worker_ops_create_ops(cmd_queue_msg_t *msg, char *container_name,
                           char *entity_id_str, uint32_t entity_id_int32,
                           bool is_new_entity, uint32_t event_id,
                           worker_ops_t *ops_out) {
  if (!msg || !container_name || !entity_id_str || !ops_out) {
    return false;
  }
  memset(ops_out, 0, sizeof(worker_ops_t));

  return true;
}