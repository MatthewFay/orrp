#include "engine/worker/worker_ops.h"
#include "core/db.h"
#include "engine/container/container.h"
#include "engine/op/op.h"
#include "engine/op_queue/op_queue_msg.h"
#include "worker_ops.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void worker_ops_free(worker_ops_t *ops) {
  if (!ops || !ops->ops) {
    return;
  }

  for (uint32_t i = 0; i < ops->num_ops; i++) {
    op_queue_msg_free(ops->ops[i]);
  }
  free(ops->ops);
  ops->ops = NULL;
  ops->num_ops = 0;
}

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

static bool _container_tag_into(char *out_buf, size_t size,
                                char *container_name, char *tag) {
  int r = snprintf(out_buf, size, "%s|%s", container_name, tag);
  if (r < 0 || (size_t)r >= size) {
    return false;
  }
  return true;
}

static bool _db_key_into(char *buffer, size_t buffer_size,
                         eng_container_db_key_t *db_key) {
  int r = -1;
  int db_type = 0;
  char *container_name;
  if (db_key->dc_type == CONTAINER_TYPE_SYSTEM) {
    db_type = db_key->sys_db_type;
    container_name = SYS_CONTAINER_NAME;
  } else {
    db_type = db_key->user_db_type;
    container_name = db_key->container_name;
  }
  if (db_key->db_key.type == DB_KEY_INTEGER) {
    r = snprintf(buffer, buffer_size, "%s|%d|%u", container_name, (int)db_type,
                 db_key->db_key.key.i);
  } else if (db_key->db_key.type == DB_KEY_STRING) {
    r = snprintf(buffer, buffer_size, "%s|%d|%s", container_name, (int)db_type,
                 db_key->db_key.key.s);
  } else {
    return false;
  }
  if (r < 0 || (size_t)r >= buffer_size) {
    return false;
  }
  return true;
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
  if (!_db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    return false;
  }

  op_t *op =
      op_create_int32_val(db_key, COND_PUT, IF_EXISTING_LESS_THAN, entity_id);
  if (!op)
    return false;

  if (!_append_op(ops, key_buffer, op, i)) {
    op_destroy(op);
    return false;
  }
  return true;
}

static bool _create_incr_event_id_op(uint32_t event_id, worker_ops_t *ops,
                                     int *i) {
  char key_buffer[512];

  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;
  db_key.user_db_type = USER_DB_METADATA;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = USR_NEXT_EVENT_ID_KEY;
  if (!_db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    return false;
  }
  op_t *op =
      op_create_int32_val(db_key, COND_PUT, IF_EXISTING_LESS_THAN, event_id);
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
  if (!_db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    return false;
  }
  op_t *ent_id_to_int_op = op_create_int32_val(db_key, PUT, NULL, ent_int_id);
  if (!ent_id_to_int_op)
    return false;
  if (!_append_op(ops, key_buffer, ent_id_to_int_op, i)) {
    op_destroy(ent_id_to_int_op);
    return false;
  }

  db_key.sys_db_type = SYS_DB_INT_TO_ENT_ID;
  db_key.db_key.type = DB_KEY_INTEGER;
  db_key.db_key.key.i = ent_int_id;
  if (!_db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    return false;
  }

  op_t *int_to_ent_id_op = op_create_str_val(db_key, PUT, NULL, ent_str_id);

  if (!_append_op(ops, key_buffer, int_to_ent_id_op, i)) {
    op_destroy(int_to_ent_id_op);
    return false;
  }
}

static bool _create_event_to_entity_op(uint32_t event_id, uint32_t ent_int_id,
                                       worker_ops_t *ops, int *i) {
  char key_buffer[512];

  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;
  db_key.user_db_type = USER_DB_EVENT_TO_ENTITY;
  db_key.db_key.type = DB_KEY_INTEGER;
  db_key.db_key.key.i = event_id;
  if (!_db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    return false;
  }

  op_t *event_to_entity_op = op_create_int32_val(db_key, PUT, NULL, ent_int_id);

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
  char routing_key[512];
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USER;
  db_key.container_name = container_Name;
  db_key.user_db_type = USER_DB_INVERTED_EVENT_INDEX;
  db_key.db_key.type = DB_KEY_STRING;

  ast_node_t *custom_tag = msg->command->custom_tags_head;

  for (uint32_t ct_i = 0; ct_i < msg->command->num_custom_tags; ct_i++) {
    if (!_custom_tag_into(key_buffer, sizeof(key_buffer), custom_tag)) {
      return false;
    }
    db_key.db_key.key.s = key_buffer;
    if (!_db_key_into(routing_key, sizeof(routing_key), &db_key)) {
      return false;
    }
    op_t *o = op_create_int32_val(db_key, BM_ADD_VALUE, NULL, event_id);
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
  char routing_key[512];
  ast_node_t *custom_tag = msg->command->custom_tags_head;

  for (uint32_t ct_i = 0; ct_i < msg->command->num_counter_tags; ct_i++) {
    if (!_custom_tag_into(tag_buffer, sizeof(tag_buffer), custom_tag)) {
      return false;
    }
    if (!_container_tag_into(routing_key, sizeof(routing_key), container_Name,
                             tag_buffer)) {
      return false;
    }

    op_t *o =
        op_create_count_tag_increment(container_Name, tag_buffer, entity_id, 1);
    if (!o) {
      // TODO: handle error
      return false;
    }
    if (!_append_op(ops, routing_key, o, i)) {
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

  if (!_create_incr_event_id_op(event_id, ops_out, &ops_created))
    goto cleanup;
  if (!_create_event_to_entity_op(event_id, entity_id_int32, ops_out,
                                  &ops_created))
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