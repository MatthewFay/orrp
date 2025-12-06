#include "engine/worker/worker_ops.h"
#include "core/db.h"
#include "engine/cmd_queue/cmd_queue_msg.h"
#include "engine/container/container_types.h"
#include "engine/eng_key_format/eng_key_format.h"
#include "engine/op/op.h"
#include "engine/op_queue/op_queue_msg.h"
#include "worker_ops.h"
#include <stdbool.h>
#include <stddef.h>
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
_create_write_to_event_index_ops(char *container_name, uint32_t event_id,
                                 cmd_queue_msg_t *msg, worker_ops_t *ops,
                                 int *i) {
  char key_buffer[512];
  char ser_db_key[512];
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USR;
  db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;
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

    op_t *o = op_create(OP_TYPE_ADD, &db_key, event_id);
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

static worker_ops_result_t _create_container_entity_op(char *container_name,
                                                       uint32_t entity_id_int32,
                                                       worker_ops_t *ops,
                                                       int *i) {
  char key_buffer[512] = {0};

  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USR;

  db_key.container_name = strdup(container_name);
  if (!db_key.container_name) {
    return WORKER_OPS_ERROR("Memory allocation failed", "container_name_dup");
  }

  db_key.usr_db_type = USR_DB_METADATA;
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

  op_t *container_entity_op = op_create(OP_TYPE_ADD, &db_key, entity_id_int32);

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
_create_ops(cmd_queue_msg_t *msg, char *container_name,
            uint32_t entity_id_int32, uint32_t event_id,
            worker_ops_t *ops_out) {
  worker_ops_result_t result;
  int ops_created = 0;
  uint32_t num_custom_tags = msg->command->num_custom_tags;

  uint32_t num_ops =
      // _create_container_entity_op
      1
      // _create_write_to_event_index_ops = `num_custom_tags` ops
      + num_custom_tags;

  ops_out->ops = malloc(num_ops * sizeof(op_queue_msg_t *));
  if (!ops_out->ops) {
    return WORKER_OPS_ERROR("Memory allocation failed", "ops_array");
  }
  ops_out->num_ops = num_ops;

  result = _create_container_entity_op(container_name, entity_id_int32, ops_out,
                                       &ops_created);
  if (!result.success)
    goto cleanup;

  result = _create_write_to_event_index_ops(container_name, event_id, msg,
                                            ops_out, &ops_created);
  if (!result.success)
    goto cleanup;

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
                                      char *container_name,
                                      uint32_t entity_id_int32,
                                      uint32_t event_id,
                                      worker_ops_t *ops_out) {
  if (!msg || !container_name  || !ops_out) {
    return WORKER_OPS_ERROR("Invalid arguments", "worker_create_ops");
  }

  memset(ops_out, 0, sizeof(worker_ops_t));

  return _create_ops(msg, container_name, entity_id_int32,
                     event_id, ops_out);
}