#include "worker_writer.h"
#include "core/db.h"
#include "engine/cmd_queue/cmd_queue_msg.h"
#include "engine/container/container_types.h"
#include "engine/engine_writer/engine_writer_queue_msg.h"
#include "engine/index/index.h"
#include "engine/worker/encoder.h"
#include "query/ast.h"
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#define SYNC_INTERVAL 1000

static bool _create_mpack_entry(cmd_queue_msg_t *cmd_msg, char *container_name,
                                uint32_t event_id, eng_writer_entry_t *entry) {
  char *msgpack = NULL;
  size_t msgpack_size;

  if (!encode_event(cmd_msg->command, event_id, &msgpack, &msgpack_size)) {
    return false;
    ;
  }

  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USR;

  db_key.container_name = strdup(container_name);
  if (!db_key.container_name) {
    free(msgpack);
    return false;
  }

  db_key.usr_db_type = USR_DB_EVENTS;
  db_key.db_key.type = DB_KEY_U32;
  db_key.db_key.key.u32 = event_id;

  entry->db_key = db_key;
  entry->bump_flush_version = false;
  entry->value = msgpack;
  entry->value_size = msgpack_size;
  entry->write_condition = WRITE_COND_ALWAYS;
  return true;
}

static bool _create_event_counter_entry(char *container_name, uint32_t event_id,
                                        eng_writer_entry_t *entry) {
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USR;

  db_key.container_name = strdup(container_name);
  if (!db_key.container_name) {
    return false;
  }

  db_key.usr_db_type = USR_DB_METADATA;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = strdup(USR_NEXT_EVENT_ID_KEY);
  if (!db_key.db_key.key.s) {
    free(db_key.container_name);
    return false;
  }

  entry->db_key = db_key;
  entry->bump_flush_version = false;
  entry->value = malloc(sizeof(uint32_t));
  *(uint32_t *)entry->value = event_id;
  entry->value_size = sizeof(uint32_t);
  entry->write_condition = WRITE_COND_INT32_GREATER_THAN;
  return true;
}

static bool _create_ent_counter_entry(uint32_t ent_id,
                                      eng_writer_entry_t *entry) {
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_SYS;

  db_key.container_name = strdup(SYS_CONTAINER_NAME);
  if (!db_key.container_name) {
    return false;
  }

  db_key.sys_db_type = SYS_DB_METADATA;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = strdup(SYS_NEXT_ENT_ID_KEY);
  if (!db_key.db_key.key.s) {
    free(db_key.container_name);
    return false;
  }

  entry->db_key = db_key;
  entry->bump_flush_version = false;
  entry->value = malloc(sizeof(uint32_t));
  *(uint32_t *)entry->value = ent_id;
  entry->value_size = sizeof(uint32_t);
  entry->write_condition = WRITE_COND_INT32_GREATER_THAN;
  return true;
}

static bool _create_ent_entry(uint32_t ent_id, ast_literal_node_t *ent_node,
                              eng_writer_entry_t *entry) {
  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_SYS;

  db_key.container_name = strdup(SYS_CONTAINER_NAME);
  if (!db_key.container_name) {
    return false;
  }

  bool ent_is_str = ent_node->type == AST_LITERAL_STRING;
  db_key.sys_db_type =
      ent_is_str ? SYS_DB_STR_TO_ENTITY_ID : SYS_DB_INT_TO_ENTITY_ID;
  db_key.db_key.type = ent_is_str ? DB_KEY_STRING : DB_KEY_I64;
  if (ent_is_str) {
    db_key.db_key.key.s = strdup(ent_node->string_value);
    if (!db_key.db_key.key.s) {
      free(db_key.container_name);
      return false;
    }
  } else {
    db_key.db_key.key.i64 = ent_node->number_value;
  }

  entry->db_key = db_key;
  entry->bump_flush_version = false;
  entry->value = malloc(sizeof(uint32_t));
  *(uint32_t *)entry->value = ent_id;
  entry->value_size = sizeof(uint32_t);
  entry->write_condition = WRITE_COND_ALWAYS;
  return true;
}

static bool _idx_resolve_tag_val(const char *key, cmd_queue_msg_t *cmd_msg,
                                 int64_t *out_val) {
  if (strcmp(key, "ts") == 0) {
    *out_val = cmd_msg->command->arrival_ts / 1000000L; // Convert ns to ms
    return true;
  }

  ast_node_t *ast_node =
      ast_find_custom_tag(&cmd_msg->command->ast->command, key);

  // we only support int64 index for now
  if (ast_node && ast_node->tag.value->literal.type == AST_LITERAL_NUMBER) {
    *out_val = ast_node->tag.value->literal.number_value;
    return true;
  }

  return false;
}

static bool _create_index_entries(uint32_t event_id, cmd_queue_msg_t *cmd_msg,
                                  char *container_name,
                                  eng_container_t *user_dc,
                                  eng_writer_msg_t *msg) {
  const char *idx_key;
  index_t idx_info;
  eng_container_db_key_t db_key = {0};

  kh_foreach(user_dc->data.usr->key_to_index, idx_key, idx_info, {
    int64_t val = 0;

    if (_idx_resolve_tag_val(idx_key, cmd_msg, &val)) {
      db_key.dc_type = CONTAINER_TYPE_USR;
      db_key.container_name = strdup(container_name);
      if (!db_key.container_name) {
        return false;
      }
      db_key.index_key = strdup(idx_key);
      db_key.usr_db_type = USR_DB_INDEX;
      db_key.db_key.type = DB_KEY_I64;
      db_key.db_key.key.i64 = val;
      uint32_t i = msg->count++;
      eng_writer_entry_t *entry = &msg->entries[i];
      entry->db_key = db_key;
      entry->bump_flush_version = false;
      entry->value = malloc(sizeof(uint32_t));
      *(uint32_t *)entry->value = event_id;
      entry->value_size = sizeof(uint32_t);
      entry->write_condition = WRITE_COND_ALWAYS;
    }
  });

  return true;
}

eng_writer_msg_t *worker_create_writer_msg(cmd_queue_msg_t *cmd_msg,
                                           char *container_name,
                                           uint32_t event_id, uint32_t ent_id,
                                           ast_literal_node_t *ent_node,
                                           bool is_new_ent,
                                           eng_container_t *user_dc) {
  eng_writer_msg_t *msg = calloc(1, sizeof(eng_writer_msg_t));
  if (!msg) {
    return NULL;
  }
  uint32_t index_count = 0;
  index_get_count(user_dc->data.usr->key_to_index, &index_count);

  // Greedy: At most 4 entries (sys entity counter, usr event counter, event
  // data, entity external id -> int) + num indexes
  msg->entries = calloc(4 + index_count, sizeof(eng_writer_entry_t));
  if (!msg->entries) {
    free(msg);
    return NULL;
  }

  msg->count = 0;
  if (!_create_mpack_entry(cmd_msg, container_name, event_id,
                           &msg->entries[msg->count++])) {
    eng_writer_queue_free_msg(msg);
    return NULL;
  }

  if (event_id && // % SYNC_INTERVAL == 0 &&
      !(_create_event_counter_entry(container_name, event_id,
                                    &msg->entries[msg->count++]))) {
    eng_writer_queue_free_msg(msg);
    return NULL;
  }

  if (index_count > 0 &&
      !_create_index_entries(event_id, cmd_msg, container_name, user_dc, msg)) {
    eng_writer_queue_free_msg(msg);
    return NULL;
  }

  if (!is_new_ent)
    return msg;

  if (ent_id && // % SYNC_INTERVAL == 0 &&
      !(_create_ent_counter_entry(ent_id, &msg->entries[msg->count++]))) {
    eng_writer_queue_free_msg(msg);
    return NULL;
  }

  if (!_create_ent_entry(ent_id, ent_node, &msg->entries[msg->count++])) {
    eng_writer_queue_free_msg(msg);
    return NULL;
  }

  return msg;
}