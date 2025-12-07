#include "worker_writer.h"
#include "core/db.h"
#include "engine/cmd_queue/cmd_queue_msg.h"
#include "engine/container/container.h"
#include "engine/container/container_types.h"
#include "engine/eng_key_format/eng_key_format.h"
#include "engine/engine_writer/engine_writer_queue_msg.h"
#include "engine/worker/encoder.h"
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

  char key_buffer[512] = {0};

  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USR;

  db_key.container_name = strdup(container_name);
  if (!db_key.container_name) {
    free(msgpack);
    return false;
  }

  db_key.usr_db_type = USR_DB_EVENTS;
  db_key.db_key.type = DB_KEY_INTEGER;
  db_key.db_key.key.i = event_id;

  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    free(db_key.container_name);
    free(msgpack);
    return false;
  }

  entry->db_key = db_key;
  entry->bump_flush_version = false;
  entry->value = msgpack;
  entry->value_size = msgpack_size;
  entry->write_condition = WRITE_COND_ALWAYS;
  return entry;
}

static bool _create_event_counter_entry(char *container_name, uint32_t event_id,
                                        eng_writer_entry_t *entry) {
  char key_buffer[512] = {0};

  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_USR;

  db_key.container_name = strdup(container_name);
  if (!db_key.container_name) {
    return false;
  }

  db_key.usr_db_type = USR_DB_EVENTS;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = strdup(USR_NEXT_EVENT_ID_KEY);
  if (!db_key.db_key.key.s) {
    free(db_key.container_name);
    return false;
  }

  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    container_free_db_key_contents(&db_key);
    return false;
  }

  entry->db_key = db_key;
  entry->bump_flush_version = false;
  entry->value = malloc(sizeof(uint32_t));
  *(uint32_t *)entry->value = event_id;
  entry->value_size = sizeof(uint32_t);
  entry->write_condition = WRITE_COND_INT32_GREATER_THAN;
  return entry;
}

static bool _create_ent_counter_entry(uint32_t ent_id,
                                      eng_writer_entry_t *entry) {
  char key_buffer[512] = {0};

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

  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    container_free_db_key_contents(&db_key);
    return false;
  }

  entry->db_key = db_key;
  entry->bump_flush_version = false;
  entry->value = malloc(sizeof(uint32_t));
  *(uint32_t *)entry->value = ent_id;
  entry->value_size = sizeof(uint32_t);
  entry->write_condition = WRITE_COND_INT32_GREATER_THAN;
  return entry;
}

static bool _create_ent_str_entry(uint32_t ent_id, char *ent_str_id,
                                  eng_writer_entry_t *entry) {
  char key_buffer[512] = {0};

  eng_container_db_key_t db_key;
  db_key.dc_type = CONTAINER_TYPE_SYS;

  db_key.container_name = strdup(SYS_CONTAINER_NAME);
  if (!db_key.container_name) {
    return false;
  }

  db_key.sys_db_type = SYS_DB_ENT_ID_TO_INT;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = strdup(ent_str_id);
  if (!db_key.db_key.key.s) {
    free(db_key.container_name);
    return false;
  }

  if (!db_key_into(key_buffer, sizeof(key_buffer), &db_key)) {
    container_free_db_key_contents(&db_key);
    return false;
  }

  entry->db_key = db_key;
  entry->bump_flush_version = false;
  entry->value = malloc(sizeof(uint32_t));
  *(uint32_t *)entry->value = ent_id;
  entry->value_size = sizeof(uint32_t);
  entry->write_condition = WRITE_COND_ALWAYS;
  return entry;
}

eng_writer_msg_t *worker_create_writer_msg(cmd_queue_msg_t *cmd_msg,
                                           char *container_name,
                                           uint32_t event_id, uint32_t ent_id,
                                           char *entity_str_id,
                                           bool is_new_ent) {
  eng_writer_msg_t *msg = malloc(sizeof(eng_writer_msg_t));
  if (!msg) {
    return NULL;
  }
  // Greedy: At most 4 entries (sys entity counter, usr event counter, event
  // data, entity str -> int)
  msg->entries = calloc(4, sizeof(eng_writer_entry_t));
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

  if (!is_new_ent)
    return msg;

  if (ent_id && // % SYNC_INTERVAL == 0 &&
      !(_create_ent_counter_entry(ent_id, &msg->entries[msg->count++]))) {
    eng_writer_queue_free_msg(msg);
    return NULL;
  }

  if (!_create_ent_str_entry(ent_id, entity_str_id,
                             &msg->entries[msg->count++])) {
    eng_writer_queue_free_msg(msg);
    return NULL;
  }
  return msg;
}