#include "container_db.h"
#include "core/db.h"
#include "core/mmap_array.h"
#include "engine/container/container_types.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int _build_container_path(char *buffer, size_t buffer_size,
                                 const char *data_dir,
                                 const char *container_name) {
  int written =
      snprintf(buffer, buffer_size, "%s/%s.mdb", data_dir, container_name);
  if (written < 0 || (size_t)written >= buffer_size) {
    return -1;
  }
  return written;
}

void container_close(eng_container_t *c) {
  if (!c) {
    return;
  }

  if (c->env) {
    if (c->type == CONTAINER_TYPE_USR) {
      db_close(c->env, c->data.usr->inverted_event_index_db);
      db_close(c->env, c->data.usr->user_dc_metadata_db);
      db_close(c->env, c->data.usr->events_db);
      mmap_array_close(&c->data.usr->event_to_entity_map);
      mmap_array_close(&c->data.usr->event_to_ts_map);
      free(c->data.usr);
    } else {
      db_close(c->env, c->data.sys->sys_dc_metadata_db);
      db_close(c->env, c->data.sys->ent_id_to_int_db);
      mmap_array_close(&c->data.sys->entity_id_map);
      free(c->data.sys);
    }
    db_env_close(c->env);
  }

  free(c->name);
  free(c);
}

eng_container_t *create_container_struct(eng_dc_type_t type) {
  eng_container_t *c = calloc(1, sizeof(eng_container_t));
  if (!c) {
    return NULL;
  }

  c->env = NULL;
  c->name = NULL;
  c->type = type;

  if (type == CONTAINER_TYPE_USR) {
    c->data.usr = calloc(1, sizeof(eng_user_dc_t));
    if (!c->data.usr) {
      free(c);
      return NULL;
    }
  } else {
    c->data.sys = calloc(1, sizeof(eng_sys_dc_t));
    if (!c->data.sys) {
      free(c);
      return NULL;
    }
  }

  return c;
}

container_result_t create_user_container(const char *name, const char *data_dir,
                                         size_t initial_container_size) {
  container_result_t result = {0};

  char c_path[MAX_CONTAINER_PATH_LENGTH];
  if (_build_container_path(c_path, sizeof(c_path), data_dir, name) < 0) {
    result.error_code = CONTAINER_ERR_PATH_TOO_LONG;
    result.error_msg = "Container path too long";
    return result;
  }

  eng_container_t *c = create_container_struct(CONTAINER_TYPE_USR);
  if (!c) {
    result.error_code = CONTAINER_ERR_ALLOC;
    result.error_msg = "Failed to allocate container structure";
    return result;
  }

  c->name = strdup(name);
  if (!c->name) {
    container_close(c);
    result.error_code = CONTAINER_ERR_ALLOC;
    result.error_msg = "Failed to duplicate container name";
    return result;
  }

  c->env = db_create_env(c_path, initial_container_size, USR_DB_COUNT);
  if (!c->env) {
    container_close(c);
    result.error_code = CONTAINER_ERR_ENV_CREATE;
    result.error_msg = "Failed to create LMDB environment";
    return result;
  }

  // Open all user databases
  bool iei = db_open(c->env, USR_DB_INVERTED_EVENT_INDEX_NAME,
                     &c->data.usr->inverted_event_index_db);
  bool meta =
      db_open(c->env, USR_DB_METADATA_NAME, &c->data.usr->user_dc_metadata_db);

  bool edb = db_open(c->env, USR_DB_EVENTS_NAME, &c->data.usr->events_db);

  if (!(iei && meta && edb)) {
    container_close(c);
    result.error_code = CONTAINER_ERR_DB_OPEN;
    result.error_msg = "Failed to open one or more databases";
    return result;
  }

  char event_to_entity_map_path[MAX_CONTAINER_PATH_LENGTH];
  snprintf(event_to_entity_map_path, sizeof(event_to_entity_map_path),
           "%s/%s_evt_ent.bin", data_dir, name);

  mmap_array_config_t event_to_entity_map_cfg = {
      .path = event_to_entity_map_path,
      .item_size = sizeof(uint32_t),
      .initial_cap = 100000 // Start small, it auto-resizes
  };

  if (mmap_array_open(&c->data.usr->event_to_entity_map,
                      &event_to_entity_map_cfg) != 0) {
    container_close(c);
    result.error_code = CONTAINER_ERR_MMAP;
    result.error_msg = "Failed to open Event-Entity mmap";
    return result;
  }

  char event_to_ts_map_path[MAX_CONTAINER_PATH_LENGTH];
  snprintf(event_to_ts_map_path, sizeof(event_to_ts_map_path),
           "%s/%s_evt_ts.bin", data_dir, name);

  mmap_array_config_t event_to_ts_map_cfg = {.path = event_to_ts_map_path,
                                             .item_size = sizeof(int64_t),
                                             .initial_cap = 100000};

  if (mmap_array_open(&c->data.usr->event_to_ts_map, &event_to_ts_map_cfg) !=
      0) {
    container_close(c);
    result.error_code = CONTAINER_ERR_MMAP;
    result.error_msg = "Failed to open Event-TS mmap";
    return result;
  }

  result.success = true;
  result.container = c;
  return result;
}

container_result_t create_system_container(const char *data_dir,
                                           size_t initial_container_size) {
  container_result_t result = {0};

  char sys_path[MAX_CONTAINER_PATH_LENGTH];
  if (_build_container_path(sys_path, sizeof(sys_path), data_dir,
                            SYS_CONTAINER_NAME) < 0) {
    result.error_code = CONTAINER_ERR_PATH_TOO_LONG;
    result.error_msg = "System container path too long";
    return result;
  }

  eng_container_t *c = create_container_struct(CONTAINER_TYPE_SYS);
  if (!c) {
    result.error_code = CONTAINER_ERR_ALLOC;
    result.error_msg = "Failed to allocate system container structure";
    return result;
  }

  c->name = strdup(SYS_CONTAINER_NAME);
  if (!c->name) {
    container_close(c);
    result.error_code = CONTAINER_ERR_ALLOC;
    result.error_msg = "Failed to duplicate system container name";
    return result;
  }

  c->env = db_create_env(sys_path, initial_container_size, SYS_DB_COUNT);
  if (!c->env) {
    container_close(c);
    result.error_code = CONTAINER_ERR_ENV_CREATE;
    result.error_msg = "Failed to create system LMDB environment";
    return result;
  }

  bool id_to_int = db_open(c->env, SYS_DB_ENT_ID_TO_INT_NAME,
                           &c->data.sys->ent_id_to_int_db);
  bool meta =
      db_open(c->env, SYS_DB_METADATA_NAME, &c->data.sys->sys_dc_metadata_db);

  if (!(id_to_int && meta)) {
    container_close(c);
    result.error_code = CONTAINER_ERR_DB_OPEN;
    result.error_msg = "Failed to open system databases";
    return result;
  }

  char map_path[MAX_CONTAINER_PATH_LENGTH];
  snprintf(map_path, sizeof(map_path), "%s/%s_ent.bin", data_dir,
           SYS_CONTAINER_NAME);

  mmap_array_config_t map_cfg = {
      .path = map_path, .item_size = 64, .initial_cap = 100000};

  if (mmap_array_open(&c->data.sys->entity_id_map, &map_cfg) != 0) {
    container_close(c);
    result.error_code = CONTAINER_ERR_MMAP;
    result.error_msg = "Failed to open system Entity mmap";
    return result;
  }

  result.success = true;
  result.container = c;
  return result;
}

bool cdb_get_user_db_handle(eng_container_t *c, eng_dc_user_db_type_t db_type,
                            MDB_dbi *db_out) {
  if (!c || c->type != CONTAINER_TYPE_USR || !db_out) {
    return false;
  }

  switch (db_type) {
  case USR_DB_INVERTED_EVENT_INDEX:
    *db_out = c->data.usr->inverted_event_index_db;
    break;
  case USR_DB_METADATA:
    *db_out = c->data.usr->user_dc_metadata_db;
    break;
  case USR_DB_EVENTS:
    *db_out = c->data.usr->events_db;
    break;
  default:
    return false;
  }

  return true;
}

bool cdb_get_system_db_handle(eng_container_t *c, eng_dc_sys_db_type_t db_type,
                              MDB_dbi *db_out) {
  if (!c || c->type != CONTAINER_TYPE_SYS || !db_out) {
    return false;
  }

  switch (db_type) {
  case SYS_DB_ENT_ID_TO_INT:
    *db_out = c->data.sys->ent_id_to_int_db;
    break;

  case SYS_DB_METADATA:
    *db_out = c->data.sys->sys_dc_metadata_db;
    break;
  default:
    return false;
  }

  return true;
}

void cdb_free_db_key_contents(eng_container_db_key_t *db_key) {
  if (!db_key) {
    return;
  }
  free(db_key->container_name);
  if (db_key->db_key.type == DB_KEY_STRING) {
    free(db_key->db_key.key.s);
  }
}