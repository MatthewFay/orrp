#include "container_db.h"
#include "core/data_constants.h"
#include "core/db.h"
#include "core/mmap_array.h"
#include "engine/container/container_types.h"
#include "engine/index/index.h"
#include "lmdb.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static bool _is_new_container(const char *path) {
  if (access(path, F_OK) == 0) {
    // 0 = File exists
    return false;
  }

  if (errno == ENOENT) {
    // ENOENT = "No such file or directory"
    // This is a new container.
    return true;
  }

  // Edge Case: File DOES exist (maybe), but we can't see it
  // due to permissions (EACCES) or other system errors.
  // Treating this as "New" is risky.
  // assume it exists to prevent overwriting/corruption attempts.
  return false;
}

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
      db_close(c->env, c->data.usr->index_registry_local_db);
      index_destroy_key_index(c);
      mmap_array_close(&c->data.usr->event_to_entity_map);
      free(c->data.usr);
    } else {
      db_close(c->env, c->data.sys->sys_dc_metadata_db);
      db_close(c->env, c->data.sys->int_to_entity_id_db);
      db_close(c->env, c->data.sys->str_to_entity_id_db);
      db_close(c->env, c->data.sys->index_registry_global_db);
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
                                         size_t max_container_size,
                                         eng_container_t *sys_c) {
  container_result_t result = {0};

  char c_path[MAX_CONTAINER_PATH_LENGTH];
  if (_build_container_path(c_path, sizeof(c_path), data_dir, name) < 0) {
    result.error_code = CONTAINER_ERR_PATH_TOO_LONG;
    result.error_msg = "Container path too long";
    return result;
  }

  bool is_new_container = _is_new_container(c_path);

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

  c->env = db_create_env(c_path, max_container_size, USR_CONTAINER_MAX_NUM_DBS);
  if (!c->env) {
    container_close(c);
    result.error_code = CONTAINER_ERR_ENV_CREATE;
    result.error_msg = "Failed to create LMDB environment";
    return result;
  }

  bool iei = db_open(c->env, USR_DB_INVERTED_EVENT_INDEX_NAME, false,
                     DB_DUP_NONE, &c->data.usr->inverted_event_index_db);
  bool meta = db_open(c->env, USR_DB_METADATA_NAME, false, DB_DUP_NONE,
                      &c->data.usr->user_dc_metadata_db);

  bool edb = db_open(c->env, USR_DB_EVENTS_NAME, true, DB_DUP_NONE,
                     &c->data.usr->events_db);

  bool ir = db_open(c->env, USR_DB_INDEX_REGISTRY_LOCAL_NAME, false,
                    DB_DUP_NONE, &c->data.usr->index_registry_local_db);

  if (!(iei && meta && edb && ir)) {
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

  if (!init_user_indexes(c, is_new_container, sys_c)) {
    container_close(c);
    result.error_code = CONTAINER_ERR_INDEX;
    result.error_msg = "Failed to initialize indexes";
    return result;
  }

  result.success = true;
  result.container = c;
  return result;
}

container_result_t create_system_container(const char *data_dir,
                                           size_t max_container_size) {
  container_result_t result = {0};

  char sys_path[MAX_CONTAINER_PATH_LENGTH];
  if (_build_container_path(sys_path, sizeof(sys_path), data_dir,
                            SYS_CONTAINER_NAME) < 0) {
    result.error_code = CONTAINER_ERR_PATH_TOO_LONG;
    result.error_msg = "System container path too long";
    return result;
  }

  bool is_new_container = _is_new_container(sys_path);

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

  c->env = db_create_env(sys_path, max_container_size, SYS_DB_COUNT);
  if (!c->env) {
    container_close(c);
    result.error_code = CONTAINER_ERR_ENV_CREATE;
    result.error_msg = "Failed to create system LMDB environment";
    return result;
  }

  bool ent_str_to_id = db_open(c->env, SYS_DB_STR_TO_ENTITY_NAME, false,
                               DB_DUP_NONE, &c->data.sys->str_to_entity_id_db);
  bool ent_int_to_id = db_open(c->env, SYS_DB_INT_TO_ENTITY_NAME, true,
                               DB_DUP_NONE, &c->data.sys->int_to_entity_id_db);

  bool meta = db_open(c->env, SYS_DB_METADATA_NAME, false, DB_DUP_NONE,
                      &c->data.sys->sys_dc_metadata_db);
  bool ir = db_open(c->env, SYS_DB_INDEX_REGISTRY_GLOBAL_NAME, false,
                    DB_DUP_NONE, &c->data.sys->index_registry_global_db);

  if (!(ent_str_to_id && ent_int_to_id && meta && ir)) {
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

  if (is_new_container && !init_sys_index_registry(c)) {
    container_close(c);
    result.error_code = CONTAINER_ERR_INDEX;
    result.error_msg = "Failed to init system index registry";
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
  case USR_DB_INDEX_REGISTRY_LOCAL:
    *db_out = c->data.usr->index_registry_local_db;
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
  case SYS_DB_STR_TO_ENTITY_ID:
    *db_out = c->data.sys->str_to_entity_id_db;
    break;

  case SYS_DB_INT_TO_ENTITY_ID:
    *db_out = c->data.sys->int_to_entity_id_db;
    break;

  case SYS_DB_METADATA:
    *db_out = c->data.sys->sys_dc_metadata_db;
    break;
  case SYS_DB_INDEX_REGISTRY_GLOBAL:
    *db_out = c->data.sys->index_registry_global_db;
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