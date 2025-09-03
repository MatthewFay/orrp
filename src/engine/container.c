#include "container.h"
#include "core/db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

eng_container_t *eng_container_create(eng_dc_type_t type) {
  eng_container_t *c = malloc(sizeof(eng_container_t));
  if (!c)
    return NULL;

  c->env = NULL;
  c->name = NULL;
  c->type = type;

  if (type == CONTAINER_TYPE_USER) {
    eng_user_dc_t *dc = malloc(sizeof(eng_user_dc_t));
    if (!dc) {
      free(c);
      return NULL;
    }
    c->data.usr = dc;
    return c;
  }
  eng_sys_dc_t *dc = malloc(sizeof(eng_sys_dc_t));
  if (!dc) {
    free(c);
    return NULL;
  }
  c->data.sys = dc;
  return c;
}

static void _close_user_dc(eng_container_t *c) {
  if (c->env) {
    db_close(c->env, c->data.usr->inverted_event_index_db);
    db_close(c->env, c->data.usr->event_to_entity_db);
    db_close(c->env, c->data.usr->user_dc_metadata_db);
    db_close(c->env, c->data.usr->counter_store_db);
    db_close(c->env, c->data.usr->count_index_db);

    db_env_close(c->env);
  }
  free(c);
}

static void _close_system_dc(eng_container_t *c) {
  if (c->env) {
    db_close(c->env, c->data.sys->sys_dc_metadata_db);
    db_close(c->env, c->data.sys->int_to_ent_id_db);
    db_close(c->env, c->data.sys->ent_id_to_int_db);
    db_env_close(c->env);
  }
  free(c);
}

void eng_container_close(eng_container_t *c) {
  if (!c)
    return;

  free(c->name);

  if (c->type == CONTAINER_TYPE_SYSTEM) {
    _close_system_dc(c);
  } else {
    _close_user_dc(c);
  }
}

bool eng_container_get_user_db(eng_container_t *c, eng_user_dc_db_type_t type,
                               MDB_dbi *db_out) {
  if (!c || c->type != CONTAINER_TYPE_USER)
    return false;
  switch (type) {
  case USER_DB_INVERTED_EVENT_INDEX:
    *db_out = c->data.usr->inverted_event_index_db;
  case USER_DB_EVENT_TO_ENTITY:
    *db_out = c->data.usr->event_to_entity_db;
  case USER_DB_METADATA:
    *db_out = c->data.usr->user_dc_metadata_db;
  case USER_DB_COUNTER_STORE:
    *db_out = c->data.usr->counter_store_db;
  case USER_DB_COUNT_INDEX:
    *db_out = c->data.usr->count_index_db;
  default:
    return false;
  }

  return true;
}