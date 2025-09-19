#include "engine.h"
#include "bitmap_cache/bitmap_cache.h"
#include "cmd_context.h"
#include "container.h"
#include "context.h"
#include "core/db.h"
#include "dc_cache.h"
#include "engine/api.h"
#include "engine_writer/engine_writer.h"
#include "entity_resolver.h"
#include "id_manager.h"
#include "lmdb.h"
#include "query/ast.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char *SYS_NEXT_ENT_ID_KEY = "next_ent_id";
const u_int32_t SYS_NEXT_ENT_ID_INIT_VAL = 1;
const char *SYS_DB_METADATA_NAME = "sys_dc_metadata_db";
const char *USR_NEXT_EVENT_ID_KEY = "next_event_id";
const u_int32_t USR_NEXT_EVENT_ID_INIT_VAL = 1;
const char *USR_DB_METADATA_NAME = "user_dc_metadata_db";

#define CONTAINER_FOLDER "data"
#define MAX_PATH_LENGTH 128
#define SYS_CONTAINER_NAME "system"
const size_t CONTAINER_SIZE = 1048576;
const int ENTITY_RESOLVER_CACHE_CAPACITY = 262144;
#define CONTAINER_CACHE_CAPACITY 64

const char *ENG_TXN_ERR = "Transaction error";
const char *ENG_ID_TRANSL_ERR = "Id translation error";
const char *ENG_COUNTER_ERR = "Counter error";
const char *ENG_BITMAP_ERR = "Bitmap error";
const char *ENG_TXN_COMMIT_ERR = "Transaction Commit error";

const int NUM_SYS_DBS = 3;
const char *SYS_DB_ENT_ID_TO_INT_NAME = "ent_id_to_int_db";
const char *SYS_DB_INT_TO_ENT_ID_NAME = "int_to_ent_id_db";

const int NUM_USR_DBS = 5;
const char *USR_DB_INVERTED_EVENT_INDEX_NAME = "inverted_event_index_db";
const char *USR_DB_EVENT_TO_ENT_NAME = "event_to_entity_db";
const char *USR_DB_COUNTER_STORE_NAME = "counter_store_db";
const char *USR_DB_COUNT_INDEX_NAME = "count_index_db";

eng_writer_t g_eng_writer;

// Make sure data directory (where we store data containers) exists
static bool _ensure_data_dir_exists() {
  struct stat st = {0};
  if (stat(CONTAINER_FOLDER, &st) == -1) {
    if (mkdir(CONTAINER_FOLDER, 0755) != 0) {
      fprintf(stderr, "mkdir failed for '%s': %s\n", CONTAINER_FOLDER,
              strerror(errno));
      return false;
    }
  }
  return true;
}

/**
 * Safely builds the full path for a given container name.
 * Returns the number of characters written, or a negative value on error.
 */
int _get_container_path(char *buffer, size_t buffer_size,
                        const char *container_name) {
  return snprintf(buffer, buffer_size, "%s/%s.mdb", CONTAINER_FOLDER,
                  container_name);
}

eng_container_t *_get_or_create_user_dc(const char *name) {
  char c_path[MAX_PATH_LENGTH];
  if (_get_container_path(c_path, sizeof(c_path), name) < 0) {
    return NULL;
  }

  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);
  if (!c) {
    return NULL;
  }
  c->name = strdup(name);

  MDB_env *env = db_create_env(c_path, CONTAINER_SIZE, NUM_USR_DBS);

  if (!env) {
    eng_container_close(c);
    return NULL;
  }
  c->env = env;

  bool iei_r = db_open(env, USR_DB_INVERTED_EVENT_INDEX_NAME,
                       &c->data.usr->inverted_event_index_db);
  bool ee_r =
      db_open(env, USR_DB_EVENT_TO_ENT_NAME, &c->data.usr->event_to_entity_db);
  bool meta_r =
      db_open(env, USR_DB_METADATA_NAME, &c->data.usr->user_dc_metadata_db);
  bool cs_r =
      db_open(env, USR_DB_COUNTER_STORE_NAME, &c->data.usr->counter_store_db);
  bool ci_r =
      db_open(env, USR_DB_COUNT_INDEX_NAME, &c->data.usr->count_index_db);
  if (!(iei_r && ee_r && meta_r && cs_r && ci_r)) {
    eng_container_close(c);
    return NULL;
  }
  return c;
}

// Initialize the db engine, returning engine context. Called at startup.
eng_context_t *eng_init(void) {
  char system_path[MAX_PATH_LENGTH];
  MDB_dbi ent_id_to_int_db, int_to_ent_id_db, sys_dc_metadata_db;

  if (!_ensure_data_dir_exists()) {
    return NULL;
  }

  if (_get_container_path(system_path, sizeof(system_path),
                          SYS_CONTAINER_NAME) < 0)
    return NULL;

  eng_context_t *ctx = eng_create_ctx();
  if (!ctx) {
    return NULL;
  }

  eng_container_t *sys_c = eng_container_create(CONTAINER_TYPE_SYSTEM);
  if (!sys_c) {
    eng_close_ctx(ctx);
    return NULL;
  }
  sys_c->env = db_create_env(system_path, CONTAINER_SIZE, NUM_SYS_DBS);

  if (!sys_c->env) {
    eng_close_ctx(ctx);
    return NULL;
  }

  bool id_to_int_db_r =
      db_open(sys_c->env, SYS_DB_ENT_ID_TO_INT_NAME, &ent_id_to_int_db);
  bool int_to_id_db_r =
      db_open(sys_c->env, SYS_DB_INT_TO_ENT_ID_NAME, &int_to_ent_id_db);
  bool metadata_db_r =
      db_open(sys_c->env, SYS_DB_METADATA_NAME, &sys_dc_metadata_db);

  if (!(id_to_int_db_r && int_to_id_db_r && metadata_db_r)) {
    eng_close_ctx(ctx);
    eng_container_close(sys_c);
    return NULL;
  }

  sys_c->name = strdup(SYS_CONTAINER_NAME);
  sys_c->type = CONTAINER_TYPE_SYSTEM;
  sys_c->data.sys->sys_dc_metadata_db = sys_dc_metadata_db;
  sys_c->data.sys->ent_id_to_int_db = ent_id_to_int_db;
  sys_c->data.sys->int_to_ent_id_db = int_to_ent_id_db;
  ctx->sys_c = sys_c;

  eng_dc_cache_init(CONTAINER_CACHE_CAPACITY, _get_or_create_user_dc);
  id_manager_init(ctx);
  entity_resolver_init(ctx, ENTITY_RESOLVER_CACHE_CAPACITY);

  eng_writer_config_t writer_config = {.flush_interval_ms = 100};

  eng_writer_start(&g_eng_writer, &writer_config);

  bitmap_cache_init(&g_eng_writer);

  return ctx;
}

// Shut down the engine
void eng_shutdown(eng_context_t *ctx) {
  eng_close_ctx(ctx);
  eng_dc_cache_destroy();
  bitmap_cache_shutdown();
  id_manager_destroy();
  entity_resolver_destroy();
  eng_writer_stop(&g_eng_writer);
}

typedef struct incr_result_s {
  bool ok;
  u_int32_t count;
} incr_result_t;

// static void _construct_counter_key_into(char *out_buf, size_t size, char *ns,
//                                         char *key, char *id) {
//   snprintf(out_buf, size, "%s:%s:%s", ns ? ns : "", key ? key : "",
//            id ? id : "");
// }

// incr_result_t *_incr(eng_user_dc_t *c, MDB_txn *txn, char *ns, char *key,
//                      char *id) {
//   bool put_r;
//   uint32_t init_val = 1;
//   uint32_t new_count;

//   char counter_buffer[512];
//   incr_result_t *r = malloc(sizeof(incr_result_t));
//   r->ok = false;
//   r->count = 0;
//   _construct_counter_key_into(counter_buffer, sizeof(counter_buffer), ns,
//   key,
//                               id);
//   db_key_t c_key;
//   c_key.type = DB_KEY_STRING;
//   c_key.key.s = counter_buffer;
//   db_get_result_t *counter = db_get(c->event_counters_db, txn, &c_key);

//   switch (counter->status) {
//   case DB_GET_NOT_FOUND:
//     put_r = db_put(c->event_counters_db, txn, &c_key, &init_val,
//                    sizeof(uint32_t), false);
//     if (put_r) {
//       r->ok = true;
//       r->count = 1;
//     }
//     break;
//   case DB_GET_OK:
//     new_count = *(uint32_t *)counter->value + 1;
//     put_r = db_put(c->event_counters_db, txn, &c_key, &new_count,
//                    sizeof(uint32_t), false);
//     if (put_r) {
//       r->ok = true;
//       r->count = new_count;
//     }
//     break;
//   case DB_GET_ERROR:
//     break;
//   }
//   db_free_get_result(counter);
//   return r;
// }

// static void _event_index_key_into(char *out_buf, size_t size, char *ns,
//                                        char *key, incr_result_t *r) {
//   snprintf(out_buf, size, "%" PRIu32 ":%s:%s", r->count, ns ? ns : "",
//            key ? key : "");
// }

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

static bool _write_to_event_index(eng_container_t *dc, cmd_ctx_t *cmd_ctx,
                                  u_int32_t event_id) {
  bitmap_cache_key_t bm_c_key;
  bm_c_key.container_name = dc->name;
  bm_c_key.db_type = USER_DB_INVERTED_EVENT_INDEX;

  db_key_t key;
  key.type = DB_KEY_STRING;
  char key_buffer[512];
  ast_node_t *custom_tag = cmd_ctx->custom_tags_head;

  while (custom_tag) {
    if (!_custom_tag_into(key_buffer, sizeof(key_buffer), custom_tag)) {
      return false;
    }
    key.key.s = key_buffer;
    bm_c_key.db_key = key;

    if (!bitmap_cache_ingest(&bm_c_key, event_id, NULL)) {
      return false;
    }

    // eng_cache_node_t *cn = eng_cache_get_or_create(
    //     dc, USER_DB_INVERTED_EVENT_INDEX, key, CACHE_LOCK_WRITE);
    // if (cn == NULL) {
    //   return false;
    // }

    // if (cn->data_object == NULL) {
    //   if (!db_get(dc->data.usr->inverted_event_index_db, txn, &key, &get_r))
    //   {
    //     return false;
    //   }
    //   if (get_r.status == DB_GET_OK) {
    //     cn->data_object = bitmap_deserialize(get_r.value, get_r.value_len);
    //   } else {
    //     cn->data_object = bitmap_create();
    //   }
    //   free(get_r.value);
    //   if (!cn->data_object) {
    //     return false;
    //   }
    //   cn->type = CACHE_TYPE_BITMAP;
    // }
    // bitmap_t *bm = cn->data_object;
    // bitmap_add(bm, event_id);

    custom_tag = custom_tag->next;
    // custom_tag_nodes[*num_custom_tags] = cn;
    // (*num_custom_tags)++;
  }
  return true;
}

// TODO: this should not write to bitmap cache
// static bool _write_to_ev2ent_map(eng_container_t *dc, u_int32_t event_id,
//                                  u_int32_t ent_id,
//                                  eng_cache_node_t **event_id_to_ent_node) {
//   db_key_t key;
//   key.type = DB_KEY_INTEGER;
//   key.key.i = event_id;

//   *event_id_to_ent_node = eng_cache_get_or_create(dc,
//   USER_DB_EVENT_TO_ENTITY,
//                                                   key, CACHE_LOCK_WRITE);
//   if (!*event_id_to_ent_node) {
//     return false;
//   }
//   if (!(*event_id_to_ent_node)->data_object) {
//     uint32_t *ent_id_ptr = malloc(sizeof(uint32_t));
//     if (!ent_id_ptr) {
//       return false;
//     }
//     *ent_id_ptr = ent_id;
//     (*event_id_to_ent_node)->data_object = ent_id_ptr;
//     (*event_id_to_ent_node)->type = CACHE_TYPE_UINT32;
//   } else {
//     // Error case- Should not exist
//     return false;
//   }

//   return true;
// }

// TODO: counters store and index
void eng_event(api_response_t *r, eng_context_t *ctx, ast_node_t *ast) {
  uint32_t event_id = 0;
  uint32_t ent_int_id = 0;
  eng_container_t *sys_c = ctx->sys_c;
  cmd_ctx_t cmd_ctx;
  MDB_txn *usr_c_txn = NULL;
  eng_container_t *dc = NULL;

  build_cmd_context(&ast->command, &cmd_ctx);
  char *str_ent_id = cmd_ctx.entity_tag_value->literal.string_value;

  if (!entity_resolver_resolve_id(sys_c, str_ent_id, &ent_int_id)) {
    r->err_msg = "Unable to resolve entity ID";
    goto cleanup;
  }

  dc = eng_dc_cache_get(cmd_ctx.in_tag_value->literal.string_value);
  if (!dc) {
    r->err_msg = "Unable to open data container";
    goto cleanup;
  }

  usr_c_txn = db_create_txn(dc->env, true);
  if (!usr_c_txn) {
    r->err_msg = ENG_TXN_ERR;
    goto cleanup;
  }

  event_id = id_manager_get_next_event_id(dc, usr_c_txn);
  if (!event_id) {
    goto cleanup;
  }

  if (!_write_to_event_index(dc, &cmd_ctx, event_id)) {
    goto cleanup;
  }

  // if (!_write_to_ev2ent_map(dc, event_id, ent_int_id, &event_id_to_ent_node))
  // {
  //   goto cleanup;
  // }

  // TODO:Support countable tags

  db_abort_txn(usr_c_txn);

  r->is_ok = true;
  return;

cleanup:
  if (usr_c_txn) {
    db_abort_txn(usr_c_txn);
  }
}
