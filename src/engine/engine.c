#include "engine/engine.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/api.h"
#include "engine_cache.h"
#include "lmdb.h"
#include "log.h"
#include "query/ast.h"
#include "uthash.h"
#include "uv.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CONTAINER_CACHE_CAPACITY 64
#define CONTAINER_FOLDER "data"
#define SYS_CONTAINER_NAME "system"
#define MAX_PATH_LENGTH 128
const size_t CONTAINER_SIZE = 1048576;
const int ENG_CACHE_CAPACITY = 16000;

const char *ENG_TXN_ERR = "Transaction error";
const char *ENG_ID_TRANSL_ERR = "Id translation error";
const char *ENG_COUNTER_ERR = "Counter error";
const char *ENG_BITMAP_ERR = "Bitmap error";
const char *ENG_TXN_COMMIT_ERR = "Transaction Commit error";

const int NUM_SYS_DBS = 3;
const char *SYS_DB_ENT_ID_TO_INT_NAME = "ent_id_to_int_db";
const char *SYS_DB_INT_TO_ENT_ID_NAME = "int_to_ent_id_db";
const char *SYS_DB_METADATA_NAME = "sys_dc_metadata_db";
const char *SYS_NEXT_ENT_ID_KEY = "next_ent_id";
const u_int32_t SYS_NEXT_ENT_ID_INIT_VAL = 1;

const int NUM_USR_DBS = 5;
const char *USR_DB_INVERTED_EVENT_INDEX_NAME = "inverted_event_index_db";
const char *USR_DB_EVENT_TO_ENT_NAME = "event_to_entity_db";
const char *USR_DB_METADATA_NAME = "user_dc_metadata_db";
const char *USR_DB_COUNTER_STORE_NAME = "counter_store_db";
const char *USR_DB_COUNT_INDEX_NAME = "count_index_db";
const char *USR_NEXT_EVENT_ID_KEY = "next_event_id";
const u_int32_t USR_NEXT_EVENT_ID_INIT_VAL = 1;

static bool _get_cache_key(char *buffer, size_t buffer_size,
                           const char *container_name, const char *db_name,
                           const char *key) {
  int r =
      snprintf(buffer, buffer_size, "%s/%s/%s", container_name, db_name, key);
  if (r < 0 || (size_t)r >= buffer_size) {
    return false;
  }
  return true;
}

/**
 * Safely builds the full path for a given container name.
 * Returns the number of characters written, or a negative value on error.
 */
static int _get_container_path(char *buffer, size_t buffer_size,
                               const char *container_name) {
  return snprintf(buffer, buffer_size, "%s/%s.mdb", CONTAINER_FOLDER,
                  container_name);
}

static eng_container_t *_eng_create_container(eng_dc_type_t type) {
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

static void _eng_close_container(eng_container_t *c) {
  if (!c)
    return;

  free(c->name);

  if (c->type == CONTAINER_TYPE_SYSTEM) {
    if (c->env) {
      db_close(c->env, c->data.sys->sys_dc_metadata_db);
      db_close(c->env, c->data.sys->int_to_ent_id_db);
      db_close(c->env, c->data.sys->ent_id_to_int_db);
      db_env_close(c->env);
    }
    free(c);
    return;
  }
  if (c->env) {
    db_close(c->env, c->data.usr->inverted_event_index_db);
    db_close(c->env, c->data.usr->event_to_entity_db);
    db_close(c->env, c->data.usr->user_dc_metadata_db);
    db_close(c->env, c->data.usr->counter_store_db);
    db_close(c->env, c->data.usr->count_index_db);

    db_env_close(c->env);
  }
  free(c);
  return;
}

static void _eng_dc_cache_destroy();

void eng_close_ctx(eng_context_t *ctx) {
  if (!ctx || !ctx->sys_c) {
    return;
  }

  _eng_close_container(ctx->sys_c);

  free(ctx);

  _eng_dc_cache_destroy();
  eng_cache_destroy();
}

// Used to validate user data container names for security purposes
static bool _is_valid_filename(const char *filename) {

  if (filename == NULL || filename[0] == '\0') {
    return false;
  }

  if (strlen(filename) > 64)
    return false;

  if (strcmp(filename, ".") == 0 || strcmp(filename, "..") == 0) {
    return false;
  }

  if (strpbrk(filename, "/\\") != NULL) {
    return false; // Contains '/' or '\', reject it.
  }

  return true;
}

// --- Data container CACHE ---

typedef struct eng_dc_cache_node_s {
  eng_container_t *c;
  int reference_count; // How many operations are currently using this handle
  struct eng_dc_cache_node_s *prev;
  struct eng_dc_cache_node_s *next;
  UT_hash_handle hh;
} eng_dc_cache_node_t;

typedef struct eng_cache_s {
  int size;
  // Hash map for O(1) lookups by name
  eng_dc_cache_node_t *nodes;
  // Doubly-linked list for LRU ordering
  eng_dc_cache_node_t *head;
  eng_dc_cache_node_t *tail;
  // A mutex to protect the cache structure itself during lookups/modifications
  uv_mutex_t lock;
} eng_cache_t;

// LRU cache for data containers //
static eng_cache_t g_container_cache;

static void _eng_reset_cache() {
  g_container_cache.nodes = NULL;
  g_container_cache.size = 0;
  g_container_cache.head = NULL;
  g_container_cache.tail = NULL;
}

static void _eng_init_dc_cache() {
  _eng_reset_cache();
  uv_mutex_init(&g_container_cache.lock);
}

static void _move_to_front(eng_dc_cache_node_t *n) {
  if (g_container_cache.head == n) {
    return;
  }
  if (n->prev) {
    n->prev->next = n->next;
  }
  if (n->next) {
    n->next->prev = n->prev;
  }
  if (g_container_cache.tail == n) {
    g_container_cache.tail = n->prev;
  }
  n->next = g_container_cache.head;
  n->prev = NULL;
  if (g_container_cache.head) {
    g_container_cache.head->prev = n;
  }
  g_container_cache.head = n;
  if (!g_container_cache.tail) {
    g_container_cache.tail = n;
  }
}

// Call this when done with container:
// Decrement ref count for container
static void _eng_release_container(eng_container_t *c) {
  if (!c->name)
    return;
  eng_dc_cache_node_t *n = NULL;
  ;
  uv_mutex_lock(&g_container_cache.lock);
  HASH_FIND_STR(g_container_cache.nodes, c->name, n);
  if (n) {
    n->reference_count--;
  }
  uv_mutex_unlock(&g_container_cache.lock);
}

static eng_container_t *_get_container(const char *name) {
  if (!_is_valid_filename(name))
    return NULL;
  eng_dc_cache_node_t *n;
  uv_mutex_lock(&g_container_cache.lock);
  HASH_FIND_STR(g_container_cache.nodes, name, n);
  if (n) {
    n->reference_count++;
    _move_to_front(n);
    uv_mutex_unlock(&g_container_cache.lock);
    return n->c;
  }
  if (g_container_cache.size >= CONTAINER_CACHE_CAPACITY) {
    eng_dc_cache_node_t *evict_candidate = g_container_cache.tail;
    // IMPORTANT: Only evict if no one is using it!
    if (evict_candidate && evict_candidate->reference_count <= 0) {
      _eng_close_container(evict_candidate->c);
      if (evict_candidate->prev) {
        evict_candidate->prev->next = evict_candidate->next;
      }
      if (evict_candidate->next) {
        evict_candidate->next->prev = evict_candidate->prev;
      }

      g_container_cache.tail = evict_candidate->prev;
      if (g_container_cache.tail) {
        g_container_cache.tail->next = NULL;
      } else {
        // list now empty
        g_container_cache.head = NULL;
      }

      HASH_DEL(g_container_cache.nodes, n);
      free(evict_candidate);
      g_container_cache.size--;
    }
  }

  char c_path[MAX_PATH_LENGTH];
  if (_get_container_path(c_path, sizeof(c_path), name) < 0) {
    uv_mutex_unlock(&g_container_cache.lock);
    return NULL;
  }

  n = malloc(sizeof(eng_dc_cache_node_t));
  if (!n) {
    uv_mutex_unlock(&g_container_cache.lock);
    return NULL;
  }
  eng_container_t *c = _eng_create_container(CONTAINER_TYPE_USER);
  if (!c) {
    free(n);
    uv_mutex_unlock(&g_container_cache.lock);
    return NULL;
  }
  c->name = strdup(name);

  MDB_env *env = db_create_env(c_path, CONTAINER_SIZE, NUM_USR_DBS);

  if (!env) {
    _eng_close_container(c);
    free(n);
    uv_mutex_unlock(&g_container_cache.lock);
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
    _eng_close_container(c);
    free(n);
    uv_mutex_unlock(&g_container_cache.lock);
    return NULL;
  }

  n->reference_count = 1;
  n->c = c;
  n->prev = NULL;
  n->next = g_container_cache.head;
  if (g_container_cache.head)
    g_container_cache.head->prev = n;
  g_container_cache.head = n;
  if (!g_container_cache.tail)
    g_container_cache.tail = n;
  HASH_ADD_KEYPTR(hh, g_container_cache.nodes, c->name, strlen(c->name), n);
  g_container_cache.size++;
  uv_mutex_unlock(&g_container_cache.lock);
  return c;
}

static void _eng_dc_cache_destroy() {
  uv_mutex_lock(&g_container_cache.lock);
  eng_dc_cache_node_t *n, *tmp;
  if (g_container_cache.nodes) {
    HASH_ITER(hh, g_container_cache.nodes, n, tmp) {
      _eng_close_container(n->c);

      HASH_DEL(g_container_cache.nodes, n);
      free(n);
    }
  }
  _eng_reset_cache();
  uv_mutex_unlock(&g_container_cache.lock);
}

// `int_id_out` will be 0 on failure
static void _get_next_int_id(eng_container_t *c, MDB_txn *txn,
                             u_int32_t *int_id_out) {
  *int_id_out = 0;
  eng_dc_type_t c_type = c->type;
  bool r;
  db_get_result_t next;
  db_key_t key;
  key.type = DB_KEY_STRING;
  key.key.s = c_type == CONTAINER_TYPE_SYSTEM ? SYS_NEXT_ENT_ID_KEY
                                              : USR_NEXT_EVENT_ID_KEY;
  MDB_dbi db = c_type == CONTAINER_TYPE_SYSTEM
                   ? c->data.sys->sys_dc_metadata_db
                   : c->data.usr->user_dc_metadata_db;
  if (!db_get(db, txn, &key, &next)) {
    const char *msg = c_type == CONTAINER_TYPE_SYSTEM
                          ? "Error getting next entity ID"
                          : "Error getting next event ID";
    log_error(msg);
  } else if (next.status == DB_GET_OK) {
    u_int32_t next_id_val = *(u_int32_t *)next.value;
    u_int32_t next_id_val_incr = next_id_val + 1;
    r = db_put(db, txn, &key, &next_id_val_incr, sizeof(u_int32_t), false);
    *int_id_out = r ? next_id_val : 0;
  } else if (next.status == DB_GET_NOT_FOUND) {
    u_int32_t start = c_type == CONTAINER_TYPE_SYSTEM
                          ? SYS_NEXT_ENT_ID_INIT_VAL
                          : USR_NEXT_EVENT_ID_INIT_VAL;
    r = db_put(db, txn, &key, &start, sizeof(u_int32_t), false);
    *int_id_out = r ? start : 0;
  }
  free(next.value);
}

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

// Initialize the db engine, returning engine context.
eng_context_t *eng_init(void) {
  MDB_dbi ent_id_to_int_db, int_to_ent_id_db, sys_dc_metadata_db;

  if (!_ensure_data_dir_exists()) {
    return NULL;
  }

  char system_path[MAX_PATH_LENGTH];
  if (_get_container_path(system_path, sizeof(system_path),
                          SYS_CONTAINER_NAME) < 0)
    return NULL;

  eng_context_t *ctx = malloc(sizeof(eng_context_t));
  if (!ctx) {
    return NULL;
  }

  eng_container_t *sys_c = _eng_create_container(CONTAINER_TYPE_SYSTEM);
  if (!sys_c) {
    free(ctx);
    return NULL;
  }

  sys_c->env = db_create_env(system_path, CONTAINER_SIZE, 3);
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
    free(ctx);
    _eng_close_container(sys_c);
    return NULL;
  }

  sys_c->name = strdup(SYS_CONTAINER_NAME);
  sys_c->type = CONTAINER_TYPE_SYSTEM;
  sys_c->data.sys->sys_dc_metadata_db = sys_dc_metadata_db;
  sys_c->data.sys->ent_id_to_int_db = ent_id_to_int_db;
  sys_c->data.sys->int_to_ent_id_db = int_to_ent_id_db;

  ctx->sys_c = sys_c;

  _eng_init_dc_cache();

  eng_cache_init(ENG_CACHE_CAPACITY);

  return ctx;
}

// Map string id to int id and vice versa
static bool _map(eng_sys_dc_t *sys_c, MDB_txn *txn, const char *str_id,
                 uint32_t int_id) {
  db_key_t str_id_key;
  str_id_key.type = DB_KEY_STRING;
  str_id_key.key.s = str_id;
  bool put_r = db_put(sys_c->ent_id_to_int_db, txn, &str_id_key, &int_id,
                      sizeof(u_int32_t), false);
  if (!put_r) {
    return false;
  }
  db_key_t int_id_key;
  int_id_key.type = DB_KEY_INTEGER;
  int_id_key.key.i = int_id;
  put_r = db_put(sys_c->int_to_ent_id_db, txn, &int_id_key, str_id,
                 strlen(str_id) + 1, false);
  if (!put_r) {
    return false;
  }
  return true;
}

// `ent_int_id_out` will be 0 on error
static void _map_str_id_to_int_id(eng_container_t *sys_c, MDB_txn *txn,
                                  char *str_id, uint32_t *ent_int_id_out) {
  *ent_int_id_out = 0;

  bool map_r;
  db_get_result_t r;
  db_key_t key;
  key.type = DB_KEY_STRING;
  key.key.s = str_id;

  if (!db_get(sys_c->data.sys->ent_id_to_int_db, txn, &key, &r)) {
    return;
  }
  switch (r.status) {
  case DB_GET_NOT_FOUND:
    _get_next_int_id(sys_c, txn, ent_int_id_out);
    if (*ent_int_id_out == 0)
      break;
    map_r = _map(sys_c->data.sys, txn, str_id, *ent_int_id_out);
    if (!map_r) {
      *ent_int_id_out = 0;
    }
    break;
  case DB_GET_OK:
    *ent_int_id_out = *(uint32_t *)r.value;
    break;
  case DB_GET_ERROR:
    *ent_int_id_out = 0;
    break;
  }

  free(r.value);
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

static bool _save_bitmap(MDB_dbi db, MDB_txn *txn, db_key_t *b_key,
                         bitmap_t *bm) {
  size_t serialized_size;
  void *buffer = bitmap_serialize(bm, &serialized_size);
  if (!buffer) {
    return false;
  }
  bool put_r = db_put(db, txn, b_key, buffer, serialized_size, false);
  if (!put_r) {
    free(buffer);
    return false;
  }
  return true;
}

typedef struct {
  // --- Fields for Reserved Tags ---
  // TODO: support multiple `in` tags, e.g, cross-container queries
  ast_node_t *in_tag_value;
  ast_node_t *entity_tag_value;
  ast_node_t *exp_tag_value;
  ast_node_t *take_tag_value;
  ast_node_t *cursor_tag_value;

  // --- A Single List for All Custom Tags ---
  ast_node_t *custom_tags_head;

} cmd_ctx_t;

static void _build_cmd_context(ast_command_node_t *cmd, cmd_ctx_t *ctx) {
  memset(ctx, 0, sizeof(cmd_ctx_t));

  ast_node_t *custom_tags_tail = NULL;

  for (ast_node_t *tag_node = cmd->tags; tag_node != NULL;
       tag_node = tag_node->next) {
    if (tag_node->type == TAG_NODE) {
      ast_tag_node_t *tag = &tag_node->tag;

      if (tag->key_type == TAG_KEY_RESERVED) {
        switch (tag->reserved_key) {
        case KEY_IN:
          ctx->in_tag_value = tag->value;
          break;
        case KEY_ENTITY:
          ctx->entity_tag_value = tag->value;
          break;
        case KEY_EXP:
          ctx->exp_tag_value = tag->value;
          break;
        case KEY_TAKE:
          ctx->take_tag_value = tag->value;
          break;
        case KEY_CURSOR:
          ctx->cursor_tag_value = tag->value;
          break;
        case KEY_ID:
          break;
        }
      } else {
        if (ctx->custom_tags_head == NULL) {
          ctx->custom_tags_head = tag_node;
          custom_tags_tail = tag_node;
        } else {
          custom_tags_tail->next = tag_node;
          custom_tags_tail = tag_node;
        }
      }
    }
  }

  // Terminate the new list. The last custom tag's `next`
  // pointer might still point to a reserved tag from the original list.
  if (custom_tags_tail != NULL) {
    custom_tags_tail->next = NULL;
  }
}

// static void _event_index_key_into(char *out_buf, size_t size, char *ns,
//                                        char *key, incr_result_t *r) {
//   snprintf(out_buf, size, "%" PRIu32 ":%s:%s", r->count, ns ? ns : "",
//            key ? key : "");
// }

static bool _tag_into(char *out_buf, size_t size, ast_node_t *custom_tag) {
  int r = snprintf(out_buf, size, "%s:%s", custom_tag->tag.custom_key,
                   custom_tag->tag.value->literal.string_value);
  if (r < 0 || (size_t)r >= size) {
    return false;
  }
  return true;
}
static bool _write_to_event_index(eng_container_t *dc, cmd_ctx_t *cmd_ctx,
                                  MDB_txn *txn, u_int32_t event_id) {
  db_get_result_t get_r;
  db_key_t key;
  char key_buffer[512];
  char cache_key[512];

  key.type = DB_KEY_STRING;

  ast_node_t *custom_tag = cmd_ctx->custom_tags_head;
  while (custom_tag) {

    if (!_tag_into(key_buffer, sizeof(key_buffer), custom_tag)) {
      return false;
    }
    if (!_get_cache_key(cache_key, sizeof(cache_key), dc->name,
                        USR_DB_INVERTED_EVENT_INDEX_NAME, key_buffer)) {
      return false;
    }

    eng_cache_node_t *cn = eng_cache_get_or_create(cache_key);
    if (cn == NULL) {
      return false;
    }
    key.key.s = key_buffer;

    if (cn->data_object == NULL) {
      if (!db_get(dc->data.usr->inverted_event_index_db, txn, &key, &get_r)) {
        eng_cache_release(cn);
        return false;
      }
      if (get_r.status == DB_GET_OK) {
        cn->data_object = bitmap_deserialize(get_r.value, get_r.value_len);
      } else {
        cn->data_object = bitmap_create();
      }
      free(get_r.value);
      if (!cn->data_object) {
        eng_cache_release(cn);
        return false;
      }
      cn->type = CACHE_TYPE_BITMAP;
    }
    bitmap_t *bm = cn->data_object;
    bitmap_add(bm, event_id);
    if (!_save_bitmap(dc->data.usr->inverted_event_index_db, txn, &key, bm)) {
      bitmap_free(bm);
      eng_cache_release(cn);
      return false;
    }

    custom_tag = custom_tag->next;
    eng_cache_release(cn);
  }
  return true;
}

static bool _write_to_ev2ent_map(eng_user_dc_t *dc, MDB_txn *txn,
                                 u_int32_t event_id, u_int32_t ent_id) {
  db_key_t key;
  key.type = DB_KEY_INTEGER;
  key.key.i = event_id;
  return db_put(dc->event_to_entity_db, txn, &key, &ent_id, sizeof(u_int32_t),
                false);
}

// TODO: counters store and index

void eng_event(api_response_t *r, eng_context_t *ctx, ast_node_t *ast) {
  uint32_t event_id = 0;
  uint32_t ent_int_id = 0;
  eng_container_t *sys_c = ctx->sys_c;

  cmd_ctx_t cmd_ctx;
  _build_cmd_context(&ast->command, &cmd_ctx);

  MDB_txn *sys_c_txn = db_create_txn(sys_c->env, false);

  if (!sys_c_txn) {
    r->err_msg = ENG_TXN_ERR;
    return;
  }

  _map_str_id_to_int_id(sys_c, sys_c_txn,
                        cmd_ctx.entity_tag_value->literal.string_value,
                        &ent_int_id);
  if (!ent_int_id) {
    db_abort_txn(sys_c_txn);
    r->err_msg = ENG_ID_TRANSL_ERR;

    return;
  }

  eng_container_t *dc =
      _get_container(cmd_ctx.in_tag_value->literal.string_value);
  if (!dc) {
    db_abort_txn(sys_c_txn);
    r->err_msg = "Unable to open data container";
    return;
  }
  MDB_txn *usr_c_txn = db_create_txn(dc->env, false);
  _get_next_int_id(dc, usr_c_txn, &event_id);
  if (!event_id) {
    db_abort_txn(sys_c_txn);
    db_abort_txn(usr_c_txn);
    _eng_release_container(dc);
    return;
  }

  if (!_write_to_event_index(dc, &cmd_ctx, usr_c_txn, event_id)) {
    db_abort_txn(sys_c_txn);
    db_abort_txn(usr_c_txn);
    _eng_release_container(dc);
    return;
  }

  if (!_write_to_ev2ent_map(dc->data.usr, usr_c_txn, event_id, ent_int_id)) {
    db_abort_txn(sys_c_txn);
    db_abort_txn(usr_c_txn);
    _eng_release_container(dc);
    return;
  }

  // TODO:Support countable tags

  // commit global id directory txn first - if `usr_commit` fails, it's OK.
  bool sys_commit = db_commit_txn(sys_c_txn);
  if (!sys_commit) {
    r->err_msg = ENG_TXN_COMMIT_ERR;

    // cleanup
    _eng_release_container(dc);

    return;
  }
  bool usr_commit = db_commit_txn(usr_c_txn);
  if (!usr_commit) {
    r->err_msg = ENG_TXN_COMMIT_ERR;

    // cleanup
    _eng_release_container(dc);

    return;
  }

  _eng_release_container(dc);
  r->is_ok = true;
}
