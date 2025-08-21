#include "engine/engine.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/api.h"
#include "lmdb.h"
#include "log.h"
#include "query/ast.h"
#include "uthash.h"
#include "uv.h"
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CONTAINER_CACHE_CAPACITY 64
#define CONTAINER_FOLDER "data"
#define SYS_CONTAINER_NAME "system"
#define MAX_PATH_LENGTH 128
const size_t CONTAINER_SIZE = 1048576;

const char *ENG_TXN_ERR = "Transaction error";
const char *ENG_ID_TRANSL_ERR = "Id translation error";
const char *ENG_COUNTER_ERR = "Counter error";
const char *ENG_BITMAP_ERR = "Bitmap error";
const char *ENG_TXN_COMMIT_ERR = "Transaction Commit error";

const char *DB_ID_TO_INT_NAME = "id_to_int";
const char *DB_INT_TO_ID_NAME = "int_to_id";
const char *DB_METADATA_NAME = "metadata";
const char *NEXT_ID_KEY = "next_id";
const u_int32_t NEXT_ID_INIT_VAL = 1;
const char *DB_EVENT_COUNTERS_NAME = "event_counters";
const char *DB_BITMAPS_NAME = "bitmaps";

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
      db_close(c->env, c->data.sys->metadata_db);
      db_close(c->env, c->data.sys->int_to_id_db);
      db_close(c->env, c->data.sys->id_to_int_db);
      db_env_close(c->env);
    }
    free(c);
    return;
  }
  if (c->env) {
    db_close(c->env, c->data.usr->bitmaps_db);
    db_close(c->env, c->data.usr->event_counters_db);
    db_env_close(c->env);
  }
  free(c);
  return;
}

void eng_close_ctx(eng_context_t *ctx) {
  if (!ctx || !ctx->sys_c) {
    return;
  }

  _eng_close_container(ctx->sys_c);

  free(ctx);
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

typedef struct eng_cache_node_s {
  eng_container_t *c;
  int reference_count; // How many operations are currently using this handle
  struct eng_cache_node_s *prev;
  struct eng_cache_node_s *next;
  UT_hash_handle hh;
} eng_cache_node_t;

typedef struct eng_cache_s {
  int size;
  // Hash map for O(1) lookups by name
  eng_cache_node_t *nodes;
  // Doubly-linked list for LRU ordering
  eng_cache_node_t *head;
  eng_cache_node_t *tail;
  // A mutex to protect the cache structure itself during lookups/modifications
  uv_mutex_t lock;
} eng_cache_t;

// LRU cache for data containers //
static eng_cache_t g_container_cache;

static void _eng_init_cache() {
  g_container_cache.nodes = NULL;
  g_container_cache.size = 0;
  g_container_cache.head = NULL;
  g_container_cache.tail = NULL;
}

static void _move_to_front(eng_cache_node_t *n) {
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
  eng_cache_node_t *n = NULL;
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
  eng_cache_node_t *n;
  uv_mutex_lock(&g_container_cache.lock);
  HASH_FIND_STR(g_container_cache.nodes, name, n);
  if (n) {
    n->reference_count++;
    _move_to_front(n);
    uv_mutex_unlock(&g_container_cache.lock);
    return n->c;
  }
  if (g_container_cache.size >= CONTAINER_CACHE_CAPACITY) {
    eng_cache_node_t *evict_candidate = g_container_cache.tail;
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

  n = malloc(sizeof(eng_cache_node_t));
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

  MDB_env *env = db_create_env(name, CONTAINER_SIZE, 2);

  if (!env) {
    _eng_close_container(c);
    free(n);
    uv_mutex_unlock(&g_container_cache.lock);
    return NULL;
  }
  c->env = env;

  bool bm_r = db_open(env, DB_BITMAPS_NAME, &c->data.usr->bitmaps_db);
  bool ec_r =
      db_open(env, DB_EVENT_COUNTERS_NAME, &c->data.usr->event_counters_db);
  if (!(bm_r && ec_r)) {
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

static void _db_cache_free() {
  eng_cache_node_t *n, *tmp;
  if (g_container_cache.nodes) {
    HASH_ITER(hh, g_container_cache.nodes, n, tmp) {
      _eng_close_container(n->c);

      HASH_DEL(g_container_cache.nodes, n);
      free(n);
    }
  }
  // reset
  _eng_init_cache();
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

// n_id will be 0 on failure !
static void _get_next_n_id(eng_sys_dc_t *sys_c, MDB_txn *txn, u_int32_t *n_id) {
  bool r;
  db_key_t key;
  key.type = DB_KEY_STRING;
  key.key.s = NEXT_ID_KEY;
  db_get_result_t *next = db_get(sys_c->metadata_db, txn, &key);
  if (next->status == DB_GET_OK) {
    u_int32_t next_id_val = *(u_int32_t *)next->value;
    u_int32_t next_id_val_incr = next_id_val + 1;
    r = db_put(sys_c->metadata_db, txn, &key, &next_id_val_incr,
               sizeof(u_int32_t), false);
    *n_id = r ? next_id_val : 0;
  } else {
    log_error("Unable to get next numeric id");
    *n_id = 0; // Should only happen if DB has not been initialized!
  }
  db_free_get_result(next);
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
  MDB_dbi id_to_int_db, int_to_id_db, metadata_db;

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
  bool id_to_int_db_r = db_open(sys_c->env, DB_ID_TO_INT_NAME, &id_to_int_db);
  bool int_to_id_db_r = db_open(sys_c->env, DB_INT_TO_ID_NAME, &int_to_id_db);
  bool metadata_db_r = db_open(sys_c->env, DB_METADATA_NAME, &metadata_db);

  if (!(id_to_int_db_r && int_to_id_db_r && metadata_db_r)) {
    free(ctx);
    _eng_close_container(sys_c);
    return NULL;
  }

  sys_c->name = SYS_CONTAINER_NAME;
  sys_c->type = CONTAINER_TYPE_SYSTEM;
  sys_c->data.sys->metadata_db = metadata_db;
  sys_c->data.sys->id_to_int_db = id_to_int_db;
  sys_c->data.sys->int_to_id_db = int_to_id_db;

  MDB_txn *txn = db_create_txn(sys_c->env, false);
  if (!txn) {
    eng_close_ctx(ctx);

    return NULL;
  }

  // Make sure next_id counter does not exist before creating
  db_key_t key;
  key.type = DB_KEY_STRING;
  key.key.s = NEXT_ID_KEY;
  db_get_result_t *r = db_get(metadata_db, txn, &key);

  if (r->status == DB_GET_NOT_FOUND) {
    bool put_r = db_put(metadata_db, txn, &key, &NEXT_ID_INIT_VAL,
                        sizeof(u_int32_t), false);
    bool committed = put_r && db_commit_txn(txn);
    db_free_get_result(r);
    return committed ? ctx : NULL;
  }

  db_abort_txn(txn);
  bool ok = r->status == DB_GET_OK;

  db_free_get_result(r);
  return ok ? ctx : NULL;
}

static bool _map(eng_sys_dc_t *sys_c, MDB_txn *txn, const char *id,
                 uint32_t n_id) {
  db_key_t id_key;
  id_key.type = DB_KEY_STRING;
  id_key.key.s = id;
  bool put_r = db_put(sys_c->id_to_int_db, txn, &id_key, &n_id,
                      sizeof(u_int32_t), false);
  if (!put_r) {
    return false;
  }
  db_key_t n_id_key;
  n_id_key.type = DB_KEY_INTEGER;
  n_id_key.key.i = n_id;
  put_r =
      db_put(sys_c->int_to_id_db, txn, &n_id_key, id, strlen(id) + 1, false);
  if (!put_r) {
    return false;
  }
  return true;
}

// ent_int_id will be 0 on error
static void _map_str_id_to_numeric(eng_sys_dc_t *sys_c, MDB_txn *txn, char *id,
                                   uint32_t *ent_int_id) {
  bool map_r;
  db_key_t key;
  key.type = DB_KEY_STRING;
  key.key.s = id;
  db_get_result_t *r = db_get(sys_c->id_to_int_db, txn, &key);

  switch (r->status) {
  case DB_GET_NOT_FOUND:
    _get_next_n_id(sys_c, txn, n_id);
    if (!n_id)
      break;
    map_r = _map(sys_c, txn, id, *n_id);
    if (!map_r)
      *n_id = 0;
    break;
  case DB_GET_OK:
    *n_id = *(uint32_t *)r->value;

    break;
  case DB_GET_ERROR:
    *n_id = 0;
    break;
  }

  db_free_get_result(r);
}

typedef struct incr_result_s {
  bool ok;
  u_int32_t count;
} incr_result_t;

static void _construct_counter_key_into(char *out_buf, size_t size, char *ns,
                                        char *key, char *id) {
  snprintf(out_buf, size, "%s:%s:%s", ns ? ns : "", key ? key : "",
           id ? id : "");
}

static void _construct_bitmap_key_into(char *out_buf, size_t size, char *ns,
                                       char *key, incr_result_t *r) {
  snprintf(out_buf, size, "%" PRIu32 ":%s:%s", r->count, ns ? ns : "",
           key ? key : "");
}

incr_result_t *_incr(eng_user_dc_t *c, MDB_txn *txn, char *ns, char *key,
                     char *id) {
  bool put_r;
  uint32_t init_val = 1;
  uint32_t new_count;

  char counter_buffer[512];
  incr_result_t *r = malloc(sizeof(incr_result_t));
  r->ok = false;
  r->count = 0;
  _construct_counter_key_into(counter_buffer, sizeof(counter_buffer), ns, key,
                              id);
  db_key_t c_key;
  c_key.type = DB_KEY_STRING;
  c_key.key.s = counter_buffer;
  db_get_result_t *counter = db_get(c->event_counters_db, txn, &c_key);

  switch (counter->status) {
  case DB_GET_NOT_FOUND:
    put_r = db_put(c->event_counters_db, txn, &c_key, &init_val,
                   sizeof(uint32_t), false);
    if (put_r) {
      r->ok = true;
      r->count = 1;
    }
    break;
  case DB_GET_OK:
    new_count = *(uint32_t *)counter->value + 1;
    put_r = db_put(c->event_counters_db, txn, &c_key, &new_count,
                   sizeof(uint32_t), false);
    if (put_r) {
      r->ok = true;
      r->count = new_count;
    }
    break;
  case DB_GET_ERROR:
    break;
  }
  db_free_get_result(counter);
  return r;
}

static bool _save_bitmap(eng_user_dc_t *c, MDB_txn *txn, db_key_t *b_key,
                         bitmap_t *bm) {
  size_t serialized_size;
  void *buffer = bitmap_serialize(bm, &serialized_size);
  if (!buffer) {
    return false;
  }
  bool put_r =
      db_put(c->bitmaps_db, txn, b_key, buffer, serialized_size, false);
  if (!put_r) {
    free(buffer);
    return false;
  }
  return true;
}

static bool _upsert_bitmap(eng_user_dc_t *c, MDB_txn *txn, char *ns, char *key,
                           uint32_t n_id, incr_result_t *counter_r) {
  bool r = false;
  char bitmap_buffer[512];
  bitmap_t *bm;
  _construct_bitmap_key_into(bitmap_buffer, sizeof(bitmap_buffer), ns, key,
                             counter_r);
  db_key_t b_key;

  b_key.type = DB_KEY_STRING;
  b_key.key.s = bitmap_buffer;
  db_get_result_t *get_r = db_get(c->bitmaps_db, txn, &b_key);
  switch (get_r->status) {
  case DB_GET_OK:
    bm = bitmap_deserialize(get_r->value, get_r->value_len);
    if (!bm)
      break;
    bitmap_add(bm, n_id);
    r = _save_bitmap(c, txn, &b_key, bm);
    if (!r)
      bitmap_free(bm);
    break;
  case DB_GET_NOT_FOUND:
    bm = bitmap_create();
    bitmap_add(bm, n_id);
    r = _save_bitmap(c, txn, &b_key, bm);
    if (!r)
      bitmap_free(bm);
    break;
  case DB_GET_ERROR:
    break;
  }
  db_free_get_result(get_r);
  return r;
}

typedef struct {
  // --- Fields for Reserved Tags ---
  ast_node_t *in_tag_value;
  ast_node_t *entity_tag_value;
  ast_node_t *exp_tag_value;
  ast_node_t *take_tag_value;
  ast_node_t *cursor_tag_value;

  // --- A Single List for All Custom Tags ---
  ast_node_t *custom_tags_head;

} cmd_ctx_t;

static void _build_cmd_context(ast_command_node_t *cmd, cmd_ctx_t *ctx) {
  memset(ctx, 0, sizeof(cmd_ctx));

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
        tag_node->next = ctx->custom_tags_head;
        ctx->custom_tags_head = tag_node;
      }
    }
  }
}

void eng_event(api_response_t *r, eng_context_t *ctx, ast_node_t *ast) {
  uint32_t ent_int_id = 0;
  eng_container_t *sys_c = ctx->sys_c;

  cmd_ctx_t cmd_ctx;
  _build_cmd_context(ast->command, &ctx);

  MDB_txn *sys_c_txn = db_create_txn(sys_c->env, false);

  if (!sys_c_txn) {
    r->err_msg = ENG_TXN_ERR;
    return;
  }

  _map_str_id_to_numeric(sys_c->data.sys, sys_c_txn,
                         cmd_ctx.entity_tag_value->literal.string_value, &n_id);
  if (!n_id) {
    db_abort_txn(sys_c_txn);
    r->err_msg = ENG_ID_TRANSL_ERR;
    return;
  }

  eng_container_t *dc =
      _get_container(cmd_ctx.in_tag_value->literal.string_value);
  if (!dc) {
    r->err_msg = "Unable to get data container!";
    // TODO: clean up
    return;
  }

  _eng_release_container(dc);

  incr_result_t *counter_r = _incr(NULL, txn, ns, key, id);
  if (!counter_r->ok) {
    db_abort_txn(txn);
    free(counter_r);
    r->err_msg = ENG_COUNTER_ERR;
    return;
  }
  bool bm_r = _upsert_bitmap(NULL, txn, ns, key, n_id, counter_r);
  if (!bm_r) {
    db_abort_txn(txn);
    free(counter_r);
    r->err_msg = ENG_BITMAP_ERR;
    return;
  }
  free(counter_r);

  bool txn_r = db_commit_txn(txn);
  if (!txn_r) {
    r->err_msg = ENG_TXN_COMMIT_ERR;
    return;
  }
  r->is_ok = true;
}
