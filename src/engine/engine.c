#include "engine.h"
#include "cmd_context/cmd_context.h"
#include "container/container.h"
#include "context/context.h"
#include "core/db.h"
#include "core/hash.h"
#include "dc_cache/dc_cache.h"
#include "engine/api.h"
#include "engine/cmd_queue/cmd_queue.h"
#include "engine/consumer/consumer.h"
#include "engine/op_queue/op_queue.h"
#include "engine/worker/worker.h"
#include "engine_writer/engine_writer.h"
#include "lmdb.h"
#include "query/ast.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CONTAINER_FOLDER "data"
#define MAX_PATH_LENGTH 128
const size_t CONTAINER_SIZE = 1048576;
#define DC_CACHE_CAPACITY 64

const char *ENG_TXN_ERR = "Transaction error";
const char *ENG_ID_TRANSL_ERR = "Id translation error";
const char *ENG_COUNTER_ERR = "Counter error";
const char *ENG_BITMAP_ERR = "Bitmap error";
const char *ENG_TXN_COMMIT_ERR = "Transaction Commit error";

#define NUM_CMD_QUEUEs 16
#define CMD_QUEUE_MASK (NUM_CMD_QUEUEs - 1)
#define CMD_QUEUE_HASH_SEED 0

#define NUM_WORKERS 4
#define CMD_QUEUES_PER_WORKER 4

#define NUM_OP_QUEUES 16

#define NUM_CONSUMERS 4
#define OP_QUEUES_PER_CONSUMER 4

cmd_queue_t g_cmd_queues[NUM_CMD_QUEUEs];
worker_t g_workers[NUM_WORKERS];
op_queue_t g_op_queues[NUM_OP_QUEUES];
eng_writer_t g_eng_writer;
consumer_t g_consumers[NUM_CONSUMERS];

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

  eng_dc_cache_init(DC_CACHE_CAPACITY, _get_or_create_user_dc);

  eng_writer_config_t writer_config = {.flush_interval_ms = 100};
  eng_writer_start(&g_eng_writer, &writer_config);

  for (int i = 0; i < NUM_CMD_QUEUEs; i++) {
    if (!cmd_queue_init(&g_cmd_queues[i])) {
      return false;
    }
  }

  for (int i = 0; i < NUM_OP_QUEUES; i++) {
    if (!op_queue_init(&g_op_queues[i])) {
      return false;
    }
  }

  for (int i = 0; i < NUM_CONSUMERS; i++) {
    consumer_config_t consumer_config = {
        .eng_context = ctx,
        .writer = &g_eng_writer,
        .flush_every_n = 128,
        .op_queues = g_op_queues,
        .op_queue_consume_start = i * OP_QUEUES_PER_CONSUMER,
        .op_queue_consume_count = OP_QUEUES_PER_CONSUMER,
        .op_queue_total_count = NUM_OP_QUEUES,
        .consumer_id = i};
    consumer_start(&g_consumers[i], &consumer_config);
  }

  for (int i = 0; i < NUM_WORKERS; i++) {
    worker_config_t worker_config = {
        .eng_ctx = ctx,
        .cmd_queues = g_cmd_queues,
        .cmd_queue_consume_start = i * CMD_QUEUES_PER_WORKER,
        .cmd_queue_consume_count = CMD_QUEUES_PER_WORKER,
        .op_queues = g_op_queues,
        .op_queue_total_count = NUM_OP_QUEUES};
    worker_start(&g_workers[i], &worker_config);
  }

  return ctx;
}

// Shut down the engine
void eng_shutdown(eng_context_t *ctx) {
  eng_close_ctx(ctx);
  eng_dc_cache_destroy();
  for (int i = 0; i < NUM_CMD_QUEUEs; i++) {
    cmd_queue_destroy(&g_cmd_queues[i]);
  }
  for (int i = 0; i < NUM_OP_QUEUES; i++) {
    op_queue_destroy(&g_op_queues[i]);
  }
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

// static bool _write_to_event_index(eng_container_t *dc, cmd_ctx_t *cmd_ctx,
//                                   u_int32_t event_id) {
//   bitmap_cache_key_t bm_c_key;
//   bm_c_key.container_name = dc->name;
//   bm_c_key.db_type = USER_DB_INVERTED_EVENT_INDEX;

//   db_key_t key;
//   key.type = DB_KEY_STRING;
//   char key_buffer[512];
//   ast_node_t *custom_tag = cmd_ctx->custom_tags_head;

//   while (custom_tag) {
//     if (!_custom_tag_into(key_buffer, sizeof(key_buffer), custom_tag)) {
//       return false;
//     }
//     key.key.s = key_buffer;
//     bm_c_key.db_key = key;

//     if (!bitmap_cache_ingest(&bm_c_key, event_id, NULL)) {
//       return false;
//     }

//     // eng_cache_node_t *cn = eng_cache_get_or_create(
//     //     dc, USER_DB_INVERTED_EVENT_INDEX, key, CACHE_LOCK_WRITE);
//     // if (cn == NULL) {
//     //   return false;
//     // }

//     // if (cn->data_object == NULL) {
//     //   if (!db_get(dc->data.usr->inverted_event_index_db, txn, &key,
//     &get_r))
//     //   {
//     //     return false;
//     //   }
//     //   if (get_r.status == DB_GET_OK) {
//     //     cn->data_object = bitmap_deserialize(get_r.value,
//     get_r.value_len);
//     //   } else {
//     //     cn->data_object = bitmap_create();
//     //   }
//     //   free(get_r.value);
//     //   if (!cn->data_object) {
//     //     return false;
//     //   }
//     //   cn->type = CACHE_TYPE_BITMAP;
//     // }
//     // bitmap_t *bm = cn->data_object;
//     // bitmap_add(bm, event_id);

//     custom_tag = custom_tag->next;
//     // custom_tag_nodes[*num_custom_tags] = cn;
//     // (*num_custom_tags)++;
//   }
//   return true;
// }

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

// void eng_event(api_response_t *r, eng_context_t *ctx, ast_node_t *ast) {
//   uint32_t event_id = 0;
//   uint32_t ent_int_id = 0;
//   eng_container_t *sys_c = ctx->sys_c;
//   cmd_ctx_t cmd_ctx;
//   MDB_txn *usr_c_txn = NULL;
//   eng_container_t *dc = NULL;

//   build_cmd_context(&ast->command, &cmd_ctx);
//   char *str_ent_id = cmd_ctx.entity_tag_value->literal.string_value;

//   if (!entity_resolver_resolve_id(sys_c, str_ent_id, &ent_int_id)) {
//     r->err_msg = "Unable to resolve entity ID";
//     goto cleanup;
//   }

//   dc = eng_dc_cache_get(cmd_ctx.in_tag_value->literal.string_value);
//   if (!dc) {
//     r->err_msg = "Unable to open data container";
//     goto cleanup;
//   }

//   usr_c_txn = db_create_txn(dc->env, true);
//   if (!usr_c_txn) {
//     r->err_msg = ENG_TXN_ERR;
//     goto cleanup;
//   }

//   event_id = id_manager_get_next_event_id(dc, usr_c_txn);
//   if (!event_id) {
//     goto cleanup;
//   }

//   if (!_write_to_event_index(dc, &cmd_ctx, event_id)) {
//     goto cleanup;
//   }

//   // if (!_write_to_ev2ent_map(dc, event_id, ent_int_id,
//   &event_id_to_ent_node))
//   // {
//   //   goto cleanup;
//   // }

//   // TODO:Support countable tags

//   db_abort_txn(usr_c_txn);

//   r->is_ok = true;
//   return;

// cleanup:
//   if (usr_c_txn) {
//     db_abort_txn(usr_c_txn);
//   }
// }

// TODO: Add enqueue retry logic
// static bool _enqueue_msg(const char *ser_db_key, bm_cache_queue_msg_t *msg) {
//   int s_idx = _get_shard_index(ser_db_key);
//   bool enqueued = false;
//   for (int i = 0; i < MAX_ENQUEUE_ATTEMPTS; i++) {
//     if (shard_enqueue_msg(&g_bm_cache.shards[s_idx], msg)) {
//       enqueued = true;
//       break;
//     }
//     // Ring buffer is full
//     ck_pr_stall();
//     // might add a short sleep here
//   }
//   return enqueued;
// }

static bool _eng_enqueue_cmd(cmd_ctx_t *command) {
  cmd_queue_msg_t *msg = cmd_queue_create_msg(command);

  if (!msg)
    return false;
  unsigned long hash =
      xxhash64(command->entity_tag_value->literal.string_value,
               strlen(command->entity_tag_value->literal.string_value),
               CMD_QUEUE_HASH_SEED);
  int queue_idx = hash & CMD_QUEUE_MASK;
  cmd_queue_t *queue = &g_cmd_queues[queue_idx];
  if (!queue) {
    cmd_queue_free_msg(msg);
    return false;
  }
  if (!cmd_queue_enqueue(queue, msg)) {
    cmd_queue_free_msg(msg);
    return false;
  }
  return true;
}

void eng_event(api_response_t *r, ast_node_t *ast) {
  cmd_ctx_t *cmd_ctx = build_cmd_context(&ast->command);
  if (!cmd_ctx) {
    r->err_msg = "Error generating command context";
    return;
  }
  if (!_eng_enqueue_cmd(cmd_ctx)) {
    r->err_msg = "Rate limit error, please try again later";
    return;
  }
  r->is_ok = true;
}
