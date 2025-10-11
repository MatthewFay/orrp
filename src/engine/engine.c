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
#include "log/log.h"
#include "query/ast.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

LOG_INIT(engine);

#define MAX_PATH_LENGTH 128

#define CONTAINER_FOLDER "data"
#define DC_CACHE_CAPACITY 128
const size_t CONTAINER_SIZE = 1048576;

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
      LOG_ERROR("Failed to create data directory '%s': %s", CONTAINER_FOLDER,
                strerror(errno));
      return false;
    }
    LOG_INFO("Created data directory: %s", CONTAINER_FOLDER);
  }
  return true;
}

/**
 * Safely builds the full path for a given container name.
 * Returns the number of characters written, or a negative value on error.
 */
int _get_container_path(char *buffer, size_t buffer_size,
                        const char *container_name) {
  int written = snprintf(buffer, buffer_size, "%s/%s.mdb", CONTAINER_FOLDER,
                         container_name);
  if (written < 0 || (size_t)written >= buffer_size) {
    LOG_ERROR("Container path buffer too small for: %s", container_name);
  }
  return written;
}

eng_container_t *_get_or_create_user_dc(const char *name) {
  char c_path[MAX_PATH_LENGTH];
  if (_get_container_path(c_path, sizeof(c_path), name) < 0) {
    LOG_ERROR("Failed to build container path for: %s", name);
    return NULL;
  }

  LOG_DEBUG("Creating/opening user container: %s at %s", name, c_path);

  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);
  if (!c) {
    LOG_ERROR("Failed to allocate container structure for: %s", name);
    return NULL;
  }
  c->name = strdup(name);
  if (!c->name) {
    LOG_ERROR("Failed to duplicate container name: %s", name);
    eng_container_close(c);
    return NULL;
  }

  MDB_env *env = db_create_env(c_path, CONTAINER_SIZE, NUM_USR_DBS);
  if (!env) {
    LOG_ERROR("Failed to create LMDB environment for container: %s", name);
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
    LOG_ERROR("Failed to open one or more databases for container: %s "
              "(iei=%d, ee=%d, meta=%d, cs=%d, ci=%d)",
              name, iei_r, ee_r, meta_r, cs_r, ci_r);
    eng_container_close(c);
    return NULL;
  }

  LOG_INFO("Successfully opened user container: %s", name);
  return c;
}

// Initialize the db engine, returning engine context. Called at startup.
eng_context_t *eng_init(void) {
  log_init_engine();
  if (!LOG_CATEGORY) {
    fprintf(stderr, "FATAL: Failed to initialize engine logging\n");
    return NULL;
  }

  LOG_INFO("Initializing database engine...");
  LOG_INFO(
      "Configuration: cmd_queues=%d, workers=%d, op_queues=%d, consumers=%d",
      NUM_CMD_QUEUEs, NUM_WORKERS, NUM_OP_QUEUES, NUM_CONSUMERS);

  char system_path[MAX_PATH_LENGTH];
  MDB_dbi ent_id_to_int_db, int_to_ent_id_db, sys_dc_metadata_db;

  if (!_ensure_data_dir_exists()) {
    LOG_FATAL("Failed to ensure data directory exists");
    return NULL;
  }

  if (_get_container_path(system_path, sizeof(system_path),
                          SYS_CONTAINER_NAME) < 0) {
    LOG_FATAL("Failed to build system container path");
    return NULL;
  }

  LOG_INFO("System container path: %s", system_path);

  eng_context_t *ctx = eng_create_ctx();
  if (!ctx) {
    LOG_FATAL("Failed to create engine context");
    return NULL;
  }

  eng_container_t *sys_c = eng_container_create(CONTAINER_TYPE_SYSTEM);
  if (!sys_c) {
    LOG_FATAL("Failed to create system container");
    eng_close_ctx(ctx);
    return NULL;
  }

  sys_c->env = db_create_env(system_path, CONTAINER_SIZE, NUM_SYS_DBS);
  if (!sys_c->env) {
    LOG_FATAL("Failed to create LMDB environment for system container");
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
    LOG_FATAL("Failed to open system databases (id_to_int=%d, int_to_id=%d, "
              "metadata=%d)",
              id_to_int_db_r, int_to_id_db_r, metadata_db_r);
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

  LOG_INFO("System container initialized successfully");

  if (!eng_dc_cache_init(DC_CACHE_CAPACITY, _get_or_create_user_dc)) {
    LOG_FATAL("Failed to initialize container cache with capacity %d",
              DC_CACHE_CAPACITY);
    eng_close_ctx(ctx);
    eng_container_close(sys_c);
    return NULL;
  }
  LOG_INFO("Container cache initialized with capacity: %d", DC_CACHE_CAPACITY);

  eng_writer_config_t writer_config = {};
  if (!eng_writer_start(&g_eng_writer, &writer_config)) {
    LOG_FATAL("Failed to start engine writer");
    eng_close_ctx(ctx);
    return NULL;
  }
  LOG_INFO("Engine writer started");

  LOG_INFO("Initializing %d command queues...", NUM_CMD_QUEUEs);
  for (int i = 0; i < NUM_CMD_QUEUEs; i++) {
    if (!cmd_queue_init(&g_cmd_queues[i])) {
      LOG_FATAL("Failed to initialize command queue %d", i);
      return NULL;
    }
  }
  LOG_INFO("Command queues initialized successfully");

  LOG_INFO("Initializing %d operation queues...", NUM_OP_QUEUES);
  for (int i = 0; i < NUM_OP_QUEUES; i++) {
    if (!op_queue_init(&g_op_queues[i])) {
      LOG_FATAL("Failed to initialize operation queue %d", i);
      return NULL;
    }
  }
  LOG_INFO("Operation queues initialized successfully");

  LOG_INFO("Starting %d consumer threads...", NUM_CONSUMERS);
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

    if (!consumer_start(&g_consumers[i], &consumer_config).success) {
      LOG_FATAL("Failed to start consumer %d (queues %d-%d)", i,
                consumer_config.op_queue_consume_start,
                consumer_config.op_queue_consume_start +
                    consumer_config.op_queue_consume_count - 1);
      return NULL;
    }
    LOG_DEBUG("Consumer %d started (op_queues %d-%d)", i,
              consumer_config.op_queue_consume_start,
              consumer_config.op_queue_consume_start +
                  consumer_config.op_queue_consume_count - 1);
  }
  LOG_INFO("All consumer threads started successfully");

  if (!worker_init_global(ctx).success) {
    LOG_FATAL("Failed to initialize global worker state");
    return NULL;
  }
  LOG_INFO("Global worker state initialized");

  LOG_INFO("Starting %d worker threads...", NUM_WORKERS);
  for (int i = 0; i < NUM_WORKERS; i++) {
    worker_config_t worker_config = {
        .eng_ctx = ctx,
        .cmd_queues = g_cmd_queues,
        .cmd_queue_consume_start = i * CMD_QUEUES_PER_WORKER,
        .cmd_queue_consume_count = CMD_QUEUES_PER_WORKER,
        .op_queues = g_op_queues,
        .op_queue_total_count = NUM_OP_QUEUES};

    if (!worker_start(&g_workers[i], &worker_config).success) {
      LOG_FATAL("Failed to start worker %d (cmd_queues %d-%d)", i,
                worker_config.cmd_queue_consume_start,
                worker_config.cmd_queue_consume_start +
                    worker_config.cmd_queue_consume_count - 1);
      return NULL;
    }
    LOG_DEBUG("Worker %d started (cmd_queues %d-%d)", i,
              worker_config.cmd_queue_consume_start,
              worker_config.cmd_queue_consume_start +
                  worker_config.cmd_queue_consume_count - 1);
  }
  LOG_INFO("All worker threads started successfully");

  LOG_INFO("Database engine initialization complete");
  return ctx;
}

// Shut down the engine
void eng_shutdown(eng_context_t *ctx) {
  LOG_INFO("Shutting down database engine...");

  LOG_INFO("Stopping %d worker threads...", NUM_WORKERS);
  for (int i = 0; i < NUM_WORKERS; i++) {
    if (!worker_stop(&g_workers[i]).success) {
      LOG_ERROR("Failed to stop worker %d", i);
    }
  }
  LOG_INFO("All worker threads stopped");

  LOG_INFO("Stopping %d consumer threads...", NUM_CONSUMERS);
  for (int i = 0; i < NUM_CONSUMERS; i++) {
    if (!consumer_stop(&g_consumers[i]).success) {
      LOG_ERROR("Failed to stop consumer %d", i);
    }
  }
  LOG_INFO("All consumer threads stopped");

  LOG_INFO("Stopping engine writer...");
  if (!eng_writer_stop(&g_eng_writer)) {
    LOG_ERROR("Failed to stop engine writer");
  }
  LOG_INFO("Engine writer stopped");

  LOG_INFO("Closing engine context and destroying caches...");
  eng_close_ctx(ctx);
  eng_dc_cache_destroy();

  LOG_INFO("Destroying command queues...");
  for (int i = 0; i < NUM_CMD_QUEUEs; i++) {
    cmd_queue_destroy(&g_cmd_queues[i]);
  }

  LOG_INFO("Destroying operation queues...");
  for (int i = 0; i < NUM_OP_QUEUES; i++) {
    op_queue_destroy(&g_op_queues[i]);
  }

  LOG_INFO("Database engine shutdown complete");
}

typedef struct incr_result_s {
  bool ok;
  u_int32_t count;
} incr_result_t;
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
  if (!msg) {
    LOG_ERROR("Failed to create command queue message");
    return false;
  }

  const char *entity_id = command->entity_tag_value->literal.string_value;
  unsigned long hash =
      xxhash64(entity_id, strlen(entity_id), CMD_QUEUE_HASH_SEED);
  int queue_idx = hash & CMD_QUEUE_MASK;

  LOG_DEBUG("Enqueuing command for entity %s to queue %d", entity_id,
            queue_idx);

  cmd_queue_t *queue = &g_cmd_queues[queue_idx];
  if (!queue) {
    LOG_ERROR("Invalid command queue at index %d", queue_idx);
    cmd_queue_free_msg(msg);
    return false;
  }

  if (!cmd_queue_enqueue(queue, msg)) {
    LOG_WARN("Command queue %d full, rejecting command for entity: %s",
             queue_idx, entity_id);
    cmd_queue_free_msg(msg);
    return false;
  }

  LOG_DEBUG("Command enqueued successfully to queue %d", queue_idx);
  return true;
}

void eng_event(api_response_t *r, ast_node_t *ast) {
  cmd_ctx_t *cmd_ctx = build_cmd_context(ast);
  if (!cmd_ctx) {
    LOG_ERROR("Failed to build command context from AST");
    r->err_msg = "Error generating command context";
    return;
  }

  if (!_eng_enqueue_cmd(cmd_ctx)) {
    LOG_WARN("Failed to enqueue command - rate limit or queue full");
    r->err_msg = "Rate limit error, please try again later";
    return;
  }

  r->is_ok = true;
}