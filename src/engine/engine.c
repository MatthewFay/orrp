#include "engine.h"
#include "cmd_context/cmd_context.h"
#include "container/container.h"
#include "core/bitmaps.h"
#include "core/data_constants.h"
#include "core/db.h"
#include "core/hash.h"
#include "engine/api.h"
#include "engine/cmd_queue/cmd_queue.h"
#include "engine/consumer/consumer.h"
#include "engine/container/container_types.h"
#include "engine/eng_eval/eng_eval.h"
#include "engine/eng_query/eng_query.h"
#include "engine/index/index.h"
#include "engine/op_queue/op_queue.h"
#include "engine/worker/worker.h"
#include "engine_writer/engine_writer.h"
#include "lmdb.h"
#include "log/log.h"
#include "query/ast.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

LOG_INIT(engine);

#define CONTAINER_FOLDER "data"
#define DC_CACHE_CAPACITY 128

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

// Initialize the db engine. Called at startup.
bool eng_init(void) {
  log_init_engine();
  if (!LOG_CATEGORY) {
    fprintf(stderr, "FATAL: Failed to initialize engine logging\n");
    return NULL;
  }

  LOG_ACTION_INFO(ACT_SYSTEM_INIT, "component=engine");
  LOG_ACTION_INFO(ACT_SYSTEM_INIT,
                  "component=engine config=\"cmd_queues=%d workers=%d "
                  "op_queues=%d consumers=%d\"",
                  NUM_CMD_QUEUEs, NUM_WORKERS, NUM_OP_QUEUES, NUM_CONSUMERS);

  // Initialize container subsystem
  if (!container_init(DC_CACHE_CAPACITY, CONTAINER_FOLDER,
                      MAX_CONTAINER_SIZE)) {
    LOG_ACTION_FATAL(ACT_SUBSYSTEM_INIT_FAILED, "subsystem=container");
    return NULL;
  }
  LOG_ACTION_INFO(ACT_SUBSYSTEM_INIT, "subsystem=container cache_capacity=%d",
                  DC_CACHE_CAPACITY);

  // Get system container
  container_result_t sys_result = container_get_system();
  if (!sys_result.success) {
    LOG_ACTION_FATAL(ACT_CONTAINER_OPEN_FAILED, "container=system err=\"%s\"",
                     sys_result.error_msg ? sys_result.error_msg
                                          : "unknown error");
    container_shutdown();
    return NULL;
  }
  LOG_ACTION_INFO(ACT_CONTAINER_OPENED, "container=system");

  // Start engine writer
  eng_writer_config_t writer_config = {};
  if (!eng_writer_start(&g_eng_writer, &writer_config)) {
    LOG_ACTION_FATAL(ACT_THREAD_START_FAILED, "thread_type=writer");
    container_shutdown();
    return NULL;
  }
  LOG_ACTION_INFO(ACT_THREAD_STARTED, "thread_type=writer");

  // Initialize command queues
  LOG_ACTION_INFO(ACT_QUEUE_INIT, "queue_type=cmd count=%d", NUM_CMD_QUEUEs);
  for (int i = 0; i < NUM_CMD_QUEUEs; i++) {
    if (!cmd_queue_init(&g_cmd_queues[i])) {
      LOG_ACTION_FATAL(ACT_QUEUE_INIT_FAILED, "queue_type=cmd queue_id=%d", i);
      container_shutdown();
      return NULL;
    }
  }
  LOG_ACTION_INFO(ACT_QUEUE_INIT, "queue_type=cmd count=%d status=complete",
                  NUM_CMD_QUEUEs);

  // Initialize operation queues
  LOG_ACTION_INFO(ACT_QUEUE_INIT, "queue_type=op count=%d", NUM_OP_QUEUES);
  for (int i = 0; i < NUM_OP_QUEUES; i++) {
    if (!op_queue_init(&g_op_queues[i])) {
      LOG_ACTION_FATAL(ACT_QUEUE_INIT_FAILED, "queue_type=op queue_id=%d", i);
      container_shutdown();
      return NULL;
    }
  }
  LOG_ACTION_INFO(ACT_QUEUE_INIT, "queue_type=op count=%d status=complete",
                  NUM_OP_QUEUES);

  // Start consumer threads
  LOG_ACTION_INFO(ACT_THREAD_POOL_STARTING, "thread_type=consumer count=%d",
                  NUM_CONSUMERS);
  for (int i = 0; i < NUM_CONSUMERS; i++) {
    consumer_config_t consumer_config = {
        .writer = &g_eng_writer,
        .flush_every_n = 128,
        .op_queues = g_op_queues,
        .op_queue_consume_start = i * OP_QUEUES_PER_CONSUMER,
        .op_queue_consume_count = OP_QUEUES_PER_CONSUMER,
        .op_queue_total_count = NUM_OP_QUEUES,
        .consumer_id = i};

    if (!consumer_start(&g_consumers[i], &consumer_config).success) {
      LOG_ACTION_FATAL(ACT_THREAD_START_FAILED,
                       "thread_type=consumer thread_id=%d op_queues=%d-%d", i,
                       consumer_config.op_queue_consume_start,
                       consumer_config.op_queue_consume_start +
                           consumer_config.op_queue_consume_count - 1);
      container_shutdown();
      return NULL;
    }
    LOG_ACTION_DEBUG(ACT_THREAD_STARTED,
                     "thread_type=consumer thread_id=%d op_queues=%d-%d", i,
                     consumer_config.op_queue_consume_start,
                     consumer_config.op_queue_consume_start +
                         consumer_config.op_queue_consume_count - 1);
  }
  LOG_ACTION_INFO(ACT_THREAD_POOL_STARTING,
                  "thread_type=consumer count=%d status=complete",
                  NUM_CONSUMERS);

  // Initialize global worker state
  if (!worker_init_global().success) {
    LOG_ACTION_FATAL(ACT_SUBSYSTEM_INIT_FAILED, "subsystem=worker_global");
    container_shutdown();
    return NULL;
  }
  LOG_ACTION_INFO(ACT_SUBSYSTEM_INIT, "subsystem=worker_global");

  // Start worker threads
  LOG_ACTION_INFO(ACT_THREAD_POOL_STARTING, "thread_type=worker count=%d",
                  NUM_WORKERS);
  for (int i = 0; i < NUM_WORKERS; i++) {
    worker_config_t worker_config = {
        .writer = &g_eng_writer,
        .cmd_queues = g_cmd_queues,
        .cmd_queue_consume_start = i * CMD_QUEUES_PER_WORKER,
        .cmd_queue_consume_count = CMD_QUEUES_PER_WORKER,
        .op_queues = g_op_queues,
        .op_queue_total_count = NUM_OP_QUEUES};

    if (!worker_start(&g_workers[i], &worker_config).success) {
      LOG_ACTION_FATAL(ACT_THREAD_START_FAILED,
                       "thread_type=worker thread_id=%d cmd_queues=%d-%d", i,
                       worker_config.cmd_queue_consume_start,
                       worker_config.cmd_queue_consume_start +
                           worker_config.cmd_queue_consume_count - 1);
      container_shutdown();
      return NULL;
    }
    LOG_ACTION_DEBUG(ACT_THREAD_STARTED,
                     "thread_type=worker thread_id=%d cmd_queues=%d-%d", i,
                     worker_config.cmd_queue_consume_start,
                     worker_config.cmd_queue_consume_start +
                         worker_config.cmd_queue_consume_count - 1);
  }
  LOG_ACTION_INFO(ACT_THREAD_POOL_STARTING,
                  "thread_type=worker count=%d status=complete", NUM_WORKERS);

  LOG_ACTION_INFO(ACT_SYSTEM_INIT, "component=engine status=complete");
  return true;
}

// Shut down the engine
void eng_shutdown(void) {
  LOG_ACTION_INFO(ACT_SYSTEM_SHUTDOWN, "component=engine");

  // Stop worker threads
  LOG_ACTION_INFO(ACT_THREAD_POOL_STOPPING, "thread_type=worker count=%d",
                  NUM_WORKERS);
  for (int i = 0; i < NUM_WORKERS; i++) {
    if (!worker_stop(&g_workers[i]).success) {
      LOG_ACTION_ERROR(ACT_THREAD_STOP_FAILED,
                       "thread_type=worker thread_id=%d", i);
    }
  }
  LOG_ACTION_INFO(ACT_THREAD_POOL_STOPPING,
                  "thread_type=worker status=complete");

  worker_destroy_global();

  // Stop engine writer before consumers
  LOG_ACTION_INFO(ACT_THREAD_STOPPING, "thread_type=writer");
  if (!eng_writer_stop(&g_eng_writer)) {
    LOG_ACTION_ERROR(ACT_THREAD_STOP_FAILED, "thread_type=writer");
  }
  LOG_ACTION_INFO(ACT_THREAD_STOPPED, "thread_type=writer");

  // Stop consumer threads
  LOG_ACTION_INFO(ACT_THREAD_POOL_STOPPING, "thread_type=consumer count=%d",
                  NUM_CONSUMERS);
  for (int i = 0; i < NUM_CONSUMERS; i++) {
    if (!consumer_stop(&g_consumers[i]).success) {
      LOG_ACTION_ERROR(ACT_THREAD_STOP_FAILED,
                       "thread_type=consumer thread_id=%d", i);
    }
  }
  LOG_ACTION_INFO(ACT_THREAD_POOL_STOPPING,
                  "thread_type=consumer status=complete");

  // Shutdown container subsystem (closes all containers)
  LOG_ACTION_INFO(ACT_SUBSYSTEM_SHUTDOWN, "subsystem=container");
  container_shutdown();

  // Destroy command queues
  LOG_ACTION_INFO(ACT_QUEUE_DESTROY, "queue_type=cmd count=%d", NUM_CMD_QUEUEs);
  for (int i = 0; i < NUM_CMD_QUEUEs; i++) {
    cmd_queue_destroy(&g_cmd_queues[i]);
  }

  // Destroy operation queues
  LOG_ACTION_INFO(ACT_QUEUE_DESTROY, "queue_type=op count=%d", NUM_OP_QUEUES);
  for (int i = 0; i < NUM_OP_QUEUES; i++) {
    op_queue_destroy(&g_op_queues[i]);
  }

  LOG_ACTION_INFO(ACT_SYSTEM_SHUTDOWN, "component=engine status=complete");
}

// Takes ownership of `cmd_ctx` (and its contained AST)
static bool _eng_enqueue_cmd(cmd_ctx_t *command) {
  cmd_queue_msg_t *msg = cmd_queue_create_msg(command);
  if (!msg) {
    LOG_ACTION_ERROR(ACT_MSG_CREATE_FAILED, "msg_type=cmd");
    cmd_context_free(command);
    return false;
  }

  unsigned long hash = 0;
  if (command->entity_tag_value->literal.type == AST_LITERAL_STRING) {
    const char *entity_id = command->entity_tag_value->literal.string_value;
    hash = xxhash64(entity_id, strlen(entity_id), CMD_QUEUE_HASH_SEED);
  } else {
    int64_t entity_id = command->entity_tag_value->literal.number_value;
    hash = xxhash64(&entity_id, sizeof(int64_t), CMD_QUEUE_HASH_SEED);
  }

  int queue_idx = hash & CMD_QUEUE_MASK;

  cmd_queue_t *queue = &g_cmd_queues[queue_idx];
  if (!queue) {
    LOG_ACTION_ERROR(ACT_QUEUE_INVALID, "queue_type=cmd queue_id=%d",
                     queue_idx);
    cmd_queue_free_msg(msg);
    return false;
  }

  if (!cmd_queue_enqueue(queue, msg)) {
    LOG_ACTION_WARN(ACT_QUEUE_FULL, "queue_type=cmd queue_id=%d", queue_idx);
    cmd_queue_free_msg(msg);
    return false;
  }

  LOG_ACTION_DEBUG(ACT_MSG_ENQUEUED, "msg_type=cmd queue_id=%d status=success",
                   queue_idx);
  return true;
}

// Takes ownership of `ast`
void eng_event(api_response_t *r, ast_node_t *ast, int64_t arrival_ts) {
  cmd_ctx_t *cmd_ctx = build_cmd_context(ast, arrival_ts);
  if (!cmd_ctx) {
    LOG_ACTION_ERROR(ACT_CMD_CTX_BUILD_FAILED, "context=eng_event");
    r->err_msg = "Error generating command context";
    ast_free(ast);
    return;
  }

  if (!_eng_enqueue_cmd(cmd_ctx)) {
    LOG_ACTION_WARN(ACT_CMD_ENQUEUE_FAILED,
                    "context=eng_event reason=rate_limit");
    r->err_msg = "Rate limit error, please try again";
    return;
  }

  r->is_ok = true;
  r->resp_type = API_RESP_TYPE_ACK;
}

// Takes ownership of `ast`
void eng_index(api_response_t *r, ast_node_t *ast) {
  cmd_ctx_t *cmd_ctx = build_cmd_context(ast, -1);
  if (!cmd_ctx) {
    LOG_ACTION_ERROR(ACT_CMD_CTX_BUILD_FAILED, "context=eng_index");
    r->err_msg = "Error generating command context";
    ast_free(ast);
    return;
  }

  index_def_t index_def = {.key = cmd_ctx->key_tag_value->literal.string_value,
                           .type = INDEX_TYPE_I64};
  db_put_result_t pr = index_add_sys(&index_def);

  switch (pr) {
  case DB_PUT_OK:
    r->is_ok = true;
    r->resp_type = API_RESP_TYPE_ACK;
    break;
  case DB_PUT_KEY_EXISTS:
    r->is_ok = false;
    r->err_msg = "Duplicate index";
    break;
  case DB_PUT_ERR:
    r->is_ok = false;
    r->err_msg = "Error adding index";
    break;
  }
  cmd_context_free(cmd_ctx);
}

static void _handle_query_result(eng_query_result_t *query_r, api_response_t *r,
                                 MDB_txn *usr_txn, eng_container_t *usr_c) {
  r->is_ok = false;
  r->err_msg = query_r->err_msg;

  if (!(query_r->success && query_r->events)) {
    LOG_ACTION_ERROR(ACT_QUERY_ERROR, "err=\"%s\"", query_r->err_msg);
    return;
  }
  r->resp_type = API_RESP_TYPE_LIST_OBJ;
  uint32_t count = bitmap_get_cardinality(query_r->events);

  LOG_ACTION_DEBUG(ACT_QUERY_STATS,
                   "context=handle_query_result event_bm_count=%d", count);

  r->payload.list_obj.objects = malloc(count * sizeof(api_obj_t));
  if (!r->payload.list_obj.objects) {
    r->err_msg = "OOM error handling query result";
    bitmap_free(query_r->events);
    return;
  }

  roaring_uint32_iterator_t *it = bitmap_iterator_create(query_r->events);
  if (!it) {
    r->err_msg = "Iterator error handling query result";
    bitmap_free(query_r->events);
    return;
  }

  db_get_result_t db_r = {0};
  db_key_t db_k = {.type = DB_KEY_U32, .key = {.u32 = 0}};
  MDB_dbi db = usr_c->data.usr->events_db;
  int i = 0;
  r->payload.list_obj.count = 0;

  while (it->has_value) {
    uint32_t event_id = it->current_value;
    db_k.key.u32 = event_id;
    if (!db_get(db, usr_txn, &db_k, &db_r) || db_r.status != DB_GET_OK) {
      LOG_ACTION_DEBUG(ACT_RACE_CONDITION,
                       "context=handle_query_result msg=\"Event ID indexed but "
                       "msgpack isn't in LMDB yet\" event_id=%d",
                       event_id);
      // TODO: handle error better, skip for now
      roaring_uint32_iterator_advance(it);
      continue;
    }

    api_obj_t *o = &r->payload.list_obj.objects[i++];
    o->id = event_id;
    o->data = db_r.value;
    o->data_size = db_r.value_len;

    // Do not clear db result as we do not want to free `db_r.value`

    roaring_uint32_iterator_advance(it);
  }

  r->is_ok = true;
  // set to `i` instead of `count` in case some events are missing
  r->payload.list_obj.count = i;
  bitmap_free(query_r->events);
}

// Takes ownership of `ast`
void eng_query(api_response_t *r, ast_node_t *ast) {
  cmd_ctx_t *cmd_ctx = build_cmd_context(ast, -1);
  if (!cmd_ctx) {
    LOG_ACTION_ERROR(ACT_CMD_CTX_BUILD_FAILED, "context=eng_query");
    r->err_msg = "Error generating command context";
    ast_free(ast);
    return;
  }

  eng_query_result_t qr = {0};
  container_result_t scr = container_get_system();
  if (!scr.success) {
    r->err_msg = "Unable to get sys container";
    return;
  }
  MDB_txn *sys_txn = db_create_txn(scr.container->env, true);
  if (!sys_txn) {
    r->err_msg = "Unable to get sys txn";
    return;
  }
  container_result_t cr =
      container_get_or_create_user(cmd_ctx->in_tag_value->literal.string_value);
  if (!cr.success) {
    db_abort_txn(sys_txn);
    r->err_msg = "Unable to get user container";
    return;
  }
  MDB_txn *user_txn = db_create_txn(cr.container->env, true);
  if (!user_txn) {
    db_abort_txn(sys_txn);
    container_release(cr.container);
    r->err_msg = "Unable to create user txn";
    return;
  }

  eval_config_t config = {.container = cr.container,
                          .sys_txn = sys_txn,
                          .user_txn = user_txn,
                          .consumers = g_consumers,
                          .op_queue_total_count = NUM_OP_QUEUES,
                          .op_queues_per_consumer = OP_QUEUES_PER_CONSUMER};

  eval_state_t state = {0};

  eval_ctx_t ctx = {.config = &config, .state = &state};

  eng_query_exec(cmd_ctx, g_consumers, &ctx, &qr);

  _handle_query_result(&qr, r, user_txn, cr.container);

  cmd_context_free(cmd_ctx);
  container_release(cr.container);
  db_abort_txn(user_txn);
  db_abort_txn(sys_txn);
}