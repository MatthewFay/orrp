#include "engine.h"
#include "cmd_context/cmd_context.h"
#include "container/container.h"
#include "core/bitmaps.h"
#include "core/hash.h"
#include "engine/api.h"
#include "engine/cmd_queue/cmd_queue.h"
#include "engine/consumer/consumer.h"
#include "engine/eng_query/eng_query.h"
#include "engine/op_queue/op_queue.h"
#include "engine/worker/worker.h"
#include "engine_writer/engine_writer.h"
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
  if (!container_init(DC_CACHE_CAPACITY, CONTAINER_FOLDER, CONTAINER_SIZE)) {
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

  const char *entity_id = command->entity_tag_value->literal.string_value;
  unsigned long hash =
      xxhash64(entity_id, strlen(entity_id), CMD_QUEUE_HASH_SEED);
  int queue_idx = hash & CMD_QUEUE_MASK;

  LOG_ACTION_DEBUG(ACT_MSG_ENQUEUED,
                   "msg_type=cmd entity_id=\"%s\" queue_id=%d", entity_id,
                   queue_idx);

  cmd_queue_t *queue = &g_cmd_queues[queue_idx];
  if (!queue) {
    LOG_ACTION_ERROR(ACT_QUEUE_INVALID, "queue_type=cmd queue_id=%d",
                     queue_idx);
    cmd_queue_free_msg(msg);
    return false;
  }

  if (!cmd_queue_enqueue(queue, msg)) {
    LOG_ACTION_WARN(ACT_QUEUE_FULL,
                    "queue_type=cmd queue_id=%d entity_id=\"%s\"", queue_idx,
                    entity_id);
    cmd_queue_free_msg(msg);
    return false;
  }

  LOG_ACTION_DEBUG(ACT_MSG_ENQUEUED, "msg_type=cmd queue_id=%d status=success",
                   queue_idx);
  return true;
}

// Takes ownership of `ast`
void eng_event(api_response_t *r, ast_node_t *ast) {
  cmd_ctx_t *cmd_ctx = build_cmd_context(ast);
  if (!cmd_ctx) {
    LOG_ACTION_ERROR(ACT_CMD_CTX_BUILD_FAILED, "context=api");
    r->err_msg = "Error generating command context";
    ast_free(ast);
    return;
  }

  if (!_eng_enqueue_cmd(cmd_ctx)) {
    LOG_ACTION_WARN(ACT_CMD_ENQUEUE_FAILED, "reason=rate_limit");
    r->err_msg = "Rate limit error, please try again";
    return;
  }

  r->is_ok = true;
}

static void _handle_query_result(eng_query_result_t *query_r,
                                 api_response_t *r) {
  r->is_ok = false;
  r->err_msg = query_r->err_msg;

  if (!(query_r->success && query_r->events)) {
    LOG_ACTION_ERROR(ACT_QUERY_ERROR, "err=\"%s\"", query_r->err_msg);
    return;
  }
  r->resp_type = API_RESP_TYPE_LIST_U32;
  uint32_t count = bitmap_get_cardinality(query_r->events);

  r->payload.list_u32.int32s = calloc(count, (sizeof(uint32_t)));
  if (!r->payload.list_u32.int32s) {
    r->err_msg = "OOM error handling query result";
    bitmap_free(query_r->events);
    return;
  }
  r->payload.list_u32.count = count;

  bitmap_to_uint32_array(query_r->events, r->payload.list_u32.int32s);

  r->is_ok = true;

  bitmap_free(query_r->events);
}

// Takes ownership of `ast`
void eng_query(api_response_t *r, ast_node_t *ast) {
  cmd_ctx_t *cmd_ctx = build_cmd_context(ast);
  if (!cmd_ctx) {
    LOG_ACTION_ERROR(ACT_CMD_CTX_BUILD_FAILED, "context=api");
    r->err_msg = "Error generating command context";
    ast_free(ast);
    return;
  }

  eng_query_result_t query_r = eng_query_exec(
      cmd_ctx, g_consumers, NUM_OP_QUEUES, OP_QUEUES_PER_CONSUMER);

  _handle_query_result(&query_r, r);

  cmd_context_free(cmd_ctx);
}