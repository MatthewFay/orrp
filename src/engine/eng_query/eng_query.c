#include "eng_query.h"
#include "core/db.h"
#include "engine/cmd_context/cmd_context.h"
#include "engine/consumer/consumer_cache.h"
#include "engine/container/container.h"
#include "engine/container/container_types.h"
#include "engine/eng_eval/eng_eval.h"
#include "lmdb.h"

eng_query_result_t eng_query_exec(cmd_ctx_t *cmd_ctx, consumer_t *consumers,
                                  uint32_t op_queue_total_count,
                                  uint32_t op_queues_per_consumer) {
  if (!cmd_ctx) {
    return (eng_query_result_t){.success = false, .err_msg = "Invalid cmd_ctx"};
  }
  eng_query_result_t r = {0};
  container_result_t scr = container_get_system();
  if (!scr.success) {
    return (eng_query_result_t){.success = false,
                                .err_msg = "Unable to get sys container"};
  }
  MDB_txn *sys_txn = db_create_txn(scr.container->env, true);
  if (!sys_txn) {
    return (eng_query_result_t){.success = false,
                                .err_msg = "Unable to get sys txn"};
  }
  container_result_t cr =
      container_get_or_create_user(cmd_ctx->in_tag_value->literal.string_value);
  if (!cr.success) {
    db_abort_txn(sys_txn);
    return (eng_query_result_t){.success = false,
                                .err_msg = "Unable to get user container"};
  }
  MDB_txn *user_txn = db_create_txn(cr.container->env, true);
  if (!user_txn) {
    db_abort_txn(sys_txn);

    container_release(cr.container);
    return (eng_query_result_t){.success = false,
                                .err_msg = "Unable to create user txn"};
  }

  // Setup config (immutable)
  eval_config_t config = {.container = cr.container,
                          .sys_txn = sys_txn,
                          .user_txn = user_txn,
                          .consumers = consumers,
                          .op_queue_total_count = op_queue_total_count,
                          .op_queues_per_consumer = op_queues_per_consumer};

  // Initialize state (mutable)
  eval_state_t state = {0};

  eval_ctx_t ctx = {.config = &config, .state = &state};
  consumer_cache_query_begin();

  eng_eval_result_t eval_result =
      eng_eval_resolve_exp_to_events(cmd_ctx->where_tag_value, &ctx);

  consumer_cache_query_end();

  eng_eval_cleanup_state(&state);

  r.success = eval_result.success;
  if (!r.success) {
    r.err_msg = eval_result.err_msg;
  }
  r.events = eval_result.events;

  container_release(cr.container);
  db_abort_txn(user_txn);
  db_abort_txn(sys_txn);

  return r;
}
