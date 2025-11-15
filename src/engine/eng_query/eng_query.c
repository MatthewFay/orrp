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
  eval_state_t state = {.cache_head = NULL, .intermediate_bitmaps_count = 0};

  eval_ctx_t ctx = {.config = &config, .state = &state};
  consumer_cache_query_begin();

  eng_eval_result_t eval_result =
      eng_eval_resolve_exp_to_entities(cmd_ctx->exp_tag_value, &ctx);

  consumer_cache_query_end();

  eng_eval_cleanup_state(&state);

  r.success = eval_result.success;
  if (!r.success) {
    r.err_msg = eval_result.err_msg;
  }
  r.entities = eval_result.entities;

  container_release(cr.container);
  db_abort_txn(user_txn);
  db_abort_txn(sys_txn);

  return r;
}

// #include "eng_cmd_query.h"
// #include "eng_eval.h" // <-- Include the new evaluator
// #include "engine/container_cache.h" // For getting DBs
// #include "lmdb.h"

// // Helper to resolve the final bitmap into string IDs
// static void* _resolve_ids(MDB_txn *txn, eng_sys_dc_t *sys_db,
//                           RoaringBitmap *entity_ids, cmd_ctx_t *ctx) {
//     // 1. Iterate the `entity_ids` bitmap
//     // 2. Apply ctx->pagination_offset/limit
//     // 3. For each uint32_t ID, do an mdb_get() on sys_db->int_to_ent_id_db
//     // 4. Build the final list of string IDs
//     // 5. Return that list as void* (or a specific struct)
//     return NULL; // Placeholder
// }

// eng_query_result_t query_cmd_execute(cmd_ctx_t *cmd_ctx) {
//   eng_query_result_t r = {0};
//   RoaringBitmap *result_bitmap = NULL;

//   // 1. Get container and DB handles from the context
//   //    (This replaces consumer_cache_query_begin())
//   eng_container_t *container = container_cache_get(cmd_ctx->in_tag_value);
//   if (!container || container->type != ENG_DC_USER) {
//       r.err_msg = "Invalid or non-user data container";
//       return r;
//   }
//   eng_user_dc_t *user_db = container->data.usr;
//   // You'll also need the system DB for final ID resolution
//   eng_container_t *sys_cont = container_cache_get_sys_db();
//   eng_sys_dc_t *sys_db = sys_cont->data.sys;

//   // 2. Start LMDB transaction
//   MDB_txn *txn;
//   mdb_txn_begin(container->env, NULL, MDB_RDONLY, &txn);
//   // NOTE: You need to decide if sys_cont env is the same.
//   // If not, you might need two transactions or a different cache strategy.

//   // 3. Call the re-usable evaluator
//   char *eval_err = NULL;
//   result_bitmap = resolve_expression_to_entities(
//       cmd_ctx->exp_tag_value, // Pass the expression AST
//       txn,
//       user_db,
//       &eval_err
//   );

//   if (!result_bitmap) {
//     r.err_msg = eval_err ? eval_err : "Query evaluation failed";
//     mdb_txn_abort(txn);
//     return r;
//   }

//   // 4. Resolve the final bitmap of EntityIDs into strings
//   r.results = _resolve_ids(txn, sys_db, result_bitmap, cmd_ctx);

//   // 5. Clean up
//   roaring_bitmap_free(result_bitmap);
//   mdb_txn_abort(txn);
//   // (This replaces consumer_cache_query_end())

//   r.success = true;
//   return r;
// }