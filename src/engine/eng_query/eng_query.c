#include "eng_query.h"
#include "core/stack.h"
#include "engine/cmd_context/cmd_context.h"
#include "query/ast.h"

static eng_query_result_t *_invalid_ast(eng_query_result_t *r) {
  r->success = false;
  r->err_msg = "Invalid AST";
  return r;
}

eng_query_result_t execute_query(cmd_ctx_t *cmd_ctx, ast_node_t *ast) {
  if (!cmd_ctx || !ast) {
    return (eng_query_result_t){.success = false, .err_msg = "Invalid args"};
  }
  eng_query_result_t r = {0};

  c_stack_t *stack = stack_create();
  if (!stack) {
    return (eng_query_result_t){.success = false,
                                .err_msg = "Cannot create stack"};
  }

  stack_push(stack, ast);

  while (!stack_is_empty(stack)) {
    ast_node_t *node = stack_pop(stack);
    if (!node) {
      return *_invalid_ast(&r);
    }
    switch (node->type) {
    case TAG_NODE:
      if (node->tag.key_type != TAG_KEY_CUSTOM) {
        return *_invalid_ast(&r);
      }
      // ast_free(node->tag.value);
      break;
    case LITERAL_NODE:
      // if (node->literal.type == LITERAL_STRING) {
      //   free(node->literal.string_value); // Free string copied for literal
      // }
      break;
    case COMPARISON_NODE:
      // ast_free(node->comparison.left);
      // ast_free(node->comparison.right);
      break;
    case LOGICAL_NODE:
      // ast_free(node->logical.left_operand);
      // ast_free(node->logical.right_operand);
      break;
    case NOT_NODE:
      // ast_free(node->not_op.operand);
      break;
    default:
      return *_invalid_ast(&r);
    }
  }

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