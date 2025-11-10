#include "eng_eval.h"
#include "core/bitmaps.h"
#include "engine/container/container_types.h"
#include "lmdb.h"
#include "query/ast.h"

typedef struct eval_bitmap_s {
  bitmap_t *bm;
  // if true, we own the bitmap (can mutate)
  bool own;
} eval_bitmap_t;

typedef struct eval_ctx_s {
  eng_container_t *container;
  MDB_txn *user_txn;
  MDB_txn *sys_txn;
  eng_eval_result_t result;
  eval_bitmap_t eval_bitmaps[128];
  unsigned int eval_bitmap_count;
} eval_ctx_t;

static eval_bitmap_t *_eval(ast_node_t *node, eval_ctx_t *ctx) {
  if (!node) {
    ctx->result.err_msg = "Invalid node";
    return NULL;
  }
  eval_bitmap_t *operand1 = NULL;
  eval_bitmap_t *operand2 = NULL;
  switch (node->type) {
  case NOT_NODE:
    operand1 = _eval(node->not_op.operand, ctx);
    // return operand1 ? bitmap_not(operand1, container_scoped_entitiy_ids) :
    // NULL;
    break;
  case LOGICAL_NODE:
    operand1 = _eval(node->logical.left_operand, ctx);
    if (!operand1)
      return NULL;
    operand2 = _eval(node->logical.right_operand, ctx);
    if (!operand2)
      return NULL;

    if (node->logical.op == LOGIC_NODE_AND) {
      return NULL;
    }
    return NULL;
  case TAG_NODE:
    break;
  case COMPARISON_NODE:
    break;
  default:
    ctx->result.err_msg = "Invalid node type";
    return NULL;
  }
}

eng_eval_result_t eng_eval_resolve_exp_to_entities(ast_node_t *exp,
                                                   eng_container_t *container,
                                                   MDB_txn *user_txn,
                                                   MDB_txn *sys_txn) {
  if (!exp || !container || !user_txn || !sys_txn) {
    return (eng_eval_result_t){.success = false, .err_msg = "Invalid args"};
  }

  eval_ctx_t ctx = {.container = container,
                    .result = {0},
                    .sys_txn = sys_txn,
                    .user_txn = user_txn,
                    .eval_bitmap_count = 0,
                    .eval_bitmaps = {0}};
  eval_bitmap_t *ebm = _eval(exp, &ctx);

  for (unsigned int i = 0; i < ctx.eval_bitmap_count; i++) {
    eval_bitmap_t *tmp_ebm = &ctx.eval_bitmaps[i];

    if (tmp_ebm == ebm)
      continue;

    if (tmp_ebm->own) {
      bitmap_free(tmp_ebm->bm);
    }
  }

  if (ebm) {
    ctx.result.success = true;
    ctx.result.entities = ebm->bm;
  } else if (!ctx.result.err_msg)
    ctx.result.err_msg = "Unknown error";
  return ctx.result;
}