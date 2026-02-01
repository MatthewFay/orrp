#include "eng_query.h"
#include "core/bitmaps.h"
#include "engine/cmd_context/cmd_context.h"
#include "engine/consumer/consumer_cache.h"
#include "engine/eng_eval/eng_eval.h"
#include "query/ast.h"

static void _handle_pagination(ast_node_t *cursor_tag, eng_query_result_t *r) {}

static void _handle_limit(ast_node_t *take_tag, eng_query_result_t *r) {
  bitmap_take(r->events, take_tag->literal.number_value);
}

void eng_query_exec(cmd_ctx_t *cmd_ctx, consumer_t *consumers, eval_ctx_t *ctx,
                    eng_query_result_t *r) {
  if (!r)
    return;
  memset(r, 0, sizeof(eng_query_result_t));
  if (!cmd_ctx || !consumers || !ctx) {
    r->err_msg = "Invalid args";
    return;
  }

  consumer_cache_query_begin();

  eng_eval_result_t eval_result =
      eng_eval_resolve_exp_to_events(cmd_ctx->where_tag_value, ctx);

  consumer_cache_query_end();

  eng_eval_cleanup_state(ctx->state);

  r->success = eval_result.success;
  r->events = eval_result.events;

  if (!r->success) {
    r->err_msg = eval_result.err_msg;
    return;
  }

  if (cmd_ctx->cursor_tag_value) {
    _handle_pagination(cmd_ctx->cursor_tag_value, r);
  }
  if (cmd_ctx->take_tag_value) {
    _handle_limit(cmd_ctx->take_tag_value, r);
  }
}
