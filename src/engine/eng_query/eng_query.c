#include "eng_query.h"
#include "ck_epoch.h"
#include "core/bitmaps.h"
#include "core/ebr.h"
#include "engine/cmd_context/cmd_context.h"
#include "engine/eng_eval/eng_eval.h"
#include "query/ast.h"
#include <stdint.h>

void eng_query_exec(cmd_ctx_t *cmd_ctx, consumer_t *consumers, eval_ctx_t *ctx,
                    eng_query_result_t *r) {
  if (!r)
    return;
  memset(r, 0, sizeof(eng_query_result_t));
  if (!cmd_ctx || !consumers || !ctx) {
    r->err_msg = "Invalid args";
    return;
  }

  ck_epoch_section_t section;
  ebr_begin(&section);

  eng_eval_result_t eval_result =
      eng_eval_resolve_exp_to_events(cmd_ctx->where_tag_value, ctx);

  ebr_end(&section);

  eng_eval_cleanup_state(ctx->state);

  r->success = eval_result.success;
  r->events = eval_result.events;

  if (!r->success) {
    r->err_msg = eval_result.err_msg;
    return;
  }

  // default to 5k limit to avoid disruption
  uint32_t limit = 5000;
  uint32_t start_val = 0;
  if (cmd_ctx->take_tag_value) {
    limit = cmd_ctx->take_tag_value->literal.number_value;
  }
  if (cmd_ctx->cursor_tag_value) {
    start_val = cmd_ctx->cursor_tag_value->literal.number_value;
  }

  if (limit || start_val) {
    uint32_t next_cursor = bitmap_take(r->events, limit, start_val);
    r->next_cursor = next_cursor;
  }
}
