#ifndef eng_query_h
#define eng_query_h

#include "core/bitmaps.h"
#include "engine/cmd_context/cmd_context.h"
#include "engine/consumer/consumer.h"
#include "engine/eng_eval/eng_eval.h"
#include <stdint.h>

typedef struct eng_query_result_s {
  bool success;
  const char *err_msg;
  bitmap_t *events;
} eng_query_result_t;

/**
 * @brief Executes a query based on the command context.
 */
void eng_query_exec(cmd_ctx_t *cmd_ctx, consumer_t *consumers, eval_ctx_t *ctx,
                    eng_query_result_t *r);

#endif