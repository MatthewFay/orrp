#ifndef eng_query_h
#define eng_query_h

#include "core/bitmaps.h"
#include "engine/cmd_context/cmd_context.h"

typedef struct eng_query_result_s {
  bool success;
  const char *err_msg;
  bitmap_t *entities;
} eng_query_result_t;

/**
 * @brief Executes a query based on the command context.
 *
 * @param cmd_ctx The validated command context.
 * @return eng_query_result_t The results of the query.
 */
eng_query_result_t eng_query_exec(cmd_ctx_t *cmd_ctx);

#endif