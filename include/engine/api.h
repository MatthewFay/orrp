#ifndef API_H
#define API_H

#include "engine.h"
#include "query/ast.h"
#include <stdbool.h>

enum api_op_type { API_INVALID, API_EVENT };

typedef struct api_response_s {
  enum api_op_type op_type;
  bool is_ok;
  void *data;
  const char *err_msg;
} api_response_t;

void free_api_response(api_response_t *r);

// The single entry point into the API/Engine layer.
api_response_t *api_exec(ast_node_t *ast, eng_context_t *ctx);

#endif // API_H