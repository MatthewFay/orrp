#ifndef API_H
#define API_H

// --- Public Engine API --- //

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

bool api_start_eng(void);
void api_stop_eng(void);

// The single entry point into the API/Engine layer for executing commands.
// Takes ownership of ast - caller must not free
api_response_t *api_exec(ast_node_t *ast);

#endif // API_H