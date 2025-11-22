#ifndef API_H
#define API_H

// --- Public Engine API --- //

#include "query/ast.h"
#include <stdbool.h>
#include <stdint.h>

enum api_op_type { API_INVALID, API_EVENT, API_QUERY };

enum api_resp_type { API_RESP_TYPE_LIST_U32 };

typedef struct api_response_type_list_u32_s {
  uint32_t *int32s;
  uint32_t count;
} api_response_type_list_u32_t;

typedef struct api_response_s {
  enum api_op_type op_type;
  enum api_resp_type resp_type;

  union {
    api_response_type_list_u32_t list_u32;
  } payload;

  bool is_ok;
  const char *err_msg;
} api_response_t;

void free_api_response(api_response_t *r);

bool api_start_eng(void);
void api_stop_eng(void);

// The single entry point into the API/Engine layer for executing commands.
// Takes ownership of ast - caller must not free
api_response_t *api_exec(ast_node_t *ast);

#endif // API_H