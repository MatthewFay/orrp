#ifndef API_H
#define API_H

// --- Public Engine API --- //

#include "query/ast.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

enum api_op_type { API_INVALID, API_EVENT, API_QUERY, API_INDEX };

enum api_resp_type {
  API_RESP_TYPE_LIST_U32,
  API_RESP_TYPE_LIST_OBJ,
  API_RESP_TYPE_ACK
};

enum api_obj_type { API_OBJ_TYPE_EVENT };

typedef struct api_obj_s {
  uint32_t id;
  char *data;
  size_t data_size;
} api_obj_t;

typedef struct api_response_type_list_obj_s {
  enum api_obj_type type;
  api_obj_t *objects;
  uint32_t count;
} api_response_type_list_obj_t;

typedef struct api_response_type_list_u32_s {
  uint32_t *int32s;
  uint32_t count;
} api_response_type_list_u32_t;

typedef struct api_response_s {
  enum api_op_type op_type;
  enum api_resp_type resp_type;

  union {
    api_response_type_list_u32_t list_u32;
    api_response_type_list_obj_t list_obj;
  } payload;

  bool is_ok;
  const char *err_msg;
} api_response_t;

void free_api_response(api_response_t *r);

bool api_start_eng(void);
void api_stop_eng(void);

// The single entry point into the API/Engine layer for executing commands.
// Takes ownership of ast - caller must not free
api_response_t *api_exec(ast_node_t *ast, int64_t arrival_ts);

#endif // API_H