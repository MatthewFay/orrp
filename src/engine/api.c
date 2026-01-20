#include "engine/api.h"
#include "engine.h"
#include "engine/validator/validator.h"
#include "query/ast.h"
#include <ctype.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

void free_api_response(api_response_t *r) {
  if (!r)
    return;
  switch (r->resp_type) {
  case API_RESP_TYPE_LIST_U32:
    free(r->payload.list_u32.int32s);
    break;
  case API_RESP_TYPE_LIST_OBJ:
    for (unsigned int i = 0; i < r->payload.list_obj.count; i++) {
      api_obj_t *o = &r->payload.list_obj.objects[i];
      if (o) {
        free(o->data);
      }
    }
    free(r->payload.list_obj.objects);
    break;
  default:
    break;
  }

  free(r);
}

static api_response_t *_create_api_resp(enum api_op_type op_type) {
  // `is_ok` will default to false w/ calloc
  api_response_t *r = calloc(1, sizeof(api_response_t));
  if (!r) {
    return NULL;
  }
  r->op_type = op_type;
  return r;
}

static api_response_t *_api_event(ast_node_t *ast, api_response_t *r,
                                  int64_t arrival_ts) {
  r->op_type = API_EVENT;

  eng_event(r, ast, arrival_ts);
  return r;
}

static api_response_t *_api_query(ast_node_t *ast, api_response_t *r) {
  r->op_type = API_QUERY;

  eng_query(r, ast);
  return r;
}

static api_response_t *_api_index(ast_node_t *ast, api_response_t *r) {
  r->op_type = API_INDEX;

  eng_index(r, ast);
  return r;
}

// The single entry point into the API/Engine layer.
// Validates the AST before passing it into the core engine for execution.
// `api_exec` takes ownership of `ast`.
api_response_t *api_exec(ast_node_t *ast, int64_t arrival_ts) {
  api_response_t *r = _create_api_resp(API_INVALID);
  if (!r) {
    ast_free(ast);
    return NULL;
  }

  validator_result_t v_r;
  validator_analyze(ast, &v_r);

  if (!v_r.is_valid) {
    r->err_msg = v_r.err_msg;
    ast_free(ast);
    return r;
  }

  switch (ast->command.type) {
  case AST_CMD_EVENT:
    _api_event(ast, r, arrival_ts);
    break;

  case AST_CMD_QUERY:
    _api_query(ast, r);

    break;

  case AST_CMD_INDEX:
    _api_index(ast, r);

    break;

  default:
    r->err_msg = "Unknown command type!";
    ;
    ast_free(ast);

    break;
  }

  return r;
}

bool api_start_eng(void) { return eng_init(); }

void api_stop_eng(void) { eng_shutdown(); }