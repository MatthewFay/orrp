#include "api.h"
#include "engine.h"
#include "query/ast.h"
#include <stdlib.h>
#include <string.h>

const int MAX_NS_LEN = 128;
const int MAX_KEY_LEN = 128;
const int MAX_ID_LEN = 128;

void free_api_response(api_response_t *r) {
  free(r->data);
  free(r);
}

static api_response_t *_create_api_resp(enum api_op_type op_type) {
  api_response_t *r = malloc(sizeof(api_response_t));
  r->is_ok = false; // Default to false
  r->data = NULL;
  r->err_msg = NULL;
  r->op_type = op_type;
  return r;
}

static bool _validate_ns_key_id(api_response_t *r, char *ns, char *key,
                                char *id) {
  if (!ns) {
    r->err_msg = "Missing namespace";
    return false;
  }
  if (!key) {
    r->err_msg = "Missing key";
    return false;
  }
  if (!id) {
    r->err_msg = "Missing id";
    return false;
  }
  if (strlen(ns) > MAX_NS_LEN) {
    r->err_msg = "Namespace too long";
    return false;
  }
  if (strlen(key) > MAX_KEY_LEN) {
    r->err_msg = "Key too long";
    return false;
  }
  if (strlen(id) > MAX_ID_LEN) {
    r->err_msg = "Id too long";
    return false;
  }
  return true;
}

static api_response_t *_api_add(ast_node_t *ast, eng_db_t *db,
                                api_response_t *r) {
  r->op_type = API_ADD;

  char *ns = ast_get_command_arg(ast->node.cmd, 0);
  char *key = ast_get_command_arg(ast->node.cmd, 1);
  char *id = ast_get_command_arg(ast->node.cmd, 2);

  bool valid = _validate_ns_key_id(r, ns, key, id);
  if (!valid) {
    r->err_msg = "Invalid arguments to ADD.";
    return r;
  }
  eng_add(r, db, ns, key, id);
  return r;
}

// The single entry point into the API/Engine layer.
api_response_t *api_exec(ast_node_t *ast, eng_db_t *db) {
  api_response_t *r = _create_api_resp(API_INVALID);

  if (!ast || ast->type != COMMAND_NODE) {
    r->err_msg = "Invalid AST provided.";
    return r;
  }

  // Dispatch based on the command type in the AST root.
  switch (ast->node.cmd->cmd_type) {
  case ADD:
    return _api_add(ast, db, r);

  case QUERY:
    return r;

    // Add new commands here in the future.
    // case COMMAND_NEW:
    //     return handle_new_command(ast, db);

  default:
    r->err_msg = "Unknown command type.";
    return r;
  }
}
