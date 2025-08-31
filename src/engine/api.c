#include "engine/api.h"
#include "engine.h"
#include "query/ast.h"
#include "uthash.h"
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

const int MAX_NS_LEN = 128;
const int MAX_KEY_LEN = 128;
const int MAX_ID_LEN = 128;

typedef struct {
  char *key;
  UT_hash_handle hh;
} custom_key;

static bool _is_valid_filename(const char *filename) {
  if (filename == NULL || filename[0] == '\0') {
    return false;
  }

  size_t len = strlen(filename);
  if (len > 64 || filename[0] == '.' || filename[len - 1] == '.') {
    return false;
  }

  // Only allow alphanumeric, underscore, hyphen
  for (size_t i = 0; i < len; i++) {
    unsigned char c = (unsigned char)filename[i];
    if (!isalnum(c) && c != '_' && c != '-') {
      return false;
    }
  }

  return true;
}

static bool _is_valid_container_name(const char *name) {
  // Container names are used as part of file name
  return _is_valid_filename(name);
}

static bool _validate_ast(ast_node_t *ast, custom_key **c_keys) {
  if (!ast)
    return false;
  bool seen_in = false;
  bool seen_id = false;
  bool seen_exp = false;
  bool seen_entity = false;
  bool seen_take = false;
  bool seen_cursor = false;
  // in future- allow multiple tag counters
  bool seen_tag_counter = false;

  if (ast->type != COMMAND_NODE || !ast->command.tags ||
      ast->command.tags->type != TAG_NODE)
    return false;

  ast_command_type_t cmd_type = ast->command.type;
  custom_key *c_key = NULL;
  ast_node_t *tag = ast->command.tags;
  while (tag) {
    ast_tag_node_t t_node = tag->tag;
    if (!t_node.value)
      return false;
    if (t_node.is_counter) {
      if (seen_tag_counter)
        return false;
      seen_tag_counter = true;
    }
    if (t_node.key_type == TAG_KEY_RESERVED) {
      switch (t_node.reserved_key) {
      case KEY_IN:
        if (seen_in ||
            !_is_valid_container_name(t_node.value->literal.string_value))
          return false;
        seen_in = true;
        break;
      case KEY_ID:
        if (seen_id)
          return false;
        seen_id = true;
        break;
      case KEY_EXP:
        if (seen_exp || cmd_type != CMD_QUERY)
          return false;
        seen_exp = true;
        break;
      case KEY_ENTITY:
        if (seen_entity)
          return false;
        seen_entity = true;
        break;
      case KEY_TAKE:
        if (seen_take)
          return false;
        seen_take = true;
        break;
      case KEY_CURSOR:
        if (seen_cursor)
          return false;
        seen_cursor = true;
        break;
      default:
        return false;
      }
    } else {
      HASH_FIND_STR(*c_keys, t_node.custom_key, c_key);
      if (c_key) {
        return false;
      }
      c_key = malloc(sizeof(custom_key));
      if (!c_key)
        return false;
      c_key->key = t_node.custom_key;
      HASH_ADD_KEYPTR(hh, *c_keys, t_node.custom_key, strlen(t_node.custom_key),
                      c_key);
    }
    tag = tag->next;
  }
  if (!seen_in) {
    return false;
  }
  if (cmd_type == CMD_EVENT && !seen_entity) {
    return false;
  }
  if (cmd_type == CMD_EVENT && seen_exp)
    return false;
  if (cmd_type == CMD_QUERY && !seen_exp) {
    return false;
  }
  if (cmd_type == CMD_QUERY && seen_entity)
    return false;
  return true;
}

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

static api_response_t *_api_event(ast_node_t *ast, eng_context_t *ctx,
                                  api_response_t *r) {
  r->op_type = API_EVENT;

  eng_event(r, ctx, ast);
  return r;
}

// The single entry point into the API/Engine layer.
// Validates the AST before passing it into the core engine for execution.
api_response_t *api_exec(ast_node_t *ast, eng_context_t *ctx) {
  api_response_t *r = _create_api_resp(API_INVALID);

  custom_key *c_keys = NULL;
  bool v_r = _validate_ast(ast, &c_keys);
  if (c_keys) {
    custom_key *c_key, *tmp;
    HASH_ITER(hh, c_keys, c_key, tmp) {
      HASH_DEL(c_keys, c_key);
      free(c_key);
    }
  }
  if (!v_r) {
    r->err_msg = "Invalid AST";
    return r;
  }

  // Dispatch based on the command type in the AST root.
  switch (ast->type) {
  case CMD_EVENT:
    _api_event(ast, ctx, r);
    break;

  case CMD_QUERY:;
    break;

  default:
    r->err_msg = "Unknown command type!";
    ;
    break;
  }

  return r;
}

eng_context_t *api_start_eng(void) { return eng_init(); }

void api_stop_eng(eng_context_t *ctx) { eng_shutdown(ctx); }