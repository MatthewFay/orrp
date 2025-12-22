#include "engine/api.h"
#include "core/data_constants.h"
#include "engine.h"
#include "query/ast.h"
#include "uthash.h"
#include <ctype.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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

static bool _validate_time_range(const int64_t *from, const int64_t *to) {
  // We use difftime to be safe across different C implementations
  // difftime(end, start) returns negative if end < start
  if (difftime(*to, *from) < 0) {
    return false;
  }

  return true;
}

static bool _validate_ast(ast_node_t *ast, custom_key **c_keys) {
  if (!ast)
    return false;
  bool seen_in = false;
  bool seen_id = false;
  bool seen_where = false;
  bool seen_entity = false;
  bool seen_take = false;
  bool seen_cursor = false;
  int64_t *from = NULL;
  int64_t *to = NULL;

  if (ast->type != AST_COMMAND_NODE || !ast->command.tags ||
      ast->command.tags->type != AST_TAG_NODE)
    return false;

  ast_command_type_t cmd_type = ast->command.type;
  custom_key *c_key = NULL;
  ast_node_t *tag = ast->command.tags;
  while (tag) {
    ast_tag_node_t t_node = tag->tag;
    if (!t_node.value)
      return false;
    if (t_node.key_type == AST_TAG_KEY_RESERVED) {
      switch (t_node.reserved_key) {
      case AST_KEY_IN:
        if (seen_in ||
            !_is_valid_container_name(t_node.value->literal.string_value))
          return false;
        seen_in = true;
        break;
      case AST_KEY_ID:
        if (seen_id)
          return false;
        seen_id = true;
        break;
      case AST_KEY_WHERE:
        if (seen_where || cmd_type != AST_CMD_QUERY)
          return false;
        seen_where = true;
        break;
      case AST_KEY_ENTITY:
        if (seen_entity)
          return false;
        seen_entity = true;
        if (t_node.value->literal.type == AST_LITERAL_STRING &&
            t_node.value->literal.string_value_len > MAX_ENTITY_STR_LEN) {
          return false;
        }
        break;
      case AST_KEY_TAKE:
        if (seen_take)
          return false;
        seen_take = true;
        break;
      case AST_KEY_CURSOR:
        if (seen_cursor)
          return false;
        seen_cursor = true;
        break;
      case AST_KEY_FROM:
        if (from)
          return false;
        *from = t_node.value->literal.number_value;
        break;
      case AST_KEY_TO:
        if (to)
          return false;
        *to = t_node.value->literal.number_value;

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

  if (cmd_type == AST_CMD_EVENT && !seen_entity) {
    return false;
  }
  if (cmd_type == AST_CMD_EVENT && (seen_where || from || to))
    return false;

  if (cmd_type == AST_CMD_QUERY && !seen_where) {
    return false;
  }
  if (cmd_type == AST_CMD_QUERY && seen_entity)
    return false;

  if (from && to && !_validate_time_range(from, to)) {
    return false;
  }
  return true;
}

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

// The single entry point into the API/Engine layer.
// Validates the AST before passing it into the core engine for execution.
// `api_exec` takes ownership of `ast`.
api_response_t *api_exec(ast_node_t *ast, int64_t arrival_ts) {
  api_response_t *r = _create_api_resp(API_INVALID);
  if (!r) {
    ast_free(ast);
    return NULL;
  }

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
    r->err_msg = "Error: Invalid command";
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