#include "validator.h"
#include "core/data_constants.h"
#include "query/ast.h"
#include <ctype.h>
#include <string.h>

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

static bool _validate_comparison_op(ast_comparison_node_t *comp_node,
                                    validator_result_t *vr) {
  if (comp_node->left->type != AST_LITERAL_NODE ||
      comp_node->right->type != AST_LITERAL_NODE) {
    vr->err_msg = "Invalid comparison";
    return false;
  }
  if ((comp_node->left->literal.type == AST_LITERAL_STRING &&
       comp_node->right->literal.type == AST_LITERAL_STRING) ||
      (comp_node->left->literal.type == AST_LITERAL_NUMBER &&
       comp_node->right->literal.type == AST_LITERAL_NUMBER)) {
    vr->err_msg = "Invalid comparison types";

    return false;
  }

  return true;
}

static bool _is_valid_where_exp(ast_node_t *node, validator_result_t *vr) {
  bool r = false;
  switch (node->type) {
  case AST_TAG_NODE:
    return true;
  case AST_LITERAL_NODE:
    // Literals must be apart of conditions
    vr->err_msg = "Unexpected literal";
    return false;
    break;
  case AST_LOGICAL_NODE:
    r = _is_valid_where_exp(node->logical.left_operand, vr);
    if (!r)
      return false;
    return _is_valid_where_exp(node->logical.right_operand, vr);
  case AST_COMPARISON_NODE:
    return _validate_comparison_op(&node->comparison, vr);
  case AST_NOT_NODE:
    return _is_valid_where_exp(node->not_op.operand, vr);
  default:
    vr->err_msg = "Unknown or unsupported system tag";
    return false;
  }
}

static void _validate_ast(ast_node_t *ast, custom_tag_key_t **c_keys,
                          validator_result_t *r) {
  bool seen_in = false;
  bool seen_where = false;
  bool seen_entity = false;
  bool seen_key = false;
  bool seen_take = false;

  ast_command_type_t cmd_type = ast->command.type;
  custom_tag_key_t *c_key = NULL;
  ast_node_t *tag = ast->command.tags;
  while (tag) {
    ast_tag_node_t t_node = tag->tag;
    if (t_node.key_type == AST_TAG_KEY_RESERVED) {
      switch (t_node.reserved_key) {
      case AST_KW_IN:
        if (cmd_type == AST_CMD_INDEX) {
          r->err_msg = "Indexing specific containers is not supported yet. "
                       "Indexes apply globally to new data containers.";
          return;
        }
        if (seen_in) {
          r->err_msg = "Duplicate `in` tags not yet supported";
          return;
        }
        if (!_is_valid_container_name(t_node.value->literal.string_value)) {
          r->err_msg = "Invalid container name";
          return;
        }
        seen_in = true;
        break;
      case AST_KW_ID:
        r->err_msg = "`id` tag not yet supported";
        return; // not yet implemented
                // if (seen_id || cmd_type != AST_CMD_EVENT)
                //   return false;
                // seen_id = true;
                // break;
      case AST_KW_WHERE:
        if (cmd_type != AST_CMD_QUERY) {
          r->err_msg = "`where` tag only supported for queries";
          return;
        }
        if (seen_where) {
          r->err_msg = "Duplicate `where` tag";
          return;
        }
        seen_where = true;
        if (!_is_valid_where_exp(t_node.value, r)) {
          return;
        }
        break;
      case AST_KW_ENTITY:
        if (cmd_type != AST_CMD_EVENT) {
          r->err_msg = "Unexpected `entity` tag";
          return;
        }
        if (seen_entity) {
          r->err_msg = "Duplicate `entity` tag";
          return;
        }
        if (t_node.value->literal.type == AST_LITERAL_STRING &&
            t_node.value->literal.string_value_len > MAX_ENTITY_STR_LEN) {
          r->err_msg = "`entity` value too long";
          return;
        }
        seen_entity = true;

        break;
      case AST_KW_TAKE:
        if (seen_take) {
          r->err_msg = "Duplicate `take` tag";
          return;
        }
        if (cmd_type != AST_CMD_QUERY) {
          r->err_msg = "Unexpected `take` tag";
          return;
        }
        if (t_node.value->literal.type != AST_LITERAL_NUMBER) {
          r->err_msg = "Value of `take` tag must be numeric";
          return;
        }
        if (t_node.value->literal.number_value <= 0) {
          r->err_msg = "Value of `take` tag must be positive";
          return;
        }
        seen_take = true;
        break;
      case AST_KW_CURSOR:
        r->err_msg = "`cursor` not yet supported";
        return; // not yet implemented
        // if (seen_cursor || cmd_type != AST_CMD_QUERY)
        //   return false;
        // seen_cursor = true;
        // break;
      case AST_KW_KEY:
        if (cmd_type != AST_CMD_INDEX) {
          r->err_msg = "Unexpected `key` tag";
          return;
        }
        if (seen_key) {
          r->err_msg = "Duplicate `key` tag";
          return;
        }
        seen_key = true;
        break;
      default:
        return;
      }
    } else {
      if (cmd_type != AST_CMD_EVENT) {
        r->err_msg = "Unexpected tag";
        return;
      }
      HASH_FIND_STR(*c_keys, t_node.custom_key, c_key);
      if (c_key) {
        r->err_msg = "Duplicate tag";
        return;
      }
      c_key = malloc(sizeof(custom_tag_key_t));
      if (!c_key) {
        r->err_msg = "Memory allocation failed during validation";
        return;
      }
      c_key->key = t_node.custom_key;
      HASH_ADD_KEYPTR(hh, *c_keys, t_node.custom_key, strlen(t_node.custom_key),
                      c_key);
    }
    tag = tag->next;
  }

  if (!seen_in && cmd_type != AST_CMD_INDEX) {
    r->err_msg = "`in` tag is required";
    return;
  }

  if (cmd_type == AST_CMD_EVENT && !seen_entity) {
    r->err_msg = "`entity` tag is required";
    return;
  }

  if (cmd_type == AST_CMD_QUERY && !seen_where) {
    r->err_msg = "`where` tag is required";
    return;
  }

  if (cmd_type == AST_CMD_INDEX && !seen_key) {
    r->err_msg = "`key` tag is required";
    return;
  }

  r->is_valid = true;
}

void validator_analyze(ast_node_t *root, validator_result_t *result_out) {
  if (!root || !result_out)
    return;
  // initialize `success` to false
  memset(result_out, 0, sizeof(validator_result_t));

  custom_tag_key_t *c_keys = NULL;
  _validate_ast(root, &c_keys, result_out);
  if (c_keys) {
    custom_tag_key_t *c_key, *tmp;
    HASH_ITER(hh, c_keys, c_key, tmp) {
      HASH_DEL(c_keys, c_key);
      free(c_key);
    }
  }
}