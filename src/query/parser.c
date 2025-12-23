#include "query/parser.h"
#include "core/data_constants.h"
#include "core/queue.h"
#include "core/stack.h"
#include "query/ast.h"
#include "query/tokenizer.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Parse the next tag, if it exists
static ast_node_t *_parse_tag(Queue *tokens, parse_result_t *r);

static bool _parse_tags(Queue *tokens, ast_node_t *cmd_node,
                        parse_result_t *r) {
  int num_cus_tags = 0;
  while (!q_empty(tokens)) {
    ast_node_t *tag = _parse_tag(tokens, r);
    if (!tag) {
      r->error_message = "Invalid tag";
      return false;
    }

    ast_append_node(&cmd_node->command.tags, tag);
    if (tag->tag.key_type == AST_TAG_KEY_CUSTOM) {
      num_cus_tags++;
    }
    if (num_cus_tags > MAX_CUSTOM_TAGS) {
      r->error_message = "Too many custom tags!";
      return false;
    }
  }
  return true;
}

static void _parse_event(Queue *tokens, parse_result_t *r) {
  ast_command_type_t cmd_type = AST_CMD_EVENT;
  ast_node_t *cmd_node = ast_create_command_node(cmd_type, NULL);
  if (!cmd_node) {
    r->error_message = "Failed to allocate command node";
    return;
  }
  if (!_parse_tags(tokens, cmd_node, r)) {
    ast_free(cmd_node);
    return;
  }

  r->type = PARSER_OP_TYPE_WRITE;
  r->ast = cmd_node;
}

// Helper function to build a logical node from the stacks.
static bool _apply_operator(c_stack_t *value_stack, c_stack_t *op_stack) {
  token_t *op_token = stack_pop(op_stack);
  if (!op_token)
    return false;

  ast_node_t *new_node = NULL;

  if (op_token->type == TOKEN_OP_NOT) {
    // --- Unary Operator Logic ---
    ast_node_t *operand = stack_pop(value_stack);
    if (!operand) {
      tok_free(op_token);
      return false;
    }
    new_node = ast_create_not_node(operand);
  } else {
    // Binary operator logic (AND, OR, comparisons)
    ast_node_t *right_node = stack_pop(value_stack);
    if (!right_node) {
      tok_free(op_token);
      return false;
    }
    ast_node_t *left_node = stack_pop(value_stack);
    if (!left_node) {
      tok_free(op_token);
      ast_free(right_node);
      return false;
    }

    // Handle logical operators
    if (op_token->type == TOKEN_OP_AND || op_token->type == TOKEN_OP_OR) {
      new_node = ast_create_logical_node(op_token->type == TOKEN_OP_AND
                                             ? AST_LOGIC_NODE_AND
                                             : AST_LOGIC_NODE_OR,
                                         left_node, right_node);
    }
    // Handle comparison operators
    else if (op_token->type == TOKEN_OP_EQ || op_token->type == TOKEN_OP_NEQ ||
             op_token->type == TOKEN_OP_GT || op_token->type == TOKEN_OP_GTE ||
             op_token->type == TOKEN_OP_LT || op_token->type == TOKEN_OP_LTE) {

      ast_comparison_op_t comp_op;
      switch (op_token->type) {
      case TOKEN_OP_EQ:
        comp_op = AST_OP_EQ;
        break;
      case TOKEN_OP_NEQ:
        comp_op = AST_OP_NEQ;
        break;
      case TOKEN_OP_GT:
        comp_op = AST_OP_GT;
        break;
      case TOKEN_OP_GTE:
        comp_op = AST_OP_GTE;
        break;
      case TOKEN_OP_LT:
        comp_op = AST_OP_LT;
        break;
      case TOKEN_OP_LTE:
        comp_op = AST_OP_LTE;
        break;
      default:
        comp_op = AST_OP_EQ;
        break; // fallback
      }

      new_node = ast_create_comparison_node(comp_op, left_node, right_node);
    }

    if (!new_node) {
      ast_free(right_node);
      ast_free(left_node);
      tok_free(op_token);

      return false;
    }
  }

  tok_free(op_token);
  if (!new_node)
    return false;

  if (!stack_push(value_stack, new_node)) {
    ast_free(new_node);
    return false;
  }

  return true;
}

static int _get_precedence(token_type type) {
  switch (type) {
  case TOKEN_OP_NOT:
    return 4; // Highest precedence
  case TOKEN_OP_EQ:
  case TOKEN_OP_NEQ:
  case TOKEN_OP_GT:
  case TOKEN_OP_GTE:
  case TOKEN_OP_LT:
  case TOKEN_OP_LTE:
    return 3;
  case TOKEN_OP_AND:
    return 2;
  case TOKEN_OP_OR:
    return 1;
  default:
    return 0; // For parentheses and other tokens
  }
}

typedef enum { RIGHT, LEFT } associativity;
static associativity get_associativity(token_type type) {
  if (type == TOKEN_OP_NOT)
    return RIGHT;
  return LEFT;
}

static void *cleanup_stacks_and_return_null(c_stack_t *value_stack,
                                            c_stack_t *op_stack) {
  while (!stack_is_empty(value_stack))
    ast_free(stack_pop(value_stack));
  while (!stack_is_empty(op_stack))
    tok_free(stack_pop(op_stack));
  stack_free(value_stack);
  stack_free(op_stack);
  return NULL;
}

// Expression parser:  Shunting-Yard
static ast_node_t *_parse_exp(Queue *tokens, parse_result_t *r) {
  c_stack_t *value_stack = stack_create();
  c_stack_t *op_stack = stack_create();
  if (!value_stack || !op_stack) {
    stack_free(value_stack);
    stack_free(op_stack);
    r->error_message = "Failed to create stacks";
    return NULL;
  }

  token_t *lparen = q_dequeue(tokens);
  if (!lparen || lparen->type != TOKEN_SYM_LPAREN) {
    tok_free(lparen);
    r->error_message = "Expression must start with '('";
    return NULL;
  }
  if (!stack_push(op_stack, lparen)) {
    tok_free(lparen);
    return cleanup_stacks_and_return_null(value_stack, op_stack);
  }

  int paren_depth = 1;

  // A state flag to track whether we expect an operand (like an identifier or
  // '(') or an operator (like 'AND' or ')').
  bool expect_operand = true;

  while (!q_empty(tokens) && paren_depth > 0) {
    token_t *token = q_peek(tokens);

    if (expect_operand) {
      if (token->type == TOKEN_IDENTIFER ||
          token->type == TOKEN_LITERAL_NUMBER) {
        token_t *operand_tok = q_dequeue(tokens);
        ast_node_t *node;
        token_t *next_tok = q_peek(tokens);

        if (operand_tok->type == TOKEN_IDENTIFER && next_tok &&
            next_tok->type == TOKEN_SYM_COLON) {
          // consume colon
          tok_free(q_dequeue(tokens));

          token_t *val_tok = q_dequeue(tokens);
          if (!val_tok) {
            tok_free(operand_tok);
            r->error_message = "Unexpected end of query after tag key";
            return cleanup_stacks_and_return_null(value_stack, op_stack);
          }

          char *final_val_str = NULL;
          size_t final_val_str_len = 0;
          int64_t final_val_int64 = 0;
          if (val_tok->type == TOKEN_IDENTIFER ||
              val_tok->type == TOKEN_LITERAL_STRING) {
            final_val_str = val_tok->text_value;
            final_val_str_len = val_tok->text_value_len;
          } else if (val_tok->type == TOKEN_LITERAL_NUMBER) {
            final_val_int64 = val_tok->number_value;
          } else {
            tok_free(operand_tok);
            tok_free(val_tok);
            r->error_message =
                "Invalid tag value. Expected identifier, string, or number.";
            return cleanup_stacks_and_return_null(value_stack, op_stack);
          }

          ast_node_t *tag_val_node =
              final_val_str != NULL
                  ? ast_create_string_literal_node(final_val_str,
                                                   final_val_str_len)
                  : ast_create_number_literal_node(final_val_int64);
          node =
              ast_create_custom_tag_node(operand_tok->text_value, tag_val_node);
          tok_free(val_tok);
        } else if (operand_tok->type == TOKEN_IDENTIFER) {
          node = ast_create_string_literal_node(operand_tok->text_value,
                                                operand_tok->text_value_len);
        } else {
          node = ast_create_number_literal_node(operand_tok->number_value);
        }

        tok_free(operand_tok);

        if (!node || !stack_push(value_stack, node)) {
          ast_free(node);
          return cleanup_stacks_and_return_null(value_stack, op_stack);
        }
        expect_operand = false; // After an operand, we expect an operator.
      } else if (token->type == TOKEN_OP_NOT ||
                 token->type == TOKEN_SYM_LPAREN) {
        paren_depth++;
        token_t *op_to_push = q_dequeue(tokens);
        if (!stack_push(op_stack, op_to_push)) {
          tok_free(op_to_push);
          return cleanup_stacks_and_return_null(value_stack, op_stack);
        }
        expect_operand =
            true; // After a prefix op or '(', we expect an operand.
      } else {
        r->error_message = "Syntax error: Unexpected token, expected operand.";
        return cleanup_stacks_and_return_null(value_stack, op_stack);
      }
    } else { // We expect a binary operator or a right parenthesis
      if (token->type == TOKEN_OP_AND || token->type == TOKEN_OP_OR ||
          token->type == TOKEN_OP_EQ || token->type == TOKEN_OP_NEQ ||
          token->type == TOKEN_OP_GT || token->type == TOKEN_OP_GTE ||
          token->type == TOKEN_OP_LT || token->type == TOKEN_OP_LTE) {
        token_t *op1 = token;
        while (!stack_is_empty(op_stack)) {
          token_t *op2 = stack_peek(op_stack);
          if (op2->type == TOKEN_SYM_LPAREN)
            break;

          if ((get_associativity(op1->type) == LEFT &&
               _get_precedence(op2->type) >= _get_precedence(op1->type)) ||
              (get_associativity(op1->type) == RIGHT &&
               _get_precedence(op2->type) > _get_precedence(op1->type))) {
            if (!_apply_operator(value_stack, op_stack)) {
              return cleanup_stacks_and_return_null(value_stack, op_stack);
            }
          } else {
            break;
          }
        }
        token_t *op_to_push = q_dequeue(tokens);
        if (!stack_push(op_stack, op_to_push)) {
          tok_free(op_to_push);
          return cleanup_stacks_and_return_null(value_stack, op_stack);
        }
        expect_operand = true; // After a binary op, we expect an operand.
      } else if (token->type == TOKEN_SYM_RPAREN) {
        paren_depth--;
        bool found_lparen = false;
        while (!stack_is_empty(op_stack)) {
          if (((token_t *)stack_peek(op_stack))->type == TOKEN_SYM_LPAREN) {
            tok_free(stack_pop(op_stack)); // Pop and discard the '('
            found_lparen = true;
            break;
          }
          if (!_apply_operator(value_stack, op_stack)) {
            return cleanup_stacks_and_return_null(value_stack, op_stack);
          }
        }
        if (!found_lparen) {
          r->error_message = "Mismatched parentheses";
          return cleanup_stacks_and_return_null(value_stack, op_stack);
        }
        tok_free(q_dequeue(tokens)); // Consume and discard the ')'
        expect_operand = false; // After a ')', we expect a binary operator.
      } else {
        r->error_message = "Syntax error: Unexpected token, expected operator.";
        return cleanup_stacks_and_return_null(value_stack, op_stack);
      }
    }
  }

  // --- Final Unwinding ---
  while (!stack_is_empty(op_stack)) {
    if (((token_t *)stack_peek(op_stack))->type == TOKEN_SYM_LPAREN) {
      r->error_message = "Mismatched parentheses";
      return cleanup_stacks_and_return_null(value_stack, op_stack);
    }
    if (!_apply_operator(value_stack, op_stack)) {
      return cleanup_stacks_and_return_null(value_stack, op_stack);
    }
  }

  ast_node_t *exp_tree = stack_pop(value_stack);
  if (!stack_is_empty(value_stack)) {
    ast_free(exp_tree);
    r->error_message = "Invalid expression structure";
    return cleanup_stacks_and_return_null(value_stack, op_stack);
  }

  stack_free(value_stack);
  stack_free(op_stack);

  return exp_tree;
}

static void _parse_query(Queue *tokens, parse_result_t *r) {
  ast_command_type_t cmd_type = AST_CMD_QUERY;
  ast_node_t *cmd_node = ast_create_command_node(cmd_type, NULL);
  if (!cmd_node) {
    r->error_message = "Failed to allocate command node";
    return;
  }
  if (!_parse_tags(tokens, cmd_node, r)) {
    ast_free(cmd_node);
    return;
  }

  r->type = PARSER_OP_TYPE_READ;
  r->ast = cmd_node;
}

static parse_result_t *_create_result(void) {
  parse_result_t *r = malloc(sizeof(parse_result_t));
  r->ast = NULL;
  r->type = PARSER_OP_TYPE_ERROR;
  r->error_message = NULL;
  return r;
}

static bool _is_token_a_command(const token_t *token) {
  if (!token) {
    return false;
  }

  switch (token->type) {
  case TOKEN_CMD_QUERY:
  case TOKEN_CMD_EVENT:
    return true;
  default:
    return false;
  }
}

parse_result_t *parse(Queue *tokens) {
  parse_result_t *r = _create_result();
  if (!tokens) {
    r->error_message = "Invalid input: token queue is NULL.";
    return r;
  }
  if (q_empty(tokens)) {
    r->error_message = "Invalid input: token queue is empty.";
    return r;
  }
  token_t *cmd_token = q_dequeue(tokens);
  if (!cmd_token || !_is_token_a_command(cmd_token)) {
    tok_clear_all(tokens);
    r->error_message = "Invalid command!";
    return r;
  }

  if (cmd_token->type == TOKEN_CMD_EVENT) {
    _parse_event(tokens, r);
  } else if (cmd_token->type == TOKEN_CMD_QUERY) {
    _parse_query(tokens, r);
  } else {
    tok_free(cmd_token);
    tok_clear_all(tokens);
    r->error_message = "Unrecognized command!";
    return r;
  }
  tok_free(cmd_token);
  tok_clear_all(tokens);
  return r;
}

void parse_free_result(parse_result_t *r) {
  if (!r)
    return;
  free(r);
}

static bool _is_token_kw(token_t *t) {
  if (!t)
    return NULL;
  switch (t->type) {
  case TOKEN_KW_ID:
  case TOKEN_KW_IN:
  case TOKEN_KW_CURSOR:
  case TOKEN_KW_ENTITY:
  case TOKEN_KW_WHERE:
  case TOKEN_KW_TAKE:
  case TOKEN_KW_BY:
  case TOKEN_KW_COUNT:
  case TOKEN_KW_HAVING:
  case TOKEN_KW_FROM:
  case TOKEN_KW_TO:
    return true;
  default:
    return false;
  }
}

static bool _is_literal_or_identifier(token_t *tok) {
  return tok->type == TOKEN_IDENTIFER || tok->type == TOKEN_LITERAL_STRING ||
         tok->type == TOKEN_LITERAL_NUMBER;
}

static ast_node_t *_parse_tag(Queue *tokens, parse_result_t *r) {
  ast_node_t *tag = NULL;
  ast_node_t *tag_val = NULL;

  // need at least 3 tokens for a tag (key:value)
  if (q_size(tokens) < 3)
    return NULL;
  token_t *key_token = q_dequeue(tokens);
  if (!key_token)
    return NULL;
  token_type key_token_type = key_token->type;

  if (key_token_type == TOKEN_IDENTIFER ||
      key_token_type == TOKEN_LITERAL_STRING) {
    tag = ast_create_custom_tag_node(key_token->text_value, NULL);
  } else if (_is_token_kw(key_token)) {
    ast_reserved_key_t kt;
    switch (key_token_type) {
    case TOKEN_KW_IN:
      kt = AST_KEY_IN;
      break;
    // case TOKEN_KW_ID:
    //   kt = AST_KEY_ID;
    //   break;
    case TOKEN_KW_ENTITY:
      kt = AST_KEY_ENTITY;
      break;
    case TOKEN_KW_WHERE:
      kt = AST_KEY_WHERE;
      break;
    // case TOKEN_KW_TAKE:
    //   kt = AST_KEY_TAKE;
    //   break;
    // case TOKEN_KW_CURSOR:
    //   kt = AST_KEY_CURSOR;
    //   break;
    case TOKEN_KW_FROM:
      kt = AST_KEY_FROM;
      break;
    case TOKEN_KW_TO:
      kt = AST_KEY_TO;
      break;
    default:
      free(key_token);
      return NULL;
    }
    tag = ast_create_tag_node(kt, NULL);
  } else {
    free(key_token);
    return NULL;
  }

  free(key_token);

  if (!tag) {
    return NULL;
  }

  token_t *sep = q_dequeue(tokens);
  if (!sep || sep->type != TOKEN_SYM_COLON) {
    free(sep);
    ast_free(tag);
    return NULL;
  }
  free(sep);

  token_t *first_val_token = q_peek(tokens);
  if (!first_val_token) {
    ast_free(tag);
    return NULL;
  }
  if (tag->tag.key_type == AST_TAG_KEY_RESERVED) {
    switch (tag->tag.reserved_key) {
    case AST_KEY_WHERE:
      // where: must be followed by a parenthesized expression
      if (first_val_token->type != TOKEN_SYM_LPAREN) {
        ast_free(tag);
        return NULL;
      }
      break;
    case AST_KEY_FROM:
    case AST_KEY_TO:
      if (first_val_token->type != TOKEN_LITERAL_NUMBER) {
        ast_free(tag);
        return NULL;
      }
      break;
    default:
      // All other reserved keys must be followed by a literal or identifier
      if (!_is_literal_or_identifier(first_val_token)) {
        ast_free(tag);
        return NULL;
      }
      break;
    }
  } else {
    // Custom keys: must be followed by a literal or identifier
    if (!_is_literal_or_identifier(first_val_token)) {
      ast_free(tag);
      return NULL;
    }
  }

  if (first_val_token->type == TOKEN_IDENTIFER ||
      first_val_token->type == TOKEN_LITERAL_STRING) {
    first_val_token = q_dequeue(tokens);
    size_t valid_len = tag->tag.reserved_key == AST_KEY_ENTITY
                           ? MAX_ENTITY_STR_LEN
                           : MAX_TEXT_VAL_LEN;
    if (first_val_token->text_value_len > valid_len) {
      free(first_val_token);
      return NULL;
    }
    tag_val = ast_create_string_literal_node(first_val_token->text_value,
                                             first_val_token->text_value_len);
    free(first_val_token);
    if (!tag_val) {
      ast_free(tag);
      return NULL;
    }
    tag->tag.value = tag_val;
  } else if (first_val_token->type == TOKEN_LITERAL_NUMBER) {
    first_val_token = q_dequeue(tokens);

    tag_val = ast_create_number_literal_node(first_val_token->number_value);
    free(first_val_token);

    if (!tag_val) {
      ast_free(tag);
      return NULL;
    }
    tag->tag.value = tag_val;

  } else {
    ast_node_t *exp_tree = _parse_exp(tokens, r);
    if (!exp_tree) {
      ast_free(tag);
      return NULL;
    }
    tag->tag.value = exp_tree;
  }

  return tag;
}
