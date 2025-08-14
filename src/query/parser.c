#include "query/parser.h"
#include "core/queue.h"
#include "core/stack.h"
#include "query/ast.h"
#include "query/tokenizer.h"
#include <stdlib.h>
#include <string.h>

static void _parse_add(Queue *tokens, parse_result_t *r) {
  if (q_size(tokens) != 3) {
    r->error_message = "Wrong number of arguments for ADD";
    return;
  }
  ast_node_t *cmd_node = ast_create_command_node(ADD, NULL, NULL);
  if (!cmd_node) {
    r->error_message = "Failed to allocate command node";
    return;
  }
  while (!q_empty(tokens)) {
    token_t *t = q_dequeue(tokens);
    if (!t || t->type != IDENTIFIER || t->text_value == NULL) {
      tok_free(t);
      ast_free(cmd_node);
      r->error_message = "Invalid argument type for ADD";
      return;
    }
    ast_list_append(&(cmd_node->node.cmd->args),
                    ast_create_identifier_node(t->text_value));
    tok_free(t);
  }
  r->type = OP_TYPE_WRITE;
  r->ast = cmd_node;
}

// Helper function to build a logical node from the stacks.
static bool _apply_operator(c_stack_t *value_stack, c_stack_t *op_stack) {
  token_t *op_token = stack_pop(op_stack);
  if (!op_token)
    return false;

  ast_node_t *new_node = NULL;

  if (op_token->type == NOT_OP) {
    // --- Unary Operator Logic ---
    ast_node_t *operand = stack_pop(value_stack);
    if (!operand) {
      tok_free(op_token);
      return false;
    }
    new_node = ast_create_not_node(operand);
  } else {
    // Binary operator logic (AND, OR)
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
    new_node = ast_create_logical_node(op_token->type == AND_OP ? AND : OR,
                                       left_node, right_node);

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
  case NOT_OP:
    return 3; // Highest precedence
  case AND_OP:
    return 2;
  case OR_OP:
    return 1;
  default:
    return 0; // For parentheses and other tokens
  }
}

typedef enum { RIGHT, LEFT } associativity;
static associativity get_associativity(token_type type) {
  if (type == NOT_OP)
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

  // A state flag to track whether we expect an operand (like an identifier or
  // '(') or an operator (like 'AND' or ')').
  bool expect_operand = true;

  while (!q_empty(tokens)) {
    token_t *token = q_peek(tokens);

    if (expect_operand) {
      if (token->type == IDENTIFIER) {
        token_t *id_to_push = q_dequeue(tokens);

        ast_node_t *node = ast_create_identifier_node(id_to_push->text_value);
        tok_free(id_to_push);
        if (!node || !stack_push(value_stack, node)) {
          ast_free(node);
          return cleanup_stacks_and_return_null(value_stack, op_stack);
        }
        expect_operand = false; // After an operand, we expect an operator.
      } else if (token->type == NOT_OP || token->type == LPAREN) {
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
      if (token->type == AND_OP || token->type == OR_OP) {
        token_t *op1 = token;
        while (!stack_is_empty(op_stack)) {
          token_t *op2 = stack_peek(op_stack);
          if (op2->type == LPAREN)
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
      } else if (token->type == RPAREN) {
        bool found_lparen = false;
        while (!stack_is_empty(op_stack)) {
          if (((token_t *)stack_peek(op_stack))->type == LPAREN) {
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
    if (((token_t *)stack_peek(op_stack))->type == LPAREN) {
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
  ast_node_t *cmd_node = ast_create_command_node(QUERY, NULL, NULL);
  if (!cmd_node) {
    r->error_message = "Failed to allocate command node";
    return;
  }

  // --- 1. Parse the MANDATORY first argument (e.g., 'analytics') ---
  token_t *arg_token = q_dequeue(tokens);
  if (!arg_token || arg_token->type != IDENTIFIER) {
    tok_free(arg_token);
    ast_free(cmd_node);
    r->error_message = "Invalid syntax: Expected an argument after QUERY";
    return;
  }

  ast_list_append(&(cmd_node->node.cmd->args),
                  ast_create_identifier_node(arg_token->text_value));
  tok_free(arg_token);

  // --- 2. Check what comes next ---
  if (q_empty(tokens)) {
    // This is a valid query with no expression: "QUERY analytics"
    cmd_node->node.cmd->exp = NULL;
    r->type = OP_TYPE_READ;
    r->ast = cmd_node;
    return;
  }

  // --- 3. If not the end, it MUST be an expression ---
  // Peek at the next token to see if it can legally start an expression.
  token_t *next_token = q_peek(tokens);
  if (next_token->type == IDENTIFIER || next_token->type == NOT_OP ||
      next_token->type == LPAREN) {

    // It looks like an expression, so parse it.
    ast_node_t *exp = _parse_exp(tokens, r);
    if (!exp) {
      // _parse_exp failed; error message is already set. Just clean up.
      ast_free(cmd_node);
      r->ast = NULL;
      return;
    }
    cmd_node->node.cmd->exp = exp;

    // After a valid expression, there should be nothing
    // left. This catches "QUERY analytics login_2024_02_02 arg3"
    if (!q_empty(tokens)) {
      r->error_message = "Invalid syntax: Unexpected token after expression";
      ast_free(cmd_node); // Free the entire tree we just built
      r->ast = NULL;
      return;
    }

  } else {
    // The token after the first argument cannot start an expression (e.g.,
    // "QUERY analytics AND ...")
    r->error_message =
        "Invalid syntax: Unexpected token after command argument";
    ast_free(cmd_node);
    r->ast = NULL;
    return;
  }

  r->type = OP_TYPE_READ;
  r->ast = cmd_node;
}

static parse_result_t *_create_result(void) {
  parse_result_t *r = malloc(sizeof(parse_result_t));
  r->ast = NULL;
  r->type = OP_TYPE_ERROR;
  r->error_message = NULL;
  return r;
}

static bool _is_token_a_command(const token_t *token) {
  if (!token) {
    return false;
  }

  switch (token->type) {
  case QUERY_CMD:
  case ADD_CMD:
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
    r->error_message = "Invalid command.";
    return r;
  }

  if (cmd_token->type == ADD_CMD) {
    _parse_add(tokens, r);
  } else if (cmd_token->type == QUERY_CMD) {
    _parse_query(tokens, r);
  } else {
    tok_free(cmd_token);
    tok_clear_all(tokens);
    r->error_message = "Unrecognized command";
    return r;
  }
  tok_free(cmd_token);
  tok_clear_all(tokens);
  return r;
}

void parse_free_result(parse_result_t *r) {
  if (!r)
    return;
  ast_free(r->ast);
  free(r);
}