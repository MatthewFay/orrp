#include "query/parser.h"
#include "core/queue.h"
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
    if (!t || t->type != TEXT || t->text_value == NULL) {
      tok_free(t);
      ast_free(cmd_node);
      r->error_message = "Invalid argument token for ADD";
      return;
    }
    ast_list_append(&(cmd_node->node.cmd->args),
                    ast_create_identifier_node(t->text_value));
    tok_free(t);
  }
  r->type = OP_TYPE_WRITE;
  r->ast = cmd_node;
}

static void _parse_exp(Queue *tokens, parse_result_t *r) {}

static parse_result_t *_create_result(void) {
  parse_result_t *r = malloc(sizeof(parse_result_t));
  r->ast = NULL;
  r->type = OP_TYPE_ERROR;
  r->error_message = NULL;
  return r;
}

parse_result_t *parse(Queue *tokens) {
  parse_result_t *r = _create_result();
  if (!tokens)
    return r;
  if (q_empty(tokens)) {
    tok_free_tokens(tokens);
    return r;
  }
  token_t *cmd_token = q_dequeue(tokens);
  if (!cmd_token || cmd_token->type != TEXT || cmd_token->text_value == NULL) {
    tok_free_tokens(tokens);
    r->error_message = "Invalid command";
    return r;
  }

  if (strcmp("add", cmd_token->text_value) == 0) {
    _parse_add(tokens, r);
  } else if (strcmp("query", cmd_token->text_value) == 0) {
    _parse_exp(tokens, r);
  } else {
    tok_free(cmd_token);
    tok_free_tokens(tokens);
    r->error_message = "Unrecognized command";
    return r;
  }
  tok_free(cmd_token);
  tok_free_tokens(tokens);
  return r;
}

void parse_free_result(parse_result_t *r) {
  if (!r)
    return;
  ast_free(r->ast);
  free(r);
}