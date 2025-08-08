#include "query/parser.h"
#include "core/queue.h"
#include "query/ast.h"
#include "query/tokenizer.h"
#include <stdlib.h>
#include <string.h>

static void _cleanup(token_t *token, Queue *tokens, ast_node_t *ast) {
  tok_free(token);
  tok_free_tokens(tokens);
  ast_free(ast);
}

static ast_node_t *_parse_add(Queue *tokens) {
  ast_node_t *cmd_node = ast_create_command_node(ADD, NULL, NULL);
  if (!cmd_node) {

    return NULL;
  }
  while (!q_empty(tokens)) {
    token_t *t = q_dequeue(tokens);
    if (t->type != TEXT || t->text_value == NULL) {
      _cleanup(t, tokens, cmd_node);
      return NULL;
    }
    ast_list_append(&(cmd_node->node.cmd->args),
                    ast_create_identifier_node(t->text_value));
  }
  return cmd_node;
}

static ast_node_t *_parse_exp(Queue *tokens) {}

ast_node_t *parse(Queue *tokens) {
  if (!tokens)
    return NULL;
  if (q_empty(tokens)) {
    q_destroy(tokens);
    return NULL;
  }
  token_t *cmd_token = q_dequeue(tokens);
  if (cmd_token->type != TEXT || cmd_token->text_value == NULL) {
    _cleanup(cmd_token, tokens, NULL);
    return NULL;
  }

  ast_node_t *ast = NULL;

  if (strcmp("add", cmd_token->text_value) == 0) {
    ast = _parse_add(tokens);
  }

  if (strcmp("query", cmd_token->text_value) == 0) {
    ast = _parse_exp(tokens);
  }

  _cleanup(cmd_token, tokens, NULL);
  return ast;
}