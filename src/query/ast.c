#include "query/ast.h"
#include <stdlib.h>
#include <string.h>

void ast_free(ast_node_t *ast) {
  if (!ast)
    return;
  switch (ast->type) {
  case COMMAND_NODE:
    if (ast->node.cmd) {
      ast_free(ast->node.cmd->args);
      ast_free(ast->node.cmd->exp);
      free(ast->node.cmd);
    }
    break;
  case LOGICAL_NODE:
    if (ast->node.logical) {
      ast_free(ast->node.logical->left_operand);
      ast_free(ast->node.logical->right_operand);
      free(ast->node.logical);
    }
    break;
  case NOT_NODE:
    if (ast->node.not_op) {
      ast_free(ast->node.not_op->operand);
      free(ast->node.not_op);
    }
    break;
  case IDENTIFIER_NODE:
    if (ast->node.id) {
      free(ast->node.id->value);
      free(ast->node.id);
    }
    break;
  case LIST_NODE:
    if (ast->node.list) {
      ast_free(ast->node.list->item);
      ast_free(ast->node.list->next);
      free(ast->node.list);
    }
    break;
  }
  free(ast);
}

ast_node_t *ast_create_list_node(ast_node_t *item, ast_node_t *next) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node)
    return NULL;
  node->type = LIST_NODE;
  node->node.list = malloc(sizeof(ast_list_node_t));
  if (!node->node.list) {
    free(node);
    return NULL;
  }
  node->node.list->item = item;
  node->node.list->next = next;
  return node;
}

// TODO: return success
void ast_list_append(ast_node_t **list_head, ast_node_t *item_to_append) {
  if (!item_to_append)
    return;
  ast_node_t *new_list_node = ast_create_list_node(item_to_append, NULL);
  if (!new_list_node) {
    return;
  }

  if (!*list_head) {
    *list_head = new_list_node;
    return;
  }

  ast_node_t *current = *list_head;
  while (current->node.list->next) {
    current = current->node.list->next;
  }
  current->node.list->next = new_list_node;
}

ast_node_t *ast_create_identifier_node(const char *value) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node)
    return NULL;
  node->type = IDENTIFIER_NODE;
  node->node.id = malloc(sizeof(ast_identifier_node_t));
  if (!node->node.id) {
    free(node);
    return NULL;
  }
  node->node.id->value = strdup(value);
  if (!node->node.id->value) {
    free(node->node.id);
    free(node);
    return NULL;
  }
  return node;
}

ast_node_t *ast_create_logical_node(ast_logical_node_op_t op, ast_node_t *left,
                                    ast_node_t *right) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node)
    return NULL;
  node->type = LOGICAL_NODE;
  node->node.logical = malloc(sizeof(ast_logical_node_t));
  if (!node->node.logical) {
    free(node);
    return NULL;
  }
  node->node.logical->op = op;
  node->node.logical->left_operand = left;
  node->node.logical->right_operand = right;
  return node;
}

ast_node_t *ast_create_not_node(ast_node_t *operand) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node)
    return NULL;
  node->type = NOT_NODE;
  node->node.not_op = malloc(sizeof(ast_not_node_t));
  if (!node->node.not_op) {
    free(node);
    return NULL;
  }
  node->node.not_op->operand = operand;
  return node;
}

ast_node_t *ast_create_command_node(ast_command_t cmd_type, ast_node_t *args,
                                    ast_node_t *exp) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node)
    return NULL;
  node->type = COMMAND_NODE;
  node->node.cmd = malloc(sizeof(ast_command_node_t));
  if (!node->node.cmd) {
    free(node);
    return NULL;
  }
  node->node.cmd->cmd_type = cmd_type;
  node->node.cmd->args = args;
  node->node.cmd->exp = exp;
  return node;
}