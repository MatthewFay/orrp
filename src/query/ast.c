#include "query/ast.h"
#include <stdlib.h>
#include <string.h>

/**
 * @brief Recursively frees an AST node and all its children.
 *
 * This function traverses the AST, freeing each node and its specific contents.
 * It also handles freeing linked lists of nodes (like tags) by recursively
 * calling itself on the `next` pointer.
 *
 * @param node The root node of the AST (or sub-tree) to free.
 */
void ast_free(ast_node_t *node) {
  if (!node) {
    return;
  }

  // Free the specific data within the node based on its type
  switch (node->type) {
  case AST_COMMAND_NODE:
    ast_free(node->command.tags); // Free the linked list of tags
    break;
  case AST_TAG_NODE:
    if (node->tag.key_type == AST_TAG_KEY_CUSTOM) {
      free(node->tag.custom_key); // Free string copied for custom key
    }
    ast_free(node->tag.value);
    break;
  case AST_LITERAL_NODE:
    if (node->literal.type == AST_LITERAL_STRING) {
      free(node->literal.string_value); // Free string copied for literal
    }
    break;
  case AST_COMPARISON_NODE:
    ast_free(node->comparison.left);
    ast_free(node->comparison.right);
    break;
  case AST_LOGICAL_NODE:
    ast_free(node->logical.left_operand);
    ast_free(node->logical.right_operand);
    break;
  case AST_NOT_NODE:
    ast_free(node->not_op.operand);
    break;
  }

  // Free the next node in the list, if one exists
  ast_free(node->next);

  // Finally, free the node structure itself
  free(node);
}

/**
 * @brief Appends a node to the end of a linked list of nodes.
 *
 * @param list_head A pointer to the head of the list.
 * @param node_to_append The node to add to the end of the list.
 */
void ast_append_node(ast_node_t **list_head, ast_node_t *node_to_append) {
  if (!node_to_append) {
    return;
  }
  node_to_append->next = NULL;

  if (!*list_head) {
    *list_head = node_to_append;
    return;
  }

  ast_node_t *current = *list_head;
  while (current->next) {
    current = current->next;
  }
  current->next = node_to_append;
}

//==============================================================================
// Node Creation Functions
//==============================================================================

ast_node_t *ast_create_command_node(ast_command_type_t type, ast_node_t *tags) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node) {
    return NULL;
  }

  node->type = AST_COMMAND_NODE;
  node->next = NULL;
  node->command.type = type;
  node->command.tags = tags;
  return node;
}

ast_node_t *ast_create_tag_node(ast_reserved_key_t key, ast_node_t *value,
                                bool is_counter) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node) {
    return NULL;
  }

  node->type = AST_TAG_NODE;
  node->next = NULL;
  node->tag.key_type = AST_TAG_KEY_RESERVED;
  node->tag.reserved_key = key;
  node->tag.value = value;
  node->tag.is_counter = is_counter;
  return node;
}

ast_node_t *ast_create_custom_tag_node(const char *key, ast_node_t *value,
                                       bool is_counter) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node) {
    return NULL;
  }

  node->type = AST_TAG_NODE;
  node->next = NULL;
  node->tag.key_type = AST_TAG_KEY_CUSTOM;
  node->tag.custom_key = strdup(key);
  if (!node->tag.custom_key) {
    free(node);
    return NULL;
  }
  node->tag.value = value;
  node->tag.is_counter = is_counter;
  return node;
}

ast_node_t *ast_create_string_literal_node(const char *value) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node) {
    return NULL;
  }

  node->type = AST_LITERAL_NODE;
  node->next = NULL;
  node->literal.type = AST_LITERAL_STRING;
  node->literal.string_value = strdup(value);
  if (!node->literal.string_value) {
    free(node);
    return NULL;
  }
  return node;
}

ast_node_t *ast_create_number_literal_node(uint32_t value) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node) {
    return NULL;
  }

  node->type = AST_LITERAL_NODE;
  node->next = NULL;
  node->literal.type = AST_LITERAL_NUMBER;
  node->literal.number_value = value;
  return node;
}

ast_node_t *ast_create_comparison_node(ast_comparison_op_t op, ast_node_t *left,
                                       ast_node_t *right) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node) {
    return NULL;
  }

  node->type = AST_COMPARISON_NODE;
  node->next = NULL;
  node->comparison.op = op;
  node->comparison.left = left;
  node->comparison.right = right;
  return node;
}

ast_node_t *ast_create_logical_node(ast_logical_node_op_t op, ast_node_t *left,
                                    ast_node_t *right) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node) {
    return NULL;
  }

  node->type = AST_LOGICAL_NODE;
  node->next = NULL;
  node->logical.op = op;
  node->logical.left_operand = left;
  node->logical.right_operand = right;
  return node;
}

ast_node_t *ast_create_not_node(ast_node_t *operand) {
  ast_node_t *node = malloc(sizeof(ast_node_t));
  if (!node) {
    return NULL;
  }

  node->type = AST_NOT_NODE;
  node->next = NULL;
  node->not_op.operand = operand;
  return node;
}