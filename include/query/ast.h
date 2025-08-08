#ifndef AST_H
#define AST_H

#include <stdbool.h>

typedef struct ast_node_s ast_node_t;
typedef struct ast_command_node_s ast_command_node_t;
typedef struct ast_list_node_s ast_list_node_t;

typedef enum {
  COMMAND_NODE,
  LOGICAL_NODE,
  NOT_NODE,
  IDENTIFIER_NODE,
  LIST_NODE
} ast_node_type;

typedef struct ast_list_node_s {
  ast_node_t *item; // A pointer to the node in this position
  ast_node_t *next; // A pointer to the next list node (or NULL)
} ast_list_node_t;

typedef struct ast_not_node_s {
  ast_node_t *operand;
} ast_not_node_t;

typedef struct ast_identifier_node_s {
  char *value;
} ast_identifier_node_t;

typedef enum { AND, OR } ast_logical_node_op_t;

typedef struct ast_logical_node_s {
  ast_logical_node_op_t op;
  ast_node_t *left_operand;
  ast_node_t *right_operand;
} ast_logical_node_t;

typedef enum {
  ADD,
  QUERY,
} ast_command_t;

typedef struct ast_command_node_s {
  ast_command_t cmd_type;
  ast_node_t *args;
  ast_node_t *exp;
} ast_command_node_t;

typedef struct ast_node_s {
  ast_node_type type;
  union {
    ast_command_node_t *cmd;
    ast_logical_node_t *logical;
    ast_identifier_node_t *id;
    ast_not_node_t *not_op;
    ast_list_node_t *list;
  } node;
} ast_node_t;

void ast_free(ast_node_t *ast);

ast_node_t *ast_create_identifier_node(const char *value);
ast_node_t *ast_create_logical_node(ast_logical_node_op_t op, ast_node_t *left,
                                    ast_node_t *right);
ast_node_t *ast_create_not_node(ast_node_t *operand);
ast_node_t *ast_create_command_node(ast_command_t cmd_type, ast_node_t *args,
                                    ast_node_t *exp);
ast_node_t *ast_create_list_node(ast_node_t *item, ast_node_t *next);
void ast_list_append(ast_node_t **list_head, ast_node_t *item_to_append);

#endif // AST_H
