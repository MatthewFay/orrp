#ifndef AST_H
#define AST_H

typedef struct ast_node_s ast_node_t;
typedef struct ast_command_node_s ast_command_node_t;

typedef enum {
  COMMAND_NODE,
  LOGICAL_NODE,
  NOT_NODE,
  IDENTIFIER_NODE
} ast_node_type;

typedef struct ast_not_node_s {
  ast_node_t *operand;
} ast_not_node_t;

typedef struct ast_identifier_node_s {
  char *value;
} ast_identifier_node_t;

typedef struct ast_identifier_node_list_node_s {
  ast_identifier_node_t *id;
  struct ast_identifier_node_list_node_s *next;
} ast_identifier_node_list_node_t;

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
  ast_identifier_node_list_node_t *args;
  ast_node_t *exp;
} ast_command_node_t;

typedef struct ast_node_s {
  ast_node_type type;
  union {
    ast_command_node_t *cmd;
    ast_logical_node_t *logical;
    ast_identifier_node_t *id;
    ast_not_node_t *not_op;
  } node;
} ast_node_t;

void ast_free(ast_node_t *ast);

#endif // AST_H
