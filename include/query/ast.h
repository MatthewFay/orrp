#ifndef AST_H
#define AST_H

typedef enum { COMMAND_NODE, LOGICAL_NODE, IDENTIFIER_NODE } ast_node_type;

typedef struct ast_identifier_node_s {
  char *value;
} ast_identifier_node_t;

typedef struct ast_logical_node_s {
  char *value;
} ast_logical_node_t;

#endif // AST_H
