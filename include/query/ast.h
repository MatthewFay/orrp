#ifndef AST_H
#define AST_H

#include <stdbool.h>
#include <stdint.h>

typedef struct ast_node_s ast_node_t;

typedef enum {
  AST_COMMAND_NODE,
  AST_TAG_NODE,
  AST_LITERAL_NODE,
  // Comparison nodes (>, <, =, etc.) operate on comparable values and produce
  // booleans
  AST_COMPARISON_NODE,
  // Logical nodes (AND, OR, NOT) operate on boolean values
  AST_LOGICAL_NODE,
  AST_NOT_NODE,
} ast_node_type;

// An enum for all known, special-purpose tag keys
typedef enum {
  AST_KEY_IN,
  AST_KEY_ENTITY,
  AST_KEY_WHERE,
  AST_KEY_TAKE,
  AST_KEY_CURSOR,
  AST_KEY_ID, // event id, for idempotency
} ast_reserved_key_t;

typedef enum { AST_TAG_KEY_RESERVED, AST_TAG_KEY_CUSTOM } ast_tag_key_type_t;

typedef struct {
  ast_tag_key_type_t key_type;
  union {
    ast_reserved_key_t reserved_key;
    char *custom_key;
  };
  ast_node_t *value;
  bool is_counter;
} ast_tag_node_t;

// Represents a string or number value.
typedef enum { AST_LITERAL_STRING, AST_LITERAL_NUMBER } ast_literal_type_t;

typedef struct {
  ast_literal_type_t type;
  union {
    char *string_value;
    uint32_t number_value;
  };
} ast_literal_node_t;

typedef enum {
  AST_OP_GT,
  AST_OP_LT,
  AST_OP_GTE,
  AST_OP_LTE,
  AST_OP_EQ,
  AST_OP_NEQ
} ast_comparison_op_t;

typedef struct {
  ast_comparison_op_t op;

  // The node's type tells the engine what to query.
  // If its type is NODE_TAG, it's a full tag comparison
  // Future version: If its type is NODE_LITERAL, it's a key-only comparison
  ast_node_t *left;  // Left operand (e.g., tag or number)
  ast_node_t *right; // Right operand (e.g., number or tag)
} ast_comparison_node_t;

typedef struct ast_not_node_s {
  ast_node_t *operand;
} ast_not_node_t;

typedef enum { AST_LOGIC_NODE_AND, AST_LOGIC_NODE_OR } ast_logical_node_op_t;

typedef struct ast_logical_node_s {
  ast_logical_node_op_t op;
  ast_node_t *left_operand;
  ast_node_t *right_operand;
} ast_logical_node_t;

typedef enum { AST_CMD_EVENT, AST_CMD_QUERY } ast_command_type_t;

// The root of the AST. It contains a pointer to the head of a linked list of
// tags.
typedef struct {
  ast_command_type_t type;
  ast_node_t
      *tags; // The first tag in the list (an ast_node_t of type NODE_TAG)
} ast_command_node_t;

struct ast_node_s {
  ast_node_type type;
  union {
    ast_command_node_t command;
    ast_tag_node_t tag;
    ast_literal_node_t literal;
    ast_logical_node_t logical;
    ast_comparison_node_t comparison;
    ast_not_node_t not_op;
  };
  ast_node_t *next; // Pointer to the next node in a list (e.g., the next tag)
};

//==============================================================================
// Function Declarations
//==============================================================================

// Memory management
void ast_free(ast_node_t *node);

// Node creation
ast_node_t *ast_create_command_node(ast_command_type_t type, ast_node_t *tags);
ast_node_t *ast_create_tag_node(ast_reserved_key_t key, ast_node_t *value,
                                bool is_counter);
ast_node_t *ast_create_custom_tag_node(const char *key, ast_node_t *value,
                                       bool is_counter);
ast_node_t *ast_create_string_literal_node(const char *value);
ast_node_t *ast_create_number_literal_node(uint32_t value);
ast_node_t *ast_create_comparison_node(ast_comparison_op_t op, ast_node_t *key,
                                       ast_node_t *value);
ast_node_t *ast_create_logical_node(ast_logical_node_op_t op, ast_node_t *left,
                                    ast_node_t *right);
ast_node_t *ast_create_not_node(ast_node_t *operand);

// List manipulation
void ast_append_node(ast_node_t **list_head, ast_node_t *node_to_append);

#endif // AST_H