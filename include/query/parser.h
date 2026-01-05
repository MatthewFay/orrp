#ifndef PARSER_H
#define PARSER_H

#include "core/queue.h"
#include "query/ast.h"

typedef enum {
  PARSER_OP_TYPE_READ,
  PARSER_OP_TYPE_WRITE,
  PARSER_OP_TYPE_ADMIN,
  PARSER_OP_TYPE_ERROR
} parser_op_type_t;

typedef struct parse_result_s {
  parser_op_type_t type;
  ast_node_t *ast;           // Can be NULL if type is ERROR
  const char *error_message; // Optional: for detailed errors
} parse_result_t;

// Parse command tokens into parse_result_t. Thread-safe; returns heap-allocated
// parse_result_t. Caller must free parse_result_t with parse_free_result().
parse_result_t *parse(queue_t *tokens);

// Does not free AST on purpose - Engine takes ownership
void parse_free_result(parse_result_t *r);

#endif // PARSER_H
