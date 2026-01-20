#ifndef VALIDATOR_H
#define VALIDATOR_H

// Validator module that performs Semantic Analysis on a given AST tree

#include "query/ast.h"
#include "uthash.h"
#include <stdbool.h>
typedef struct validator_result_s {
  bool is_valid;
  const char *err_msg; // If not valid

} validator_result_t;

typedef struct {
  char *key;
  UT_hash_handle hh;
} custom_tag_key_t;

// Semantic analysis
void validator_analyze(ast_node_t *root, validator_result_t *result_out);

#endif