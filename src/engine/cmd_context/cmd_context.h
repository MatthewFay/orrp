#ifndef CMD_CONTEXT_H
#define CMD_CONTEXT_H

#include "query/ast.h"
#include <stdint.h>

// --- Command Context --- //

typedef struct {
  // --- Fields for Reserved Tags ---
  // TODO: support multiple `in` tags, e.g, cross-container queries
  ast_node_t *in_tag_value;
  ast_node_t *entity_tag_value;
  ast_node_t *exp_tag_value;
  ast_node_t *take_tag_value;
  ast_node_t *cursor_tag_value;

  // --- A Single List for All Custom Tags ---
  ast_node_t *custom_tags_head;
  uint32_t num_custom_tags;
  uint32_t num_counter_tags;

  // Root AST
  ast_node_t *ast;
} cmd_ctx_t;

// Takes ownership of AST - used by Engine
cmd_ctx_t *build_cmd_context(ast_node_t *ast);

void cmd_context_free(cmd_ctx_t *command);

#endif
