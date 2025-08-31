#include "query/ast.h"

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

} cmd_ctx_t;

void build_cmd_context(ast_command_node_t *cmd, cmd_ctx_t *ctx);
