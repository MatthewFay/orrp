#include "cmd_context.h"
#include "query/ast.h"
#include <stdlib.h>
#include <string.h>

cmd_ctx_t *build_cmd_context(ast_node_t *ast, int64_t arrival_ts) {
  // init `num_custom_tags` to 0
  cmd_ctx_t *ctx = calloc(1, sizeof(cmd_ctx_t));
  if (!ctx)
    return NULL;

  ctx->ast = ast;
  ast_command_node_t *cmd = &ast->command;
  ast_node_t *custom_tags_tail = NULL;

  for (ast_node_t *tag_node = cmd->tags; tag_node != NULL;
       tag_node = tag_node->next) {
    if (tag_node->type == AST_TAG_NODE) {
      ast_tag_node_t *tag = &tag_node->tag;

      if (tag->key_type == AST_TAG_KEY_RESERVED) {
        switch (tag->reserved_key) {
        case AST_KW_IN:
          ctx->in_tag_value = tag->value;
          break;
        case AST_KW_ENTITY:
          ctx->entity_tag_value = tag->value;
          break;
        case AST_KW_WHERE:
          ctx->where_tag_value = tag->value;
          break;
        case AST_KW_TAKE:
          ctx->take_tag_value = tag->value;
          break;
        case AST_KW_CURSOR:
          ctx->cursor_tag_value = tag->value;
          break;
        case AST_KW_ID:
          break;
        case AST_KW_KEY:
          ctx->key_tag_value = tag->value;
          break;
        default:
          break;
        }
      } else {
        // Custom tag
        ctx->num_custom_tags++;
        if (ctx->custom_tags_head == NULL) {
          ctx->custom_tags_head = tag_node;
          custom_tags_tail = tag_node;
        } else {
          custom_tags_tail->next = tag_node;
          custom_tags_tail = tag_node;
        }
      }
    }
  }

  // Terminate the new list. The last custom tag's `next`
  // pointer might still point to a reserved tag from the original list.
  if (custom_tags_tail != NULL) {
    custom_tags_tail->next = NULL;
  }

  ctx->arrival_ts = arrival_ts;

  return ctx;
}

void cmd_context_free(cmd_ctx_t *command) {
  if (!command)
    return;

  // Free the entire AST, which this context now owns.
  ast_free(command->ast);
  command->ast = NULL;

  // The other pointers (in_tag_value, etc.) are inside the AST,
  // so they are freed by the call above. We only need to free the context
  // struct itself.
  free(command);
}