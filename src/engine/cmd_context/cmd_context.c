#include "cmd_context.h"
#include "query/ast.h"
#include <stdlib.h>
#include <string.h>

cmd_ctx_t *build_cmd_context(ast_command_node_t *cmd) {
  cmd_ctx_t *ctx = calloc(1, sizeof(cmd_ctx_t));
  if (!ctx)
    return NULL;

  ast_node_t *custom_tags_tail = NULL;

  for (ast_node_t *tag_node = cmd->tags; tag_node != NULL;
       tag_node = tag_node->next) {
    if (tag_node->type == TAG_NODE) {
      ast_tag_node_t *tag = &tag_node->tag;

      if (tag->key_type == TAG_KEY_RESERVED) {
        switch (tag->reserved_key) {
        case KEY_IN:
          ctx->in_tag_value = tag->value;
          break;
        case KEY_ENTITY:
          ctx->entity_tag_value = tag->value;
          break;
        case KEY_EXP:
          ctx->exp_tag_value = tag->value;
          break;
        case KEY_TAKE:
          ctx->take_tag_value = tag->value;
          break;
        case KEY_CURSOR:
          ctx->cursor_tag_value = tag->value;
          break;
        case KEY_ID:
          break;
        }
      } else {
        // Custom tag
        ctx->num_custom_tags++;
        if (tag_node->tag.is_counter) {
          ctx->num_counter_tags++;
        }
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

  return ctx;
}

void cmd_context_free(cmd_ctx_t *command) {
  if (!command)
    return;
  ast_free(command->cursor_tag_value);
  ast_free(command->entity_tag_value);
  ast_free(command->exp_tag_value);
  ast_free(command->in_tag_value);
  ast_free(command->take_tag_value);
  ast_free(command->custom_tags_head);
  free(command);
}