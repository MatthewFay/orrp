#include "encoder.h"
#include "mpack.h"
#include <stdio.h>

bool encode_event(cmd_ctx_t *cmd_ctx, uint32_t event_id, char **data_out,
                  size_t *size_out) {
  if (!cmd_ctx || !data_out || !size_out) {
    fprintf(stderr, "encode_event: NULL parameter(s)\n");
    return false;
  }

  if (!cmd_ctx->in_tag_value) {
    fprintf(stderr, "encode_event: missing 'in' tag\n");
    return false;
  }

  if (!cmd_ctx->entity_tag_value) {
    fprintf(stderr, "encode_event: missing 'entity' tag\n");
    return false;
  }

  if (cmd_ctx->in_tag_value->type != AST_LITERAL_NODE ||
      cmd_ctx->in_tag_value->literal.type != AST_LITERAL_STRING) {
    fprintf(stderr, "encode_event: 'in' tag is not a string literal\n");
    return false;
  }

  if (cmd_ctx->entity_tag_value->type != AST_LITERAL_NODE ||
      cmd_ctx->entity_tag_value->literal.type != AST_LITERAL_STRING) {
    fprintf(stderr, "encode_event: 'entity' tag is not a string literal\n");
    return false;
  }

  ast_node_t *node = cmd_ctx->custom_tags_head;
  mpack_writer_t writer;
  mpack_writer_init_growable(&writer, data_out, size_out);

  mpack_build_map(&writer);

  mpack_write_cstr(&writer, "id");
  mpack_write_u32(&writer, event_id);

  mpack_write_cstr(&writer, "in");
  mpack_write_cstr(&writer, cmd_ctx->in_tag_value->literal.string_value);

  mpack_write_cstr(&writer, "entity");
  mpack_write_cstr(&writer, cmd_ctx->entity_tag_value->literal.string_value);

  while (node) {
    if (node->type != AST_TAG_NODE) {
      fprintf(stderr, "encode_event: invalid node type in custom tags\n");
      mpack_writer_destroy(&writer);
      if (*data_out) {
        MPACK_FREE(*data_out);
        *data_out = NULL;
        *size_out = 0;
      }
      return false;
    }

    if (node->tag.key_type != AST_TAG_KEY_CUSTOM) {
      fprintf(stderr, "encode_event: expected custom key type\n");
      mpack_writer_destroy(&writer);
      if (*data_out) {
        MPACK_FREE(*data_out);
        *data_out = NULL;
        *size_out = 0;
      }
      return false;
    }

    if (!node->tag.value || node->tag.value->type != AST_LITERAL_NODE ||
        node->tag.value->literal.type != AST_LITERAL_STRING) {
      fprintf(stderr,
              "encode_event: custom tag value is not a string literal\n");
      mpack_writer_destroy(&writer);
      if (*data_out) {
        MPACK_FREE(*data_out);
        *data_out = NULL;
        *size_out = 0;
      }
      return false;
    }

    mpack_write_cstr(&writer, node->tag.custom_key);
    mpack_write_cstr(&writer, node->tag.value->literal.string_value);

    node = node->next;
  }

  mpack_complete_map(&writer);

  if (mpack_writer_destroy(&writer) != mpack_ok) {
    fprintf(stderr, "An error occurred encoding the data!\n");
    if (*data_out) {
      MPACK_FREE(*data_out);
      *data_out = NULL;
      *size_out = 0;
    }
    return false;
  }

  return true;
}