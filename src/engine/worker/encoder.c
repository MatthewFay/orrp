#include "encoder.h"
#include "mpack.h"
#include "query/ast.h"
#include <stdio.h>

bool encode_event(cmd_ctx_t *cmd_ctx, uint32_t event_id, char **data_out,
                  size_t *size_out) {
  if (!cmd_ctx || !data_out || !size_out) {
    fprintf(stderr, "encode_event: NULL parameter(s)\n");
    return false;
  }

  if (!cmd_ctx->in_tag_value ||
      cmd_ctx->in_tag_value->type != AST_LITERAL_NODE) {
    fprintf(stderr, "encode_event: missing or invalid 'in' tag\n");
    return false;
  }

  if (!cmd_ctx->entity_tag_value ||
      cmd_ctx->entity_tag_value->type != AST_LITERAL_NODE) {
    fprintf(stderr, "encode_event: missing or invalid 'entity' tag\n");
    return false;
  }

  // `mpack_start_map` requires exact element count upfront.
  // 'id', 'in', 'entity', and `ts` are always present (4), plus the count of
  // custom tags.
  uint32_t map_count = 4 + cmd_ctx->num_custom_tags;

  mpack_writer_t writer;
  // Initialize growable buffer. mpack handles realloc; caller must free
  // *data_out.
  mpack_writer_init_growable(&writer, data_out, size_out);

  mpack_start_map(&writer, map_count);

  mpack_write_cstr(&writer, "id");
  mpack_write_u32(&writer, event_id);

  mpack_write_cstr(&writer, "in");
  mpack_write_cstr(&writer, cmd_ctx->in_tag_value->literal.string_value);

  mpack_write_cstr(&writer, "entity");
  if (cmd_ctx->entity_tag_value->literal.type == AST_LITERAL_STRING) {
    mpack_write_cstr(&writer, cmd_ctx->entity_tag_value->literal.string_value);
  } else {
    mpack_write_i64(&writer, cmd_ctx->entity_tag_value->literal.number_value);
  }

  mpack_write_cstr(&writer, "ts");
  // we use milliseconds for compatability
  int64_t ts_ms = cmd_ctx->arrival_ts / 1000000L;
  mpack_write_i64(&writer, ts_ms);

  ast_node_t *node = cmd_ctx->custom_tags_head;
  while (node) {
    // Fail fast if AST structure is invalid to prevent writing corrupt data
    if (node->type != AST_TAG_NODE) {
      mpack_writer_destroy(&writer);
      // Clean up buffer so caller doesn't try to use invalid/partial data
      if (*data_out) {
        free(*data_out);
        *data_out = NULL;
        *size_out = 0;
      }
      return false;
    }

    if (node->tag.key_type == AST_TAG_KEY_CUSTOM) {
      mpack_write_cstr(&writer, node->tag.custom_key);
    } else {
      // Fallback for reserved keys in custom list to maintain map pairing
      mpack_write_cstr(&writer, "reserved_unknown");
    }

    if (node->tag.value && node->tag.value->type == AST_LITERAL_NODE) {
      if (node->tag.value->literal.type == AST_LITERAL_STRING) {
        mpack_write_cstr(&writer, node->tag.value->literal.string_value);
      } else if (node->tag.value->literal.type == AST_LITERAL_NUMBER) {
        mpack_write_i64(&writer, node->tag.value->literal.number_value);
      } else {
        mpack_write_nil(&writer);
      }
    } else {
      mpack_write_nil(&writer); // Unsupported value type
    }

    node = node->next;
  }

  mpack_finish_map(&writer);

  // Verify that the number of items written matches map_count
  if (mpack_writer_destroy(&writer) != mpack_ok) {
    fprintf(stderr, "Encoding error or map size mismatch\n");
    if (*data_out) {
      free(*data_out);
      *data_out = NULL;
      *size_out = 0;
    }
    return false;
  }

  return true;
}