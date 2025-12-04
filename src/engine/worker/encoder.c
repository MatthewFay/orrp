#include "encoder.h"
#include "mpack.h"

bool encode_event(cmd_ctx_t *cmd_ctx, uint32_t event_id, char **data_out,
                  size_t *size_out) {
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
    mpack_write_cstr(&writer, node->tag.custom_key);
    mpack_write_cstr(&writer, node->tag.value->literal.string_value);
    node = node->next;
  }

  mpack_complete_map(&writer);

  if (mpack_writer_destroy(&writer) != mpack_ok) {
    fprintf(stderr, "An error occurred encoding the data!\n");
    return false;
  }
  return true;
}