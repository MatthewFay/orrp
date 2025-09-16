#include "flush_msg.h"

flush_msg_t *flush_msg_create(flush_msg_data_type data_type, void *data) {
  if (!data)
    return NULL;
  flush_msg_t *flush_msg = calloc(1, sizeof(flush_msg_t));
  if (!flush_msg)
    return NULL;
  flush_msg->data_type = data_type;
  switch (data_type) {
  case BITMAP_DIRTY_SNAPSHOT:
    flush_msg->data.bm_cache_dirty_snapshot = data;
    break;
  default:
    break;
  }
  return flush_msg;
}
void flush_msg_free(flush_msg_t *flush_msg) { free(flush_msg); }
