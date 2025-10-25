#include "engine_writer_queue_msg.h"
#include "core/bitmaps.h"
#include "engine/container/container.h"

void eng_writer_queue_free_msg_entry(eng_writer_entry_t *e) {
  if (!e)
    return;
  if (e->val_type == ENG_WRITER_VAL_BITMAP) {
    bitmap_free(e->val.bitmap_copy);
  } else if (e->val_type == ENG_WRITER_VAL_STR) {
    free(e->val.str_copy);
  }
  container_free_db_key_contents(&e->db_key);
  // Don't free `flush_version_ptr` as consumer owns this
  free(e);
}

void eng_writer_queue_free_msg(eng_writer_msg_t *msg) {
  if (!msg)
    return;
  if (msg->entries) {
    for (unsigned int i = 0; i < msg->count; i++) {
      eng_writer_entry_t *e = &msg->entries[i];
      eng_writer_queue_free_msg_entry(e);
    }
  }
  free(msg->entries);
  free(msg);
}