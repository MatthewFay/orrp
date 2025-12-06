#include "engine_writer_queue_msg.h"
#include "engine/container/container.h"

// Doesn't free `flush_version_ptr` as caller owns this
void eng_writer_queue_free_msg_entry(eng_writer_entry_t *e) {
  if (!e)
    return;
  free(e->value);
  container_free_db_key_contents(&e->db_key);
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