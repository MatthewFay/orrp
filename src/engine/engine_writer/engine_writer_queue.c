#include "engine_writer_queue.h"

bool eng_writer_queue_init(eng_writer_queue_t *eng_writer_queue) {
  if (!eng_writer_queue)
    return false;
  ck_ring_init(&eng_writer_queue->ring, ENG_WRITER_QUEUE_CAPACITY);
  return true;
}
void eng_writer_queue_destroy(eng_writer_queue_t *eng_writer_queue) {
  (void)eng_writer_queue;
}

bool eng_writer_queue_enqueue(eng_writer_queue_t *eng_writer_queue,
                              eng_writer_msg_t *msg) {
  if (!eng_writer_queue || !msg)
    return false;
  return ck_ring_enqueue_mpsc(&eng_writer_queue->ring,
                              eng_writer_queue->ring_buffer, msg);
}
