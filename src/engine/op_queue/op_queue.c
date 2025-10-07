#include "op_queue.h"
#include "ck_ring.h"

bool op_queue_init(op_queue_t *op_queue) {
  if (!op_queue)
    return false;
  ck_ring_init(&op_queue->ring, CAPACITY_PER_op_queue);
  return true;
}
void op_queue_destroy(op_queue_t *op_queue) { (void)op_queue; }

bool op_queue_enqueue(op_queue_t *op_queue, op_queue_msg_t *msg) {
  if (!op_queue || !msg)
    return false;
  return ck_ring_enqueue_mpsc(&op_queue->ring, op_queue->ring_buffer, msg);
}

bool op_queue_dequeue(op_queue_t *op_queue, op_queue_msg_t **msg_out) {
  if (!op_queue)
    return false;
  return ck_ring_dequeue_mpsc(&op_queue->ring, op_queue->ring_buffer, msg_out);
}
