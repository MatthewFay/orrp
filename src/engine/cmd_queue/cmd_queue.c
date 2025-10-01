#include "cmd_queue.h"
#include "cmd_queue_msg.h"
#include <string.h>

bool cmd_queue_init(cmd_queue_t *cmd_queue) {
  if (!cmd_queue)
    return false;
  ck_ring_init(&cmd_queue->ring, CAPACITY_PER_cmd_queue);
  return true;
}

void cmd_queue_destroy(cmd_queue_t *cmd_queue) {
  if (!cmd_queue)
    return;
  // TODO: graceful handling of inflight msgs
}

bool cmd_queue_enqueue(cmd_queue_t *cmd_queue, cmd_queue_msg_t *msg) {
  if (!cmd_queue || !msg)
    return false;
  return ck_ring_enqueue_mpsc(&cmd_queue->ring, cmd_queue->ring_buffer, msg);
}

bool cmd_queue_dequeue(cmd_queue_t *cmd_queue, cmd_queue_msg_t **msg_out) {
  if (!cmd_queue)
    return false;
  return ck_ring_dequeue_mpsc(&cmd_queue->ring, cmd_queue->ring_buffer,
                              msg_out);
}
