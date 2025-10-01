#ifndef OP_QUEUE_H
#define OP_QUEUE_H
#include "ck_ring.h"
#include "engine/op_queue/op_queue_msg.h"
#define CAPACITY_PER_op_queue 65536

typedef struct op_queue_s {
  ck_ring_t ring;
  ck_ring_buffer_t ring_buffer[CAPACITY_PER_op_queue];
} op_queue_t;

bool op_queue_init(op_queue_t *op_queue);
void op_queue_destroy(op_queue_t *op_queue);

bool op_queue_enqueue(op_queue_t *op_queue, op_queue_msg_t *msg);
bool op_queue_dequeue(op_queue_t *op_queue, op_queue_msg_t **msg_out);

#endif