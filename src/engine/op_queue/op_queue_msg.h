#ifndef op_queue_MSG_H
#define op_queue_MSG_H

#include "engine/op/op.h"
#include <stdint.h>

typedef struct op_queue_msg_s {
  op_t *op;
  char *routing_key; // key is hashed and mapped to an actual op queue owned by
                     // a dedicated thread
} op_queue_msg_t;

op_queue_msg_t *op_queue_msg_create(const char *key, op_t *op);

void op_queue_msg_free(op_queue_msg_t *msg);

#endif