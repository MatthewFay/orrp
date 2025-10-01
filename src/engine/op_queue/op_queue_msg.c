#include "op_queue_msg.h"
#include <stdlib.h>
#include <string.h>

op_queue_msg_t *op_queue_msg_create(const char *key, op_t *op) {
  op_queue_msg_t *msg = malloc(sizeof(op_queue_msg_t));
  if (!msg)
    return NULL;

  msg->routing_key = strdup(key); // takes ownership of key
  msg->op = op;                   // takes ownership of op

  return msg;
}

void op_queue_msg_free(op_queue_msg_t *msg) {
  if (!msg)
    return;

  op_destroy(msg->op);

  free(msg->op);
  free(msg->routing_key);
  free(msg);
}
