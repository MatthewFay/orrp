#ifndef consumer_ops_h
#define consumer_ops_h

#include "engine/container/container_types.h"
#include "engine/op_queue/op_queue_msg.h"
#include "uthash.h"
#include <stdbool.h>

typedef struct {
  bool success;
  const char *err_msg;
} COP_RESULT_T;

COP_RESULT_T process_batch(consumer_batch_container_t *batch);

#endif