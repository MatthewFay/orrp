#ifndef WORKER_OPS_H
#define WORKER_OPS_H

#include "engine/cmd_queue/cmd_queue_msg.h"
#include "engine/op_queue/op_queue_msg.h"
#include <stdint.h>

typedef struct {
  op_queue_msg_t **ops;
  uint32_t count;
} worker_ops_array_t;

typedef struct worker_ops_s {
  // System ops
  op_queue_msg_t *incr_entity_id_op;
  op_queue_msg_t *ent_id_to_int_op;
  op_queue_msg_t *int_to_ent_id_op;
  op_queue_msg_t *incr_event_id_op;

  // User ops
  worker_ops_array_t write_to_event_index_ops;
  op_queue_msg_t *event_to_entity_op;
  worker_ops_array_t counter_store_ops;
  worker_ops_array_t count_index_ops;
} worker_ops_t;

bool worker_ops_create_ops(cmd_queue_msg_t *msg, char *container_name,
                           char *entity_id_str, uint32_t entity_id_int32,
                           bool is_new_entity, uint32_t event_id,
                           worker_ops_t *ops_out);

#endif