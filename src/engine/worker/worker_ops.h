#ifndef WORKER_OPS__H
#define WORKER_OPS__H

#include "engine/cmd_queue/cmd_queue_msg.h"
#include "engine/op_queue/op_queue_msg.h"
#include <stdint.h>

typedef struct worker_ops_s {
  op_queue_msg_t **ops;
  uint32_t num_ops;
} worker_ops_t;

bool worker_create_ops(cmd_queue_msg_t *msg, char *container_name,
                       char *entity_id_str, uint32_t entity_id_int32,
                       bool is_new_entity, uint32_t event_id,
                       worker_ops_t *ops_out);

void worker_ops_free(worker_ops_t *ops);

#endif