#ifndef WORKER_OPS__H
#define WORKER_OPS__H

#include "engine/cmd_queue/cmd_queue_msg.h"
#include "engine/op_queue/op_queue_msg.h"
#include "engine/worker/worker_types.h"
#include <stdint.h>

typedef struct {
  bool success;
  const char *error_msg; // Static string, do not free
  const char *context;   // Static string for additional context, do not free
} worker_ops_result_t;

#define WORKER_OPS_SUCCESS()                                                   \
  ((worker_ops_result_t){.success = true, .error_msg = NULL, .context = NULL})
#define WORKER_OPS_ERROR(msg, ctx)                                             \
  ((worker_ops_result_t){                                                      \
      .success = false, .error_msg = (msg), .context = (ctx)})

typedef struct worker_ops_s {
  op_queue_msg_t **ops;
  uint32_t num_ops;
} worker_ops_t;

worker_ops_result_t worker_create_ops(cmd_queue_msg_t *msg,
                                      char *container_name, char *entity_id_str,
                                      uint32_t entity_id_int32,
                                      bool is_new_entity, uint32_t event_id,
                                      worker_entity_tag_counter_t *tag_counters,
                                      worker_ops_t *ops_out,
                                    bool is_new_container_ent);

// Free ops array
void worker_ops_clear(worker_ops_t *ops);

#endif