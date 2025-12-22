#ifndef WORKER_WRITER_H
#define WORKER_WRITER_H

#include "engine/cmd_queue/cmd_queue_msg.h"
#include "engine/engine_writer/engine_writer_queue_msg.h"
#include "query/ast.h"

// Create writer queue message
eng_writer_msg_t *worker_create_writer_msg(cmd_queue_msg_t *cmd_msg,
                                           char *container_name,
                                           uint32_t event_id, uint32_t ent_id,
                                           ast_literal_node_t *ent_node,
                                           bool is_new_ent);

#endif