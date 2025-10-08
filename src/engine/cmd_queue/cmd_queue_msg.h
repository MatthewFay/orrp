#ifndef CMD_queue_MSG_H
#define CMD_queue_MSG_H

#include "engine/cmd_context/cmd_context.h"
#include <stdint.h>

typedef struct cmd_queue_msg_s {
  cmd_ctx_t *command;
} cmd_queue_msg_t;

cmd_queue_msg_t *cmd_queue_create_msg(cmd_ctx_t *command);

void cmd_queue_free_msg(cmd_queue_msg_t *msg);

#endif