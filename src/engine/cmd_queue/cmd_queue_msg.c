#include "cmd_queue_msg.h"
#include <stdlib.h>

cmd_queue_msg_t *cmd_queue_create_msg(cmd_ctx_t *command) {
  if (!command)
    return false;
  cmd_queue_msg_t *msg = calloc(1, sizeof(cmd_queue_msg_t));
  if (!msg)
    return false;
  msg->command = command;
  return msg;
}

void cmd_queue_free_msg(cmd_queue_msg_t *msg) {
  if (!msg)
    return;
  cmd_context_free(msg->command);
  free(msg);
}
