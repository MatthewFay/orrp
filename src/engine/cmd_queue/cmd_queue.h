#ifndef CMD_QUEUE_H
#define CMD_QUEUE_H

#include "ck_ring.h"
#include "cmd_queue_msg.h"
#include <stdbool.h>

#define CAPACITY_PER_cmd_queue 65536

typedef struct cmd_queue_s {
  ck_ring_t ring;
  ck_ring_buffer_t ring_buffer[CAPACITY_PER_cmd_queue];
} cmd_queue_t;

bool cmd_queue_init(cmd_queue_t *cmd_queue);
void cmd_queue_destroy(cmd_queue_t *cmd_queue);

bool cmd_queue_enqueue(cmd_queue_t *cmd_queue, cmd_queue_msg_t *msg);
bool cmd_queue_dequeue(cmd_queue_t *cmd_queue, cmd_queue_msg_t **msg_out);

#endif