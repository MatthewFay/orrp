#ifndef consumer_batch_h
#define consumer_batch_h

#include "engine/op_queue/op_queue_msg.h"
#include "uthash.h"
#include <stdint.h>

// Linked list node for messages with same db_key
typedef struct consumer_batch_msg_node_s {
  op_queue_msg_t *msg;
  struct consumer_batch_msg_node_s *next;
} consumer_batch_msg_node_t;

// Hash table entry: one per unique db_key within a container
typedef struct consumer_batch_db_key_s {
  UT_hash_handle hh;
  const char *ser_db_key; // hash key (borrowed)
  consumer_batch_msg_node_t *head;
  consumer_batch_msg_node_t *tail;
  uint32_t count;
} consumer_batch_db_key_t;

// Hash table entry: one per unique container
typedef struct consumer_batch_container_s {
  UT_hash_handle hh;
  const char *container_name; // hash key (borrowed)
  eng_dc_type_t container_type;
  consumer_batch_db_key_t *db_keys; // uthash table of db_keys
} consumer_batch_container_t;

bool consumer_batch_add_msg(consumer_batch_container_t **container_table,
                            op_queue_msg_t *msg);

// Also free's consumed `op` msgs
void consumer_batch_free_table(consumer_batch_container_t *container_table);

#endif