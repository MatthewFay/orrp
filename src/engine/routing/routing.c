#include "routing.h"
#include "core/hash.h"
#include <string.h>

#define ROUTING_HASH_SEED 0

int route_key_to_queue(const char *ser_db_key, int op_queue_total_count) {
  unsigned long hash =
      xxhash64(ser_db_key, strlen(ser_db_key), ROUTING_HASH_SEED);
  return hash & (op_queue_total_count - 1);
}

int route_key_to_consumer(const char *ser_db_key, int op_queue_total_count,
                          int op_queues_per_consumer) {
  int queue_idx = route_key_to_queue(ser_db_key, op_queue_total_count);
  return queue_idx / op_queues_per_consumer;
}