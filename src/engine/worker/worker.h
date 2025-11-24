#ifndef WORKER_H
#define WORKER_H

#include "core/bitmaps.h"
#include "engine/cmd_queue/cmd_queue.h"
#include "engine/container/container_types.h"
#include "engine/op_queue/op_queue.h"
#include "lmdb.h"
#include "uthash.h"
#include "uv.h" // IWYU pragma: keep
#include "worker_types.h"
#include <stdint.h>

typedef struct worker_container_entities_s {
  UT_hash_handle hh;
  char *container_name;
  bitmap_t *entities;
} worker_container_entities_t;

typedef struct worker_entity_mapping_s {
  UT_hash_handle hh;
  char *ent_str_id;
  uint32_t ent_int_id;
} worker_entity_mapping_t;

// user data containers for processing optimization across cmd msgs
typedef struct worker_user_dc_s {
  UT_hash_handle hh;
  char *container_name;
  eng_container_t *dc;
  MDB_txn *txn;
} worker_user_dc_t;

typedef struct worker_config_s {
  cmd_queue_t *cmd_queues;
  uint32_t cmd_queue_consume_start; // Starting cmd queue index to consume
  uint32_t cmd_queue_consume_count; // Number of cmd queues to consume from
  op_queue_t *op_queues;
  uint32_t op_queue_total_count; // Total count of op queues
} worker_config_t;

typedef struct worker_s {
  worker_config_t config;
  uv_thread_t thread;
  worker_entity_mapping_t *entity_mappings;
  // we keep one per thread because each thread is entity-scoped
  worker_container_entities_t *container_entities;
  worker_entity_tag_counter_t *entity_tag_counters;
  worker_user_dc_t *user_dcs;
  volatile bool should_stop;
  uint64_t messages_processed; // Stats
} worker_t;

typedef struct {
  bool success;
  const char *msg;
  uint32_t next_ent_id;
} worker_init_result_t;

// Call this before `worker_start` - sets up environment for worker threads
worker_init_result_t worker_init_global(void);

// Call this AFTER all worker threads have been stopped and joined.
// It cleans up shared static resources (like the event ID cache).
void worker_destroy_global(void);

typedef struct {
  bool success;
  const char *msg;
} worker_result_t;

worker_result_t worker_start(worker_t *worker, const worker_config_t *config);
worker_result_t worker_stop(worker_t *worker);

#endif