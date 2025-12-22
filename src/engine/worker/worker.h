#ifndef WORKER_H
#define WORKER_H

#include "core/map.h" // IWYU pragma: keep
#include "engine/cmd_queue/cmd_queue.h"
#include "engine/container/container_types.h"
#include "engine/engine_writer/engine_writer.h"
#include "engine/op_queue/op_queue.h"
#include "khash.h"
#include "lmdb.h"
#include "uv.h" // IWYU pragma: keep
#include <stdint.h>

// user data containers cached across batch of cmd msgs
typedef struct worker_user_dc_s {
  UT_hash_handle hh;
  char *container_name;
  eng_container_t *dc;
  MDB_txn *txn;
} worker_user_dc_t;

typedef struct worker_config_s {
  eng_writer_t *writer;
  cmd_queue_t *cmd_queues;
  uint32_t cmd_queue_consume_start; // Starting cmd queue index to consume
  uint32_t cmd_queue_consume_count; // Number of cmd queues to consume from
  op_queue_t *op_queues;
  uint32_t op_queue_total_count; // Total count of op queues
} worker_config_t;

typedef struct worker_s {
  worker_config_t config;
  uv_thread_t thread;
  // we keep entity maps per thread because each thread is entity-scoped.
  // using 2 maps because we accept both str and int external entity ids.
  khash_t(str_u32) * str_to_entity_id;
  khash_t(i64_u32) * int_to_entity_id;
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