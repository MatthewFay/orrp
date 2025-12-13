#pragma once
#include "core/bitmaps.h"
#include "engine/consumer/consumer.h"
#include "engine/container/container_types.h"
#include "lmdb.h"
#include "query/ast.h"
#include "uthash.h"

typedef struct eng_eval_result_s {
  bool success;
  const char *err_msg;
  bitmap_t *events;
} eng_eval_result_t;

typedef struct eval_bitmap_s {
  bitmap_t *bm;
  bool own; // If true, we own and can mutate/free
} eval_bitmap_t;

typedef struct eval_cache_entry_s {
  UT_hash_handle hh;
  char *ser_db_key;
  eval_bitmap_t *bm;
} eval_cache_entry_t;

// Immutable config
typedef struct eval_config_s {
  eng_container_t *container;
  MDB_txn *user_txn;
  MDB_txn *sys_txn;
  consumer_t *consumers;
  uint32_t op_queue_total_count;
  uint32_t op_queues_per_consumer;
} eval_config_t;

// Mutable state
typedef struct eval_state_s {
  eval_cache_entry_t *cache_head;

  eval_cache_entry_t cache_entries[128];
  eval_bitmap_t cache_bitmaps[128];
  unsigned int cache_entry_count;

  eval_bitmap_t intermediate_bitmaps[128];
  unsigned int intermediate_bitmaps_count;

  unsigned int max_event_id;
  bool max_event_id_loaded;
} eval_state_t;

typedef struct eval_ctx_s {
  const eval_config_t *config;
  eval_state_t *state;
} eval_ctx_t;

// Ownership of bitmap result transfers to caller
eng_eval_result_t eng_eval_resolve_exp_to_events(ast_node_t *exp,
                                                 eval_ctx_t *ctx);

// Call this when done with evaluations
void eng_eval_cleanup_state(eval_state_t *state);