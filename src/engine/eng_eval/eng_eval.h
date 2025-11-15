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
  bitmap_t *entities;
} eng_eval_result_t;

typedef struct eval_bitmap_s {
  bitmap_t *bm;
  // if true, we own the bitmap (can mutate)
  bool own;
} eval_bitmap_t;

typedef struct eval_cache_entry_s {
  UT_hash_handle hh;
  char *ser_db_key;
  eval_bitmap_t *bm;
} eval_cache_entry_t;

// Read-only input configuration
typedef struct eval_config_s {
  eng_container_t *container;
  MDB_txn *user_txn;
  MDB_txn *sys_txn;
  consumer_t *consumers;
  uint32_t op_queue_total_count;
  uint32_t op_queues_per_consumer;
} eval_config_t;

typedef struct eval_state_s {
  eval_cache_entry_t *cache_head;

  eval_cache_entry_t cache_entries[128];
  eval_bitmap_t cache_bitmaps[128];
  unsigned int cache_entry_count;

  eval_bitmap_t intermediate_bitmaps[128];
  unsigned int intermediate_bitmaps_count;
} eval_state_t;

typedef struct eval_ctx_s {
  const eval_config_t *config; // Immutable
  eval_state_t *state;         // Mutable
} eval_ctx_t;

eng_eval_result_t eng_eval_resolve_exp_to_entities(ast_node_t *exp,
                                                   eval_ctx_t *ctx);

// Call this when done with evaluations
void eng_eval_cleanup_state(eval_state_t *state);