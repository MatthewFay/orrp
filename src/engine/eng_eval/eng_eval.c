#include "eng_eval.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/consumer/consumer.h"
#include "engine/consumer/consumer_cache.h"
#include "engine/container/container.h"
#include "engine/container/container_types.h"
#include "engine/eng_key_format/eng_key_format.h"
#include "engine/routing/routing.h"
#include "lmdb.h"
#include "query/ast.h"
#include "uthash.h"
#include <stdint.h>
#include <string.h>

#define MAX_EVAL_STACK 128

// --- Internal Helpers ---

static eval_bitmap_t *_store_intermediate_bitmap(eval_ctx_t *ctx, bitmap_t *bm,
                                                 bool own) {
  if (ctx->state->intermediate_bitmaps_count >= MAX_EVAL_STACK) {
    // Stack overflow protection: prevent leaks
    if (own && bm)
      bitmap_free(bm);
    return NULL;
  }
  eval_bitmap_t *ebm =
      &ctx->state
           ->intermediate_bitmaps[ctx->state->intermediate_bitmaps_count++];
  ebm->bm = bm;
  ebm->own = own;
  return ebm;
}

static eval_cache_entry_t *_check_eval_local_cache(eval_ctx_t *ctx,
                                                   const char *ser_db_key) {
  if (ctx->state->cache_head == NULL)
    return NULL;
  eval_cache_entry_t *entry = NULL;
  HASH_FIND_STR(ctx->state->cache_head, ser_db_key, entry);
  return entry;
}

static eval_bitmap_t *_add_to_eval_local_cache(eval_ctx_t *ctx,
                                               const char *ser_db_key,
                                               bitmap_t *bm, bool own) {
  if (ctx->state->cache_entry_count >= MAX_EVAL_STACK) {
    return NULL;
  }
  int entry_idx = ctx->state->cache_entry_count++;
  eval_cache_entry_t *entry = &ctx->state->cache_entries[entry_idx];

  entry->ser_db_key = strdup(ser_db_key);
  if (!entry->ser_db_key)
    return NULL;

  eval_bitmap_t *ebm = &ctx->state->cache_bitmaps[entry_idx];
  ebm->own = own;
  ebm->bm = bm;

  entry->bm = ebm;

  HASH_ADD_KEYPTR(hh, ctx->state->cache_head, entry->ser_db_key,
                  strlen(entry->ser_db_key), entry);
  return ebm;
}

// --- Data Fetching ---

static eval_bitmap_t *_fetch_bitmap_data(eval_ctx_t *ctx,
                                         eng_container_db_key_t *db_key) {
  char ser_db_key[512];
  if (!db_key_into(ser_db_key, sizeof(ser_db_key), db_key)) {
    return NULL;
  }

  // 1. Check Local Eval Cache
  eval_cache_entry_t *entry = _check_eval_local_cache(ctx, ser_db_key);
  if (entry) {
    return entry->bm;
  }

  // 2. Check Consumer Cache
  int consumer_idx =
      route_key_to_consumer(ser_db_key, ctx->config->op_queue_total_count,
                            ctx->config->op_queues_per_consumer);

  consumer_t *consumer = &ctx->config->consumers[consumer_idx];
  if (consumer) {
    consumer_cache_t *cc = consumer_get_cache(consumer);
    if (cc) {
      const bitmap_t *cached_bm = consumer_cache_get_bm(cc, ser_db_key);
      if (cached_bm) {
        // We do not own this; it belongs to the cache
        return _add_to_eval_local_cache(ctx, ser_db_key, (bitmap_t *)cached_bm,
                                        false);
      }
    }
  }

  // 3. Check LMDB
  db_get_result_t r;
  MDB_dbi dbi;

  if (!container_get_db_handle(ctx->config->container, db_key, &dbi)) {
    return NULL;
  }

  MDB_txn *txn = (db_key->dc_type == CONTAINER_TYPE_SYSTEM)
                     ? ctx->config->sys_txn
                     : ctx->config->user_txn;

  if (!db_get(dbi, txn, &db_key->db_key, &r)) {
    return NULL;
  }

  bitmap_t *bm = NULL;
  if (r.status == DB_GET_OK) {
    bm = bitmap_deserialize(r.value, r.value_len);
  } else {
    bm = bitmap_create();
  }

  if (!bm)
    return NULL;

  // ownership is true because deserialize created a new object
  return _add_to_eval_local_cache(ctx, ser_db_key, bm, true);
}

static uint32_t _get_max_event_id(eval_ctx_t *ctx) {
  if (ctx->state->max_event_id_loaded) {
    return ctx->state->max_event_id;
  }

  eng_container_db_key_t db_key;
  db_key.container_name = ctx->config->container->name;
  db_key.user_db_type = USER_DB_METADATA;
  db_key.dc_type = CONTAINER_TYPE_USER;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = USR_NEXT_EVENT_ID_KEY;

  char ser_db_key[512];
  if (!db_key_into(ser_db_key, sizeof(ser_db_key), &db_key)) {
    return 0;
  }

  uint32_t max_id = 0;

  int consumer_idx =
      route_key_to_consumer(ser_db_key, ctx->config->op_queue_total_count,
                            ctx->config->op_queues_per_consumer);

  consumer_t *consumer = &ctx->config->consumers[consumer_idx];
  if (consumer) {
    consumer_cache_t *cc = consumer_get_cache(consumer);
    max_id = cc ? consumer_cache_get_u32(cc, ser_db_key) : 0;
  }

  db_get_result_t r;
  MDB_dbi dbi;
  if (!max_id &&
      container_get_db_handle(ctx->config->container, &db_key, &dbi)) {
    if (db_get(dbi, ctx->config->user_txn, &db_key.db_key, &r) &&
        r.status == DB_GET_OK) {
      if (r.value_len == sizeof(uint32_t)) {
        memcpy(&max_id, r.value, sizeof(uint32_t));
      } else if (r.value_len == sizeof(uint64_t)) {
        uint64_t tmp;
        memcpy(&tmp, r.value, sizeof(uint64_t));
        max_id = (uint32_t)tmp;
      }
      db_get_result_clear(&r);
    }
  }

  ctx->state->max_event_id = max_id;
  ctx->state->max_event_id_loaded = true;
  return max_id;
}

static eval_bitmap_t *_tag(ast_node_t *tag_node, eval_ctx_t *ctx,
                           eng_eval_result_t *result) {
  eng_container_db_key_t db_key;
  db_key.container_name = ctx->config->container->name;
  db_key.user_db_type = USER_DB_INVERTED_EVENT_INDEX;
  db_key.dc_type = CONTAINER_TYPE_USER;
  db_key.db_key.type = DB_KEY_STRING;

  char tag_key[512];
  if (!custom_tag_into(tag_key, sizeof(tag_key), tag_node)) {
    result->err_msg = "Failed to format tag key";
    return NULL;
  }
  db_key.db_key.key.s = tag_key;

  return _fetch_bitmap_data(ctx, &db_key);
}

static eval_bitmap_t *_not(eval_bitmap_t *operand, eval_ctx_t *ctx,
                           eng_eval_result_t *result) {
  uint32_t max_event_id = _get_max_event_id(ctx);
  bitmap_t *r = bitmap_flip(operand->bm, 0, max_event_id);
  if (!r) {
    result->err_msg = "Failed to perform NOT operation";
    return NULL;
  }
  return _store_intermediate_bitmap(ctx, r, true);
}

static eval_bitmap_t *_and(eval_bitmap_t *left, eval_bitmap_t *right,
                           eval_ctx_t *ctx, eng_eval_result_t *result) {
  if (left->own) {
    // We own Left, so we can mutate it in-place
    bitmap_and_inplace(left->bm, right->bm);
    return left;
  }
  if (right->own) {
    // We own Right, so we can mutate it in-place
    bitmap_and_inplace(right->bm, left->bm);
    return right;
  }
  bitmap_t *res_bm = bitmap_and(left->bm, right->bm);
  if (!res_bm)
    return NULL;

  return _store_intermediate_bitmap(ctx, res_bm, true);
}

static eval_bitmap_t *_or(eval_bitmap_t *left, eval_bitmap_t *right,
                          eval_ctx_t *ctx, eng_eval_result_t *result) {
  if (left->own) {
    // We own Left, so we can mutate it in-place
    bitmap_or_inplace(left->bm, right->bm);
    return left;
  }
  if (right->own) {
    // We own Right, so we can mutate it in-place
    bitmap_or_inplace(right->bm, left->bm);
    return right;
  }
  bitmap_t *res_bm = bitmap_or(left->bm, right->bm);
  if (!res_bm)
    return NULL;

  return _store_intermediate_bitmap(ctx, res_bm, true);
}

static eval_bitmap_t *_eval(ast_node_t *node, eval_ctx_t *ctx,
                            eng_eval_result_t *result) {
  if (!node) {
    result->err_msg = "Invalid node";
    return NULL;
  }

  eval_bitmap_t *op1 = NULL;
  eval_bitmap_t *op2 = NULL;

  switch (node->type) {
  case AST_NOT_NODE:
    op1 = _eval(node->not_op.operand, ctx, result);
    if (!op1)
      return NULL;
    return _not(op1, ctx, result);

  case AST_LOGICAL_NODE:
    op1 = _eval(node->logical.left_operand, ctx, result);
    if (!op1)
      return NULL;

    op2 = _eval(node->logical.right_operand, ctx, result);
    if (!op2)
      return NULL;

    if (node->logical.op == AST_LOGIC_NODE_AND) {
      return _and(op1, op2, ctx, result);
    } else {
      return _or(op1, op2, ctx, result);
    }

  case AST_TAG_NODE:
    return _tag(node, ctx, result);

  case AST_COMPARISON_NODE:
    result->err_msg = "Comparisons not supported in WHERE clause";
    return NULL;

  default:
    result->err_msg = "Invalid node type";
    return NULL;
  }
}

static void _cleanup_intermediate(eval_state_t *state,
                                  eng_eval_result_t *result) {
  for (unsigned int i = 0; i < state->intermediate_bitmaps_count; i++) {
    eval_bitmap_t *ebm = &state->intermediate_bitmaps[i];

    // Only free if we own it AND it is NOT the final result returned to user
    if (ebm->own && ebm->bm != result->events) {
      bitmap_free(ebm->bm);
    }
  }
  state->intermediate_bitmaps_count = 0;
}

// --- Public API ---

eng_eval_result_t eng_eval_resolve_exp_to_events(ast_node_t *exp,
                                                 eval_ctx_t *ctx) {
  if (!exp || !ctx || !ctx->config) {
    return (eng_eval_result_t){.success = false, .err_msg = "Invalid args"};
  }

  eng_eval_result_t result = {0};

  eval_bitmap_t *ebm = _eval(exp, ctx, &result);

  if (ebm) {
    result.success = true;

    // If intermediate bitmap is owned, pass ownership to the result.
    // If it's from cache (not owned), we MUST copy it so the result owns its
    // own data.
    if (ebm->own) {
      result.events = ebm->bm;
      // Explicitly revoke ownership from the state (cache or intermediate
      // stack) so that subsequent cleanup calls do not double-free this
      // pointer.
      ebm->own = false;
    } else {
      result.events = bitmap_copy(ebm->bm);
    }
  } else {
    result.success = false;
    if (!result.err_msg)
      result.err_msg = "Evaluation failed";
  }

  _cleanup_intermediate(ctx->state, &result);

  return result;
}

void eng_eval_cleanup_state(eval_state_t *state) {
  if (!state)
    return;

  eval_cache_entry_t *entry, *tmp = NULL;

  HASH_ITER(hh, state->cache_head, entry, tmp) {
    HASH_DEL(state->cache_head, entry);
    // Free the bitmap if the cache entry owns it
    if (entry->bm->own) {
      bitmap_free(entry->bm->bm);
    }
    free(entry->ser_db_key);
  }
  state->cache_entry_count = 0;
}