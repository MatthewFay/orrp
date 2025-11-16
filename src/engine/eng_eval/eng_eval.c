#include "eng_eval.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/consumer/consumer.h"
#include "engine/consumer/consumer_cache.h"
#include "engine/consumer/consumer_cache_internal.h"
#include "engine/container/container.h"
#include "engine/container/container_types.h"
#include "engine/eng_key_format/eng_key_format.h"
#include "engine/routing/routing.h"
#include "khash.h"
#include "lmdb.h"
#include "query/ast.h"
#include "uthash.h"
#include <stdatomic.h>
#include <string.h>

static eval_cache_entry_t *_check_eval_cache(eval_ctx_t *ctx,
                                             const char *ser_db_key) {
  if (ctx->state->cache_head == NULL) {
    return NULL;
  }
  eval_cache_entry_t *entry = NULL;
  HASH_FIND_STR(ctx->state->cache_head, ser_db_key, entry);
  if (!entry)
    return NULL;
  return entry;
}

static const bitmap_t *_check_consumer_cache(eval_ctx_t *ctx,
                                             const char *ser_db_key) {
  int consumer_idx =
      route_key_to_consumer(ser_db_key, ctx->config->op_queue_total_count,
                            ctx->config->op_queues_per_consumer);
  consumer_t *consumer = &ctx->config->consumers[consumer_idx];
  if (!consumer)
    return NULL;
  consumer_cache_t *consumer_cache = consumer_get_cache(consumer);
  if (!consumer_cache)
    return NULL;
  return consumer_cache_get_bm(consumer_cache, ser_db_key);
}

static eval_bitmap_t *_add_to_eval_cache(eval_ctx_t *ctx,
                                         const char *ser_db_key,
                                         const bitmap_t *bm, bool own) {
  eval_cache_entry_t *entry = NULL;
  int entry_idx = ctx->state->cache_entry_count++;
  entry = &ctx->state->cache_entries[entry_idx];
  entry->ser_db_key = strdup(ser_db_key);
  if (!entry->ser_db_key) {
    return NULL;
  }
  eval_bitmap_t *ebm = &ctx->state->cache_bitmaps[entry_idx];
  ebm->own = own;
  ebm->bm = (bitmap_t *)bm;
  HASH_ADD_KEYPTR(hh, ctx->state->cache_head, entry->ser_db_key,
                  strlen(entry->ser_db_key), entry);
  return ebm;
}

static eval_bitmap_t *_check_caches(eval_ctx_t *ctx,
                                    eng_container_db_key_t *db_key) {
  char ser_db_key[512];
  if (!db_key_into(ser_db_key, sizeof(ser_db_key), db_key))
    return NULL;
  eval_cache_entry_t *entry = _check_eval_cache(ctx, ser_db_key);
  if (entry)
    return entry->bm;
  const bitmap_t *bm = _check_consumer_cache(ctx, ser_db_key);

  if (bm) {
    return _add_to_eval_cache(ctx, ser_db_key, bm, false);
  }
  return NULL;
}

// returns NULL on error, otherwise empty bitmap
static eval_bitmap_t *_get_bitmap(eval_ctx_t *ctx,
                                  eng_container_db_key_t *db_key) {
  eval_bitmap_t *ebm = _check_caches(ctx, db_key);
  if (ebm) {
    return ebm;
  }
  char ser_db_key[512];
  if (!db_key_into(ser_db_key, sizeof(ser_db_key), db_key))
    return NULL;

  db_get_result_t r;
  MDB_dbi dbi;
  if (!container_get_db_handle(ctx->config->container, db_key, &dbi)) {
    return NULL;
  }
  MDB_txn *txn = db_key->dc_type == CONTAINER_TYPE_SYSTEM
                     ? ctx->config->sys_txn
                     : ctx->config->user_txn;
  if (!db_get(dbi, txn, &db_key->db_key, &r)) {
    return NULL;
  }
  if (r.status == DB_GET_OK) {
    bitmap_t *bm = bitmap_deserialize(r.value, r.value_len);
    db_get_result_clear(&r);
    if (!bm) {
      return NULL;
    }
    return _add_to_eval_cache(ctx, ser_db_key, bm, true);
  }
  bitmap_t *empty = bitmap_create();
  return _add_to_eval_cache(ctx, ser_db_key, empty, true);
}

static eval_bitmap_t *_store_intermediate_bitmap(eval_ctx_t *ctx, bitmap_t *bm,
                                                 bool own) {

  eval_bitmap_t *ebm =
      &ctx->state
           ->intermediate_bitmaps[ctx->state->intermediate_bitmaps_count++];
  ebm->bm = bm;
  ebm->own = own;
  return ebm;
}

static eval_bitmap_t *_tag(ast_node_t *tag_node, eval_ctx_t *ctx,
                           eng_eval_result_t *result) {
  // get bitmap for tag with event ids:
  //      The Event Index:
  //      Key: The tag (e.g., `loc:ca`)
  //      Value: A Roaring Bitmap of all local `event_id`s that have this tag
  // MDB_dbi inverted_event_index_db;

  // TODO: look into this: Why can't we pre-compute this at write time?
  // I don't want to do a loop for a million events at query time
  // convert into bitmap with entity ids associated with event ids (loop)
  //. // Event-to-Entity Map
  // Key: The local `event_id` (`uint32_t`)
  // Value: The global `entity_id` (`uint32_t`) associated with the event
  // MDB_dbi event_to_entity_db;
}

static eval_bitmap_t *_comp(ast_node_t *comp_node, eval_ctx_t *ctx,
                            eng_eval_result_t *result) {}

static eval_bitmap_t *_not(eval_bitmap_t *operand, eval_ctx_t *ctx,
                           eng_eval_result_t *result) {
  eng_container_db_key_t db_key;
  db_key_t key;
  eval_bitmap_t *ebm = NULL;
  db_key.container_name = ctx->config->container->name;
  db_key.user_db_type = USER_DB_METADATA;
  db_key.dc_type = CONTAINER_TYPE_USER;
  key.type = DB_KEY_STRING;
  key.key.s = USR_ENTITIES_KEY;
  ebm = _get_bitmap(ctx, &db_key);
  if (!ebm) {
    return NULL;
  }
  // TODO: optimizations around ownership and inplace vs copy operations
  bitmap_t *r = bitmap_not(operand->bm, ebm->bm);
  if (!r) {
    return NULL;
  }
  return _store_intermediate_bitmap(ctx, r, true);
}

static eval_bitmap_t *_and(eval_bitmap_t *left_operand,
                           eval_bitmap_t *right_operand, eval_ctx_t *ctx,
                           eng_eval_result_t *result) {}

static eval_bitmap_t *_or(eval_bitmap_t *left_operand,
                          eval_bitmap_t *right_operand, eval_ctx_t *ctx,
                          eng_eval_result_t *result) {}

static eval_bitmap_t *_eval(ast_node_t *node, eval_ctx_t *ctx,
                            eng_eval_result_t *result) {
  if (!node) {
    result->err_msg = "Invalid node";
    return NULL;
  }
  eval_bitmap_t *operand1 = NULL;
  eval_bitmap_t *operand2 = NULL;
  switch (node->type) {
  case NOT_NODE:
    operand1 = _eval(node->not_op.operand, ctx, result);
    if (!operand1) {
      return NULL;
    }
    return _not(operand1, ctx, result);
  case LOGICAL_NODE:
    operand1 = _eval(node->logical.left_operand, ctx, result);
    if (!operand1)
      return NULL;
    operand2 = _eval(node->logical.right_operand, ctx, result);
    if (!operand2)
      return NULL;

    if (node->logical.op == LOGIC_NODE_AND) {
      return _and(operand1, operand2, ctx, result);
    }
    return _or(operand1, operand2, ctx, result);
  case TAG_NODE:
    return _tag(node, ctx, result);
  case COMPARISON_NODE:
    return _comp(node, ctx, result);
  default:
    result->err_msg = "Invalid node type";
    return NULL;
  }
}

static void _cleanup_intermediate(eval_state_t *state,
                                  eng_eval_result_t *result) {
  for (uint i = 0; i < state->intermediate_bitmaps_count; i++) {
    if (state->intermediate_bitmaps[i].own &&
        state->intermediate_bitmaps[i].bm != result->entities) {
      bitmap_free(state->intermediate_bitmaps[i].bm);
    }
  }
  state->intermediate_bitmaps_count = 0;
}

eng_eval_result_t eng_eval_resolve_exp_to_entities(ast_node_t *exp,
                                                   eval_ctx_t *ctx) {
  if (!exp || !ctx || !ctx->config || !ctx->config->consumers ||
      !ctx->config->container || !ctx->config->sys_txn ||
      !ctx->config->user_txn || !ctx->state) {
    return (eng_eval_result_t){.success = false, .err_msg = "Invalid args"};
  }

  eng_eval_result_t result = {0};

  eval_bitmap_t *ebm = _eval(exp, ctx, &result);

  if (ebm) {
    result.success = true;
    result.entities = ebm->bm;
  } else if (!result.err_msg)
    result.err_msg = "Unknown error";

  _cleanup_intermediate(ctx->state, &result);

  return result;
}

void eng_eval_cleanup_state(eval_state_t *state) {
  if (!state)
    return;
  eval_cache_entry_t *entry, *tmp = NULL;

  HASH_ITER(hh, state->cache_head, entry, tmp) {
    HASH_DEL(state->cache_head, entry);
    if (entry->bm->own) {
      bitmap_free(entry->bm->bm);
    }
    free(entry->ser_db_key);
  }
  state->cache_entry_count = 0;
}