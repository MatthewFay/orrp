#include "cache_ebr.h"
#include "cache_entry.h"
#include "cache_queue_consumer.h"
#include "cache_queue_msg.h"
#include "cache_shard.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/container.h"
#include "engine/dc_cache.h"
#include "engine/engine_writer/engine_writer.h"
#include "lmdb.h"
#include "uthash.h"
#include "uv.h"
#include "uv/unix.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define MAX_BATCH_SIZE_PER_SHARD 128
#define RECLAIM_BATCH_SIZE 100

typedef struct queue_msg_batch_entry_s {
  bm_cache_queue_msg_t *msg;
  struct queue_msg_batch_entry_s *next;
} queue_msg_batch_entry_t;

typedef struct queue_msg_batch_key_s {
  UT_hash_handle hh;
  const char *cache_key; // hash key
  queue_msg_batch_entry_t *head;
  queue_msg_batch_entry_t *tail;

  // Can have same container on multiple shards
  bm_cache_shard_t *shard;
} queue_msg_batch_key_t;

typedef struct queue_msg_batch_s {
  UT_hash_handle hh;
  const char *container_name;  // outer hash key
  queue_msg_batch_key_t *keys; // inner hash table
} queue_msg_batch_t;

static bm_cache_entry_t *_get_or_Create_cache_entry(eng_container_t *dc,
                                                    queue_msg_batch_key_t *key,
                                                    bm_cache_queue_msg_t *msg,
                                                    MDB_dbi db, MDB_txn *txn,
                                                    bool *was_cached_out) {
  *was_cached_out = false;

  db_get_result_t r;
  bm_cache_entry_t *cached_entry = NULL;
  if (!shard_get_entry(key->shard, key->cache_key, &cached_entry)) {
    return false;
  }
  if (cached_entry) {
    *was_cached_out = true;
    return cached_entry;
  }
  if (!db_get(db, txn, &msg->db_key, &r)) {
    return NULL;
  }
  bm_cache_entry_t *new_cache_entry =
      bm_cache_create_entry(msg->db_type, msg->db_key, dc, key->cache_key);
  if (!new_cache_entry) {
    return NULL;
  }
  if (r.status == DB_GET_OK) {
    atomic_store(&new_cache_entry->bitmap, r.value);
    return new_cache_entry;
  }
  bitmap_t *new_BITMAP = bitmap_create();
  if (!new_BITMAP) {
    // free bm_cache_entry_t
    return NULL;
  }
  atomic_store(&new_cache_entry->bitmap, new_BITMAP);
  return new_cache_entry;
}

// Process all messages for a container
static bool _process_cache_msgs(eng_container_t *dc,
                                queue_msg_batch_key_t *keys, MDB_txn *txn) {
  queue_msg_batch_key_t *key, *tmp;
  bm_cache_queue_msg_t *msg;

  HASH_ITER(hh, keys, key, tmp) {
    bool was_cached;
    queue_msg_batch_entry_t *b_entry = key->head;
    MDB_dbi db;
    if (!eng_container_get_user_db(dc, b_entry->msg->db_type, &db)) {
      return false;
    }
    bm_cache_entry_t *cache_entry =
        _get_or_Create_cache_entry(dc, key, b_entry->msg, db, txn, &was_cached);
    bitmap_t *old_bm;
    bitmap_t *bm = atomic_load(&cache_entry->bitmap);
    if (was_cached) {
      // if cached, create a copy because other threads could be using it
      old_bm = bm;
      bm = bitmap_copy(bm);
    }
    bool dirty = false;
    while (b_entry) {
      msg = b_entry->msg;

      switch (msg->op_type) {
      case BM_CACHE_BITMAP:
        // Will be cached, no work to do
        break;
      case BM_CACHE_ADD_VALUE:
        bitmap_add(bm, msg->value);
        dirty = true;
        break;
      default:
        break;
      }
      b_entry = b_entry->next;
    }
    if (dirty) {
      bm->version++;
      // store the copied bitmap in cache entry
      atomic_store(&cache_entry->bitmap, bm);
      if (was_cached) {
        bm_cache_ebr_retire(&old_bm->epoch_entry);
      }
    } else {
      // destroy the copy, not needed
      bitmap_free(bm);
    }

    if (was_cached) {
      shard_lru_move_to_front(key->shard, cache_entry, dirty);
    } else if (!shard_add_entry(key->shard, key->cache_key, cache_entry,
                                dirty)) {
      // TODO: handle error
    }
  }

  return true;
}

// returns number of msgs processed, or -1 on error
// TODO: return detail results (processed vs unprocessd etc)
static int _process_batch(queue_msg_batch_t *batch) {
  if (!batch || !batch->container_name || !batch->keys)
    return -1;
  int n = 0;
  queue_msg_batch_key_t *keys = batch->keys;

  eng_container_t *dc = eng_dc_cache_get(batch->container_name);
  if (!dc) {
    return -1;
  }

  MDB_txn *txn = db_create_txn(dc->env, true);
  if (!txn) {
    eng_dc_cache_release_container(dc);
    return -1;
  }

  if (!_process_cache_msgs(dc, keys, txn)) {
    db_abort_txn(txn);
    eng_dc_cache_release_container(dc);
    return -1;
  }

  db_abort_txn(txn);
  eng_dc_cache_release_container(dc);

  return n;
}

static void _process_batches(queue_msg_batch_t *batch_hash) {
  queue_msg_batch_t *batch, *tmp;
  HASH_ITER(hh, batch_hash, batch, tmp) { _process_batch(batch); }
}

static queue_msg_batch_t *_create_batch(const char *container_name) {
  queue_msg_batch_t *batch = calloc(1, sizeof(queue_msg_batch_t));
  if (!batch) {
    return NULL;
  }
  batch->container_name = container_name;
  batch->keys = NULL;
  return batch;
}

static queue_msg_batch_entry_t *_create_batch_entry(bm_cache_queue_msg_t *msg) {
  queue_msg_batch_entry_t *m = calloc(1, sizeof(queue_msg_batch_entry_t));
  if (!m) {
    return NULL;
  }
  m->msg = msg;
  m->next = NULL;
  return m;
}

static bool _add_msg_to_batch(queue_msg_batch_t *batch,
                              bm_cache_queue_msg_t *msg,
                              bm_cache_shard_t *shard) {
  bool new_key = false;
  queue_msg_batch_key_t *key = NULL;
  HASH_FIND_STR(batch->keys, msg->key, key);
  if (!key) {
    new_key = true;
    key = calloc(1, sizeof(queue_msg_batch_key_t));
    if (!key) {
      return false;
    }
    key->shard = shard;
    key->cache_key = msg->key;
    key->head = NULL;
    key->tail = NULL;
    HASH_ADD_KEYPTR(hh, batch->keys, msg->key, strlen(msg->key), key);
  }

  queue_msg_batch_entry_t *m = _create_batch_entry(msg);
  if (!m) {
    if (new_key)
      free(key);
    return false;
  }

  if (!key->head) {
    key->head = m;
    key->tail = m;
    return true;
  }
  key->tail->next = m;
  key->tail = m;

  return true;
}

static void _free_batch_hash(queue_msg_batch_t *batch_hash) {
  if (!batch_hash)
    return;
  queue_msg_batch_t *b, *tmp;
  queue_msg_batch_key_t *k, *tmp_k;
  queue_msg_batch_entry_t *b_entry, *b_entry_tmp;
  HASH_ITER(hh, batch_hash, b, tmp) {
    queue_msg_batch_key_t *keys = b->keys;
    HASH_ITER(hh, keys, k, tmp_k) {
      b_entry = k->head;
      while (b_entry) {
        b_entry_tmp = b_entry->next;
        free(b_entry);
        b_entry = b_entry_tmp;
      }
      HASH_DEL(keys, k);
      free(k);
    }
    HASH_DEL(batch_hash, b);
    free(b);
  }
}

// TODO: return list of msgs unable to process
static bool _batch_by_container(const bm_cache_consumer_config_t *config,
                                queue_msg_batch_t **batch_hash_out,
                                bool *batched_any_out) {
  bm_cache_queue_msg_t *msg;
  queue_msg_batch_t *batch = NULL;

  for (uint32_t i = 0; i < config->shard_count; i++) {
    uint32_t shard_idx = config->shard_start + i;
    bm_cache_shard_t *shard = &config->shards[shard_idx];

    for (int j = 0; j < MAX_BATCH_SIZE_PER_SHARD; j++) {
      if (!shard_dequeue_msg(shard, &msg)) {
        break; // No more messages in this shard
      }
      batch = NULL;
      HASH_FIND_STR(*batch_hash_out, msg->container_name, batch);
      if (batch) {
        if (!_add_msg_to_batch(batch, msg, shard)) {
          _free_batch_hash(*batch_hash_out);
          // TODO: dont fail everything if unable to process a single msg
          return false;
        }
      } else {
        batch = _create_batch(msg->container_name);
        if (!batch || !_add_msg_to_batch(batch, msg, shard)) {
          _free_batch_hash(*batch_hash_out);
          return false;
        }
        HASH_ADD_KEYPTR(hh, *batch_hash_out, batch->container_name,
                        strlen(batch->container_name), batch);
      }
      *batched_any_out = true;
    }
  }
  return true;
}

static void _check_flush(const bm_cache_consumer_config_t *config) {
  for (uint32_t i = 0; i < config->shard_count; i++) {
    uint32_t shard_idx = config->shard_start + i;
    bm_cache_shard_t *shard = &config->shards[shard_idx];
    bm_cache_dirty_snapshot_t *snap = shard_get_dirty_snapshot(shard);
    if (!snap)
      continue;
    if (!eng_writer_queue_up_bm_dirty_snapshot(config->writer, snap)) {
      shard_free_dirty_snapshot(snap);
    }
    shard_clear_dirty_list(shard);
  }
}

// TODO: store queue msgs unable to process (DLQ)
static void _consumer_thread_func(void *arg) {
  bm_cache_ebr_reg();

  bm_cache_consumer_t *consumer = (bm_cache_consumer_t *)arg;
  const bm_cache_consumer_config_t *config = &consumer->config;
  queue_msg_batch_t *batch_hash = NULL;
  bool batched_any = false;
  uint32_t cycle = 0;

  // TODO: weigh sleep approach vs. libuv notify approach (w/ focus on
  // performance)
  while (!consumer->should_stop) {
    batched_any = false;
    cycle++;

    // KIS for now ( need err handling)
    _batch_by_container(config, &batch_hash, &batched_any);

    if (batched_any) {
      _process_batches(batch_hash);
      _free_batch_hash(batch_hash);
      batch_hash = NULL;
    } else {
      // If no work, yield briefly to avoid spinning
      // TODO: change to spin + backoff
      uv_sleep(1);
    }

    if (cycle == config->flush_every_n) {
      cycle = 0;
      _check_flush(config);
      if (bitmap_cache_thread_epoch_record.n_pending >= RECLAIM_BATCH_SIZE) {
        bm_cache_reclamation();
      }
    }
  }
}

bool bm_cache_consumer_start(bm_cache_consumer_t *consumer,
                             const bm_cache_consumer_config_t *config) {
  consumer->config = *config;
  consumer->should_stop = false;
  consumer->messages_processed = 0;

  if (uv_thread_create(&consumer->thread, _consumer_thread_func, consumer) !=
      0) {
    return false;
  }
  return true;
}

bool bm_cache_consumer_stop(bm_cache_consumer_t *consumer) {
  consumer->should_stop = true;
  if (uv_thread_join(&consumer->thread) != 0) {
    return false;
  }
  return true;
}