#include "consumer.h"
#include "consumer_cache_ebr.h"
#include "consumer_cache_entry.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/consumer/consumer_cache_internal.h"
#include "engine/container/container.h"
#include "engine/dc_cache/dc_cache.h"
#include "engine/engine_writer/engine_writer.h"
#include "engine/op/op.h"
#include "engine/op_queue/op_queue_msg.h"
#include "lmdb.h"
#include "sched.h"
#include "uthash.h"
#include "uv.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// spin count before sleeping
#define WORKER_SPIN_LIMIT 100
#define WORKER_MAX_SLEEP_MS 64

#define MAX_BATCH_SIZE_PER_OP_Queue 128
#define MIN_RECLAIM_BATCH_SIZE 100
#define MAX_WRITER_ENQUEUE_ATTEMPTS 3

typedef struct op_queue_msg_batch_entry_s {
  op_queue_msg_t *msg;
  struct op_queue_msg_batch_entry_s *next;
} op_queue_msg_batch_entry_t;

typedef struct {
  UT_hash_handle hh;
  const char *ser_db_key; // hash key
  op_queue_msg_batch_entry_t *head;
  op_queue_msg_batch_entry_t *tail;
} op_queue_msg_batch_key_t;

typedef struct {
  UT_hash_handle hh;
  const char *container_name;     // outer hash key
  op_queue_msg_batch_key_t *keys; // inner hash table
} op_queue_msg_batch_t;

static consumer_cache_entry_t *
_get_or_Create_cache_entry(consumer_cache_t *cache,
                           op_queue_msg_batch_key_t *key, op_queue_msg_t *msg,
                           MDB_dbi db, MDB_txn *txn, bool *was_cached_out) {
  *was_cached_out = false;

  db_get_result_t r;
  consumer_cache_entry_t *cached_entry = NULL;
  if (!consumer_cache_get_entry(cache, key->ser_db_key, &cached_entry)) {
    return false;
  }
  if (cached_entry) {
    *was_cached_out = true;
    return cached_entry;
  }
  if (!db_get(db, txn, &msg->op->db_key.db_key, &r)) {
    return NULL;
  }
  consumer_cache_entry_t *new_cache_entry =
      consumer_cache_create_entry(&msg->op->db_key, key->ser_db_key);
  if (!new_cache_entry) {
    return NULL;
  }
  if (r.status == DB_GET_OK) {
    bitmap_t *db_bm = bitmap_deserialize(r.value, r.value_len);
    atomic_store(&new_cache_entry->bitmap, db_bm);
    return new_cache_entry;
  }
  bitmap_t *new_BITMAP = bitmap_create();
  if (!new_BITMAP) {
    consumer_cache_free_entry(new_cache_entry);
    return NULL;
  }
  atomic_store(&new_cache_entry->bitmap, new_BITMAP);
  return new_cache_entry;
}

// Process all messages for a container
static bool _process_cache_msgs(consumer_t *consumer, eng_container_t *dc,
                                op_queue_msg_batch_key_t *keys, MDB_txn *txn) {
  op_queue_msg_batch_key_t *key, *tmp;
  op_queue_msg_t *msg;

  HASH_ITER(hh, keys, key, tmp) {
    bool was_cached;
    op_queue_msg_batch_entry_t *b_entry = key->head;
    MDB_dbi db;
    // what about sys containers?
    if (!eng_container_get_db_handle(dc, b_entry->msg->op->db_key.user_db_type,
                                     &db)) {
      return false;
    }
    consumer_cache_entry_t *cache_entry = _get_or_Create_cache_entry(
        &consumer->cache, key, b_entry->msg, db, txn, &was_cached);
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

      switch (msg->op->op_type) {
      case BM_CACHE:
        // Will be cached, no work to do
        break;
      case BM_ADD_VALUE:
        bitmap_add(bm, msg->op->data.int32_value);
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
        consumer_cache_ebr_retire(&consumer->consumer_cache_thread_epoch_record,
                                  &old_bm->epoch_entry);
      }
    } else {
      // destroy the copy, not needed
      bitmap_free(bm);
    }

    if (!was_cached &&
        !consumer_cache_add_entry(&consumer->cache, key->ser_db_key,
                                  cache_entry, dirty)) {
      // TODO: handle error
    }
  }

  return true;
}

// returns number of msgs processed, or -1 on error
// TODO: return detail results (processed vs unprocessd etc)
static int _process_batch(op_queue_msg_batch_t *batch) {
  if (!batch || !batch->container_name || !batch->keys)
    return -1;
  int n = 0;
  op_queue_msg_batch_key_t *keys = batch->keys;

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

static void _process_batches(op_queue_msg_batch_t *batch_hash) {
  op_queue_msg_batch_t *batch, *tmp;
  HASH_ITER(hh, batch_hash, batch, tmp) { _process_batch(batch); }
}

static op_queue_msg_batch_t *_create_batch(const char *container_name) {
  op_queue_msg_batch_t *batch = calloc(1, sizeof(op_queue_msg_batch_t));
  if (!batch) {
    return NULL;
  }
  batch->container_name = container_name;
  batch->keys = NULL;
  return batch;
}

static op_queue_msg_batch_entry_t *_create_batch_entry(op_queue_msg_t *msg) {
  op_queue_msg_batch_entry_t *m = calloc(1, sizeof(op_queue_msg_batch_entry_t));
  if (!m) {
    return NULL;
  }
  m->msg = msg;
  m->next = NULL;
  return m;
}

static bool _add_msg_to_batch(op_queue_msg_batch_t *batch, op_queue_msg_t *msg,
                              shard_t *shard) {
  bool new_key = false;
  op_queue_msg_batch_key_t *key = NULL;
  HASH_FIND_STR(batch->keys, msg->key, key);
  if (!key) {
    new_key = true;
    key = calloc(1, sizeof(op_queue_msg_batch_key_t));
    if (!key) {
      return false;
    }
    key->shard = shard;
    key->ser_db_key = msg->key;
    key->head = NULL;
    key->tail = NULL;
    HASH_ADD_KEYPTR(hh, batch->keys, msg->key, strlen(msg->key), key);
  }

  op_queue_msg_batch_entry_t *m = _create_batch_entry(msg);
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

static void _free_batch_hash(op_queue_msg_batch_t *batch_hash) {
  if (!batch_hash)
    return;
  op_queue_msg_batch_t *b, *tmp;
  op_queue_msg_batch_key_t *k, *tmp_k;
  op_queue_msg_batch_entry_t *b_entry, *b_entry_tmp;
  HASH_ITER(hh, batch_hash, b, tmp) {
    op_queue_msg_batch_key_t *keys = b->keys;
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
static bool _batch_by_container(const consumer_config_t *config,
                                op_queue_msg_batch_t **batch_hash_out,
                                bool *batched_any_out) {
  op_queue_msg_t *msg;
  op_queue_msg_batch_t *batch = NULL;

  for (uint32_t i = 0; i < config->shard_count; i++) {
    uint32_t shard_idx = config->shard_start + i;
    shard_t *shard = &config->shards[shard_idx];

    for (int j = 0; j < MAX_BATCH_SIZE_PER_OP_Queue; j++) {
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

// void consumer_flush(consumer_t *c) {
//   eng_writer_dirty_entry_t entries[100];
//   // populate from cache...
//   eng_writer_submit(writer, entries, count);
// }

static void _check_flush(const consumer_config_t *config) {
  for (uint32_t i = 0; i < config->shard_count; i++) {
    uint32_t shard_idx = config->shard_start + i;
    shard_t *shard = &config->shards[shard_idx];
    dirty_snapshot_t *snap = shard_get_dirty_snapshot(shard);
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
  consumer_t *consumer = (consumer_t *)arg;
  const consumer_config_t *config = &consumer->config;

  consumer_cache_init(&consumer->cache, &consumer->config);
  ebr_reg();

  op_queue_msg_batch_t *batch_hash = NULL;
  bool batched_any = false;
  uint32_t cycle = 0;
  int backoff = 1;
  int spin_count = 0;

  while (!consumer->should_stop) {
    if (_do_work(worker) > 0) {
      backoff = 1;
      spin_count = 0;
    } else {
      if (spin_count < WORKER_SPIN_LIMIT) {
        sched_yield();
        spin_count++;
      } else {
        uv_sleep(backoff);
        backoff =
            backoff < WORKER_MAX_SLEEP_MS ? backoff * 2 : WORKER_MAX_SLEEP_MS;
      }
    }
  }

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
        reclamation();
      }
    }
  }
}

bool consumer_start(consumer_t *consumer, const consumer_config_t *config) {
  consumer->config = *config;
  consumer->should_stop = false;
  consumer->messages_processed = 0;

  if (uv_thread_create(&consumer->thread, _consumer_thread_func, consumer) !=
      0) {
    return false;
  }
  return true;
}

bool consumer_stop(consumer_t *consumer) {
  consumer->should_stop = true;
  if (uv_thread_join(&consumer->thread) != 0) {
    return false;
  }
  return true;
}