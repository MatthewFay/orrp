#include "consumer.h"
#include "consumer_cache_entry.h"
#include "consumer_ebr.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/consumer/consumer_cache_internal.h"
#include "engine/container/container.h"
#include "engine/dc_cache/dc_cache.h"
#include "engine/engine_writer/engine_writer_queue.h"
#include "engine/op/op.h"
#include "engine/op_queue/op_queue.h"
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
#define CONSUMER_SPIN_LIMIT 100
#define CONSUMER_MAX_SLEEP_MS 64

#define MAX_BATCH_SIZE_PER_OP_Queue 128
#define MIN_RECLAIM_BATCH_SIZE 100
// TODO: writer enqueue re-tries
#define MAX_WRITER_ENQUEUE_ATTEMPTS 3

#define CONSUMER_CACHE_CAPACITY 65536

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
    return NULL;
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
    if (!cache_entry) {
      return false;
    }
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
        consumer_ebr_retire(&consumer->consumer_cache_thread_epoch_record,
                            &old_bm->epoch_entry);
      }
    } else {
      // destroy the copy, not needed
      bitmap_free(bm);
    }

    if (dirty) {
      consumer_cache_add_entry_to_dirty_list(&consumer->cache, cache_entry);
    }

    if (was_cached)
      continue;

    if (consumer->cache.n_entries >= CONSUMER_CACHE_CAPACITY) {
      consumer_cache_entry_t *victim =
          consumer_cache_evict_lru(&consumer->cache);
      if (victim) {
        consumer_ebr_retire(&consumer->consumer_cache_thread_epoch_record,
                            &victim->bitmap->epoch_entry);
        consumer_cache_free_entry(victim);
      }
    }

    if (!consumer_cache_add_entry(&consumer->cache, key->ser_db_key,
                                  cache_entry)) {
      // TODO: handle error
    }
  }

  return true;
}

// returns number of msgs processed, or -1 on error
// TODO: return detail results (processed vs unprocessd etc)
static int _process_batch(consumer_t *consumer, op_queue_msg_batch_t *batch) {
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

  if (!_process_cache_msgs(consumer, dc, keys, txn)) {
    db_abort_txn(txn);
    eng_dc_cache_release_container(dc);
    return -1;
  }

  db_abort_txn(txn);
  eng_dc_cache_release_container(dc);

  return n;
}

static void _process_batches(consumer_t *consumer,
                             op_queue_msg_batch_t *batch_hash) {
  op_queue_msg_batch_t *batch, *tmp;
  HASH_ITER(hh, batch_hash, batch, tmp) { _process_batch(consumer, batch); }
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

static bool _add_msg_to_batch(op_queue_msg_batch_t *batch,
                              op_queue_msg_t *msg) {
  bool new_key = false;
  op_queue_msg_batch_key_t *key = NULL;
  HASH_FIND_STR(batch->keys, msg->ser_db_key, key);
  if (!key) {
    new_key = true;
    key = calloc(1, sizeof(op_queue_msg_batch_key_t));
    if (!key) {
      return false;
    }
    key->ser_db_key = msg->ser_db_key;
    key->head = NULL;
    key->tail = NULL;
    HASH_ADD_KEYPTR(hh, batch->keys, msg->ser_db_key, strlen(msg->ser_db_key),
                    key);
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

// Also free's consumed `op` msgs
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
        op_queue_msg_free(b_entry->msg);
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
static bool _batch_by_container(const consumer_t *consumer,
                                op_queue_msg_batch_t **batch_hash_out,
                                bool *batched_any_out) {
  op_queue_msg_t *msg;
  op_queue_msg_batch_t *batch = NULL;

  for (uint32_t i = 0; i < consumer->config.op_queue_consume_count; i++) {
    uint32_t op_queue_idx = consumer->config.op_queue_consume_start + i;
    op_queue_t *queue = &consumer->config.op_queues[op_queue_idx];

    for (int j = 0; j < MAX_BATCH_SIZE_PER_OP_Queue; j++) {
      if (!op_queue_dequeue(queue, &msg)) {
        break; // No more messages in this shard
      }
      batch = NULL;
      HASH_FIND_STR(*batch_hash_out, msg->op->db_key.container_name, batch);
      if (batch) {
        if (!_add_msg_to_batch(batch, msg)) {
          _free_batch_hash(*batch_hash_out);
          // TODO: dont fail everything if unable to process a single msg
          return false;
        }
      } else {
        batch = _create_batch(msg->op->db_key.container_name);
        if (!batch || !_add_msg_to_batch(batch, msg)) {
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

static void _flush(consumer_t *c) {
  uint32_t count = c->cache.num_dirty_entries;
  if (count < 1 || !c->cache.dirty_head) {
    return;
  }
  eng_writer_msg_t *msg = malloc(sizeof(eng_writer_msg_t));
  if (!msg)
    return;
  msg->entries = malloc(sizeof(eng_writer_entry_t) * count);
  if (!msg->entries) {
    free(msg);
    return;
  }
  msg->count = 0;
  for (consumer_cache_entry_t *cache_entry = c->cache.dirty_head; cache_entry;
       cache_entry = cache_entry->dirty_next) {
    bitmap_t *bm = atomic_load(&cache_entry->bitmap);
    if (!bm) {
      continue;
    }
    eng_writer_entry_t *entry = &msg->entries[msg->count];
    entry->bitmap_copy = bitmap_copy(bm);
    if (!entry->bitmap_copy) {
      continue;
    }
    entry->flush_version_ptr = &cache_entry->flush_version;
    entry->db_key = cache_entry->db_key;
    entry->db_key.container_name = strdup(cache_entry->db_key.container_name);
    if (!entry->db_key.container_name) {
      bitmap_free(entry->bitmap_copy);
      continue;
    }
    if (cache_entry->db_key.db_key.type == DB_KEY_STRING) {
      entry->db_key.db_key.key.s = strdup(cache_entry->db_key.db_key.key.s);
      if (!entry->db_key.db_key.key.s) {
        free((void *)entry->db_key.container_name);
        bitmap_free(entry->bitmap_copy);
        continue;
      }
    }
    msg->count++;
  }

  if (msg->count > 0) {
    eng_writer_queue_enqueue(&c->config.writer->queue, msg);
  } else {
    free(msg->entries);
    free(msg);
  }

  consumer_cache_clear_dirty_list(&c->cache);
}

static void _reclamation(consumer_t *consumer) {
  if (consumer->consumer_cache_thread_epoch_record.n_pending >=
      MIN_RECLAIM_BATCH_SIZE) {
    consumer_ebr_reclaim(&consumer->consumer_cache_thread_epoch_record);
  }
}

// TODO: store queue msgs unable to process (DLQ)
static void _consumer_thread_func(void *arg) {
  consumer_t *consumer = (consumer_t *)arg;
  const consumer_config_t *config = &consumer->config;
  consumer_cache_config_t cache_config = {.capacity = CONSUMER_CACHE_CAPACITY};

  consumer_cache_init(&consumer->cache, &cache_config);
  consumer_ebr_register(&consumer->epoch,
                        &consumer->consumer_cache_thread_epoch_record);

  op_queue_msg_batch_t *batch_hash = NULL;
  bool batched_any = false;
  uint32_t cycle = 0;
  int backoff = 1;
  int spin_count = 0;

  while (!consumer->should_stop) {
    batched_any = false;
    cycle++;

    // TODO: err handling
    _batch_by_container(consumer, &batch_hash, &batched_any);

    if (batched_any) {
      backoff = 1;
      spin_count = 0;
      _process_batches(consumer, batch_hash);
      _free_batch_hash(batch_hash);
      batch_hash = NULL;
    } else {
      if (spin_count < CONSUMER_SPIN_LIMIT) {
        sched_yield();
        spin_count++;
      } else {
        uv_sleep(backoff);
        backoff = backoff < CONSUMER_MAX_SLEEP_MS ? backoff * 2
                                                  : CONSUMER_MAX_SLEEP_MS;
      }
    }

    if (cycle == config->flush_every_n) {
      cycle = 0;
      _flush(consumer);
      _reclamation(consumer);
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
  consumer_ebr_unregister(&consumer->consumer_cache_thread_epoch_record);
  consumer_cache_destroy(&consumer->cache);
  return true;
}

void consumer_get_stats(consumer_t *consumer, uint64_t *processed_out) {
  if (!consumer || !processed_out) {
    return;
  }
  *processed_out = consumer->messages_processed;
}
