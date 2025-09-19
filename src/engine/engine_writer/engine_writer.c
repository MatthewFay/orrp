#include "engine_writer.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/bitmap_cache/cache_shard.h"
#include "engine/container.h"
#include "engine/dc_cache.h"
#include "flush_msg.h"
#include "lmdb.h"
#include "uthash.h"
#include "uv.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define MAX_ENQUEUE_ATTEMPTS 3
#define MAX_DEQUEUE_MSG_COUNT 32

typedef struct write_batch_entry_s {
  bm_cache_dirty_copy_t *copy;
  struct write_batch_entry_s *next;
} write_batch_entry_t;

typedef struct write_batch_s {
  UT_hash_handle hh;
  char *container_name;
  write_batch_entry_t *head;
  write_batch_entry_t *tail;
} write_batch_t;

// Free batch hash structure (not inner data)
static void _free_batch_hash(write_batch_t *batch_hash) {
  if (!batch_hash)
    return;
  write_batch_t *b, *tmp;
  HASH_ITER(hh, batch_hash, b, tmp) {
    write_batch_entry_t *b_entry = b->head;
    while (b_entry) {
      write_batch_entry_t *next = b_entry->next;
      free(b_entry);
      b_entry = next;
    }
    HASH_DEL(batch_hash, b);
    free(b);
  }
}

static write_batch_t *_create_batch(char *container_name) {
  write_batch_t *batch = calloc(1, sizeof(write_batch_t));
  if (!batch) {
    return NULL;
  }
  batch->container_name = container_name;
  return batch;
}

static bool _add_dirty_copy_to_batch(write_batch_t *batch,
                                     bm_cache_dirty_copy_t *copy) {
  write_batch_entry_t *entry = calloc(1, sizeof(write_batch_entry_t));
  if (!entry) {
    return false;
  }
  entry->copy = copy;
  if (!batch->head) {
    batch->head = entry;
    batch->tail = entry;
    return true;
  }
  batch->tail->next = entry;
  batch->tail = entry;
  return true;
}

static bool _group_dirty_copies_by_container(write_batch_t **b_hash,
                                             bm_cache_dirty_snapshot_t *snap) {
  write_batch_t *batch_hash = *b_hash;
  write_batch_t *batch = NULL;

  for (uint32_t i = 0; i < snap->entry_count; ++i) {
    bm_cache_dirty_copy_t *copy = &snap->dirty_copies[i];
    HASH_FIND_STR(batch_hash, copy->container_name, batch);
    if (batch) {
      if (!_add_dirty_copy_to_batch(batch, copy)) {
        _free_batch_hash(batch_hash);
        return false;
      }
      continue;
    }

    batch = _create_batch(copy->container_name);
    if (!batch) {
      _free_batch_hash(batch_hash);
      return false;
    }

    if (!_add_dirty_copy_to_batch(batch, copy)) {
      _free_batch_hash(batch_hash);
      return false;
    }
  }
  return true;
}

static bool _get_val(bm_cache_dirty_copy_t *copy, void **val_out,
                     size_t *val_size_out) {
  *val_out = bitmap_serialize(copy->bitmap, val_size_out);
  if (!*val_out) {
    return false;
  }
  return true;
}

static bool _write_to_db(eng_container_t *c, MDB_txn *txn,
                         bm_cache_dirty_copy_t *copy) {
  MDB_dbi target_db;
  size_t val_size;
  void *val;
  if (!eng_container_get_user_db(c, copy->db_type, &target_db)) {
    return false;
  }
  if (!_get_val(copy, &val, &val_size)) {
    return false;
  }
  if (!db_put(target_db, txn, &copy->db_key, val, val_size, false)) {
    return false;
  }
  return true;
}

static void _bump_flush_version(write_batch_t *container_batch) {
  write_batch_entry_t *entry = container_batch->head;
  while (entry) {
    uint32_t v = entry->copy->bitmap->version;
    atomic_store(entry->copy->flush_version_ptr, v);
    entry = entry->next;
  }
}

static void _flush_dirty_snapshots_to_db(write_batch_t *hash) {
  if (!hash)
    return;

  write_batch_t *batch, *tmp;
  HASH_ITER(hh, hash, batch, tmp) {
    eng_container_t *c = eng_dc_cache_get(batch->container_name);
    if (!c) {
      // TODO: handle error case - retry next cycle?
      continue;
    }
    MDB_txn *txn = db_create_txn(c->env, false);
    if (!txn) {
      // TODO: handle error case - retry next cycle?
      eng_dc_cache_release_container(c);
      continue;
    }
    write_batch_entry_t *entry = batch->head;
    bool all_successful = true;
    while (entry) {
      if (!_write_to_db(c, txn, entry->copy)) {
        all_successful = false;
        break; // Stop processing this batch on first error
      }
      entry = entry->next;
    }
    if (all_successful && db_commit_txn(txn)) {
      _bump_flush_version(batch);
    } else {
      db_abort_txn(txn); // Roll back all changes for this batch
    }
    eng_dc_cache_release_container(c);
  }
}

static bool _deque(eng_writer_t *w, flush_msg_t **msg_out) {
  return ck_ring_dequeue_mpsc(&w->ring, w->ring_buffer, msg_out);
}

static void _eng_writer_thread_func(void *arg) {
  eng_writer_t *writer = (eng_writer_t *)arg;
  const eng_writer_config_t *config = &writer->config;

  while (!writer->should_stop) {
    uv_sleep(config->flush_interval_ms);

    write_batch_t *batch_hash = NULL;
    flush_msg_t *msg;
    for (int i = 0; i < MAX_DEQUEUE_MSG_COUNT; i++) {
      bool dq = _deque(writer, &msg);
      if (!dq)
        break;
      bm_cache_dirty_snapshot_t *snap = msg->data.bm_cache_dirty_snapshot;
      if (!_group_dirty_copies_by_container(&batch_hash, snap)) {
        shard_free_dirty_snapshot(snap); // todo: retry, use Queue for DLQ
        flush_msg_free(msg);
        break;
      }
      flush_msg_free(msg);
    }
    if (!batch_hash) {
      // handle err
      continue;
    }
    _flush_dirty_snapshots_to_db(batch_hash);
  }
}

bool eng_writer_start(eng_writer_t *writer, const eng_writer_config_t *config) {
  writer->config = *config;
  writer->should_stop = false;
  writer->entries_written = 0;

  if (uv_thread_create(&writer->thread, _eng_writer_thread_func, writer) != 0) {
    return false;
  }
  return true;
}
bool eng_writer_stop(eng_writer_t *writer) {
  writer->should_stop = true;
  if (uv_thread_join(&writer->thread) != 0) {
    return false;
  }
  return true;
}

static bool _enqueue_msg(eng_writer_t *writer, flush_msg_t *msg) {
  bool enqueued = false;
  for (int i = 0; i < MAX_ENQUEUE_ATTEMPTS; i++) {
    if (ck_ring_enqueue_mpsc(&writer->ring, writer->ring_buffer, msg)) {
      enqueued = true;
      break;
    }
    // Ring buffer is full
    ck_pr_stall();
    // might add a short sleep here
  }
  return enqueued;
}

bool eng_writer_queue_up_bm_dirty_snapshot(
    eng_writer_t *writer, bm_cache_dirty_snapshot_t *dirty_snapshot) {
  if (!writer || !dirty_snapshot)
    return false;
  flush_msg_t *msg = flush_msg_create(BITMAP_DIRTY_SNAPSHOT, dirty_snapshot);
  if (!msg)
    return false;
  if (!_enqueue_msg(writer, msg)) {
    flush_msg_free(msg);
    return false;
  }
  return true;
}