#include "engine_writer.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/container/container.h"
#include "engine/dc_cache/dc_cache.h"
#include "engine/engine_writer/engine_writer_queue_msg.h"
#include "lmdb.h"
#include "uthash.h"
#include "uv.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// spin count before sleeping
#define ENG_WRITER_SPIN_LIMIT 100
#define ENG_WRITER_MAX_SLEEP_MS 64

#define MAX_DEQUEUE_MSG_COUNT 32

typedef struct write_batch_item_s {
  eng_writer_entry_t *entry;
  struct write_batch_item_s *next;
} write_batch_item_t;

typedef struct write_batch_s {
  UT_hash_handle hh;
  char *container_name;
  write_batch_item_t *head;
  write_batch_item_t *tail;
} write_batch_t;

// Free batch hash structure (not inner data)
static void _free_batch_hash(write_batch_t *batch_hash) {
  if (!batch_hash)
    return;
  write_batch_t *b, *tmp;
  HASH_ITER(hh, batch_hash, b, tmp) {
    write_batch_item_t *b_item = b->head;
    while (b_item) {
      write_batch_item_t *next = b_item->next;
      free(b_item);
      b_item = next;
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

static bool _add_entry_to_batch(write_batch_t *batch,
                                eng_writer_entry_t *entry) {
  write_batch_item_t *item = calloc(1, sizeof(write_batch_item_t));
  if (!item) {
    return false;
  }
  item->entry = entry;
  if (!batch->head) {
    batch->head = item;
    batch->tail = item;
    return true;
  }
  batch->tail->next = item;
  batch->tail = item;
  return true;
}

static bool _group_dirty_copies_by_container(write_batch_t **b_hash,
                                             eng_writer_msg_t *msg) {
  write_batch_t *batch_hash = *b_hash;
  write_batch_t *batch = NULL;

  for (uint32_t i = 0; i < msg->count; ++i) {
    eng_writer_entry_t *entry = &msg->entries[i];
    HASH_FIND_STR(batch_hash, entry->db_key.container_name, batch);
    if (batch) {
      if (!_add_entry_to_batch(batch, entry)) {
        _free_batch_hash(batch_hash);
        return false;
      }
      continue;
    }

    batch = _create_batch(entry->db_key.container_name);
    if (!batch) {
      _free_batch_hash(batch_hash);
      return false;
    }

    if (!_add_entry_to_batch(batch, entry)) {
      _free_batch_hash(batch_hash);
      return false;
    }
  }
  return true;
}

static bool _get_val(eng_writer_entry_t *entry, void **val_out,
                     size_t *val_size_out) {
  *val_out = bitmap_serialize(entry->bitmap_copy, val_size_out);
  if (!*val_out) {
    return false;
  }
  return true;
}

static bool _write_to_db(eng_container_t *c, MDB_txn *txn,
                         eng_writer_entry_t *entry) {
  MDB_dbi target_db;
  size_t val_size;
  void *val;
  if (!eng_container_get_db_handle(c, entry->db_key.user_db_type, &target_db)) {
    return false;
  }
  if (!_get_val(entry, &val, &val_size)) {
    return false;
  }
  if (!db_put(target_db, txn, &entry->db_key.db_key, val, val_size, false)) {
    return false;
  }
  return true;
}

static void _bump_flush_version(write_batch_t *container_batch) {
  write_batch_item_t *item = container_batch->head;
  while (item) {
    uint32_t v = item->entry->bitmap_copy->version;
    atomic_store(item->entry->flush_version_ptr, v);
    item = item->next;
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
    write_batch_item_t *item = batch->head;
    bool all_successful = true;
    while (item) {
      if (!_write_to_db(c, txn, item->entry)) {
        all_successful = false;
        break; // Stop processing this batch on first error
      }
      item = item->next;
    }
    if (all_successful && db_commit_txn(txn)) {
      _bump_flush_version(batch);
    } else {
      db_abort_txn(txn); // Roll back all changes for this batch
    }
    eng_dc_cache_release_container(c);
  }
}

// Try to Pull a message from the eng writer queue
static bool _deque(eng_writer_t *w, eng_writer_msg_t **msg_out) {
  return ck_ring_dequeue_mpsc(&w->queue.ring, w->queue.ring_buffer, msg_out);
}

static void _eng_writer_thread_func(void *arg) {
  eng_writer_t *writer = (eng_writer_t *)arg;
  const eng_writer_config_t *config = &writer->config;
  int backoff = 1;
  int spin_count = 0;
  bool have_work = false;

  while (!writer->should_stop) {
    have_work = false;

    write_batch_t *batch_hash = NULL;
    eng_writer_msg_t *msg;
    for (int i = 0; i < MAX_DEQUEUE_MSG_COUNT; i++) {
      bool dq = _deque(writer, &msg);
      if (!dq)
        break;
      have_work = true;
      if (!_group_dirty_copies_by_container(&batch_hash, msg)) {
        // todo: retry, DLQ
        eng_writer_queue_free_msg(msg);
        break;
      }
    }

    if (have_work) {
      backoff = 1;
      spin_count = 0;
      if (!batch_hash) {
        // handle err
        continue;
      }
      _flush_dirty_snapshots_to_db(batch_hash);
    } else {
      if (spin_count < ENG_WRITER_SPIN_LIMIT) {
        sched_yield();
        spin_count++;
      } else {
        uv_sleep(backoff);
        backoff = backoff < ENG_WRITER_MAX_SLEEP_MS ? backoff * 2
                                                    : ENG_WRITER_MAX_SLEEP_MS;
      }
    }
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
