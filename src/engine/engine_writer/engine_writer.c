#include "engine_writer.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/container/container.h"
#include "engine/container/container_types.h"
#include "engine/engine_writer/engine_writer_queue.h"
#include "engine/engine_writer/engine_writer_queue_msg.h"
#include "lmdb.h"
#include "log/log.h"
#include "uthash.h"
#include "uv.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

LOG_INIT(writer);

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
  uint32_t count;
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
    LOG_ACTION_ERROR(ACT_MEMORY_ALLOC_FAILED,
                     "context=\"write_batch\" container=\"%s\"",
                     container_name);
    return NULL;
  }
  batch->container_name = container_name;
  batch->count = 0;
  return batch;
}

static bool _add_entry_to_batch(write_batch_t *batch,
                                eng_writer_entry_t *entry) {
  write_batch_item_t *item = calloc(1, sizeof(write_batch_item_t));
  if (!item) {
    LOG_ACTION_ERROR(ACT_MEMORY_ALLOC_FAILED,
                     "context=\"write_batch_item\" container=\"%s\"",
                     batch->container_name);
    return false;
  }
  item->entry = entry;
  if (!batch->head) {
    batch->head = item;
    batch->tail = item;
  } else {
    batch->tail->next = item;
    batch->tail = item;
  }
  batch->count++;
  return true;
}

static bool _group_dirty_copies_by_container(write_batch_t **b_hash,
                                             eng_writer_msg_t *msg) {
  write_batch_t *batch_hash = *b_hash;
  write_batch_t *batch = NULL;

  LOG_ACTION_DEBUG(ACT_BATCH_GROUPING, "entry_count=%u", msg->count);

  for (uint32_t i = 0; i < msg->count; ++i) {
    eng_writer_entry_t *entry = &msg->entries[i];
    HASH_FIND_STR(batch_hash, entry->db_key.container_name, batch);
    if (batch) {
      if (!_add_entry_to_batch(batch, entry)) {
        LOG_ACTION_ERROR(ACT_BATCH_ADD_FAILED,
                         "context=\"existing_batch\" container=\"%s\"",
                         entry->db_key.container_name);
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
      LOG_ACTION_ERROR(ACT_BATCH_ADD_FAILED,
                       "context=\"new_batch\" container=\"%s\"",
                       entry->db_key.container_name);
      _free_batch_hash(batch_hash);
      return false;
    }

    HASH_ADD_KEYPTR(hh, batch_hash, batch->container_name,
                    strlen(batch->container_name), batch);
    LOG_ACTION_DEBUG(ACT_BATCH_CREATED,
                     "context=\"write_batch\" container=\"%s\"",
                     batch->container_name);
  }

  *b_hash = batch_hash;
  return true;
}

static bool _ser_bitmap(eng_writer_entry_t *entry, void **val_out,
                        size_t *val_size_out) {
  *val_out = bitmap_serialize(entry->val.bitmap_copy, val_size_out);
  if (!*val_out) {
    LOG_ACTION_ERROR(ACT_SERIALIZATION_FAILED, "val_type=bitmap");
    return false;
  }
  LOG_ACTION_DEBUG(ACT_SERIALIZATION_SUCCESS,
                   "val_type=bitmap size_bytes=%zu version=%llu", *val_size_out,
                   entry->version);
  return true;
}

static bool _write_to_db(eng_container_t *c, MDB_txn *txn,
                         eng_writer_entry_t *entry) {
  MDB_dbi target_db;
  size_t val_size = 0;
  void *val = NULL;

  if (!container_get_user_db_handle(c, entry->db_key.user_db_type,
                                    &target_db)) {
    LOG_ACTION_ERROR(ACT_DB_HANDLE_FAILED, "container=\"%s\" db_type=%d",
                     c->name, entry->db_key.user_db_type);
    return false;
  }

  switch (entry->val_type) {
  case ENG_WRITER_VAL_BITMAP:
    if (!_ser_bitmap(entry, &val, &val_size)) {
      return false;
    }
    break;
  case ENG_WRITER_VAL_STR:
    val = entry->val.str_copy;
    val_size = strlen(entry->val.str_copy);
    break;
  case ENG_WRITER_VAL_INT32:
    val = &entry->val.int32;
    val_size = sizeof(uint32_t);
    break;
  default:
    LOG_ACTION_ERROR(ACT_WRITE_FAILED,
                     "err=\"unknown_value_type\" container=\"%s\"", c->name);
    return false;
  }

  if (!db_put(target_db, txn, &entry->db_key.db_key, val, val_size, false)) {
    LOG_ACTION_ERROR(ACT_DB_WRITE_FAILED, "container=\"%s\" size_bytes=%zu",
                     c->name, val_size);
    free(val);
    return false;
  }

  free(val);
  return true;
}

static void _bump_flush_version(write_batch_t *container_batch) {
  write_batch_item_t *item = container_batch->head;
  uint32_t bumped = 0;

  while (item) {
    uint32_t v = item->entry->version;
    atomic_store(item->entry->flush_version_ptr, v);
    bumped++;
    item = item->next;
  }

  LOG_ACTION_DEBUG(ACT_FLUSH_VERSION_UPDATED, "count=%u container=\"%s\"",
                   bumped, container_batch->container_name);
}

static void _flush_dirty_snapshots_to_db(write_batch_t *hash) {
  if (!hash)
    return;

  write_batch_t *batch, *tmp;
  uint32_t total_batches = 0;
  uint32_t successful_batches = 0;
  uint32_t total_entries = 0;
  uint32_t successful_entries = 0;

  HASH_ITER(hh, hash, batch, tmp) {
    total_batches++;
    total_entries += batch->count;

    container_result_t cr = container_get_or_create_user(batch->container_name);
    if (!cr.success) {
      LOG_ACTION_ERROR(ACT_CONTAINER_OPEN_FAILED, "container=\"%s\"",
                       batch->container_name);
      continue;
    }
    eng_container_t *c = cr.container;

    MDB_txn *txn = db_create_txn(c->env, false);
    if (!txn) {
      LOG_ACTION_ERROR(ACT_TXN_BEGIN, "err=\"failed\" container=\"%s\"",
                       batch->container_name);
      container_release(c);
      continue;
    }

    write_batch_item_t *item = batch->head;
    bool all_successful = true;
    uint32_t batch_written = 0;

    while (item) {
      if (!_write_to_db(c, txn, item->entry)) {
        LOG_ACTION_ERROR(ACT_DB_WRITE_FAILED, "entry=%u/%u container=\"%s\"",
                         batch_written + 1, batch->count,
                         batch->container_name);
        all_successful = false;
        break;
      }
      batch_written++;
      item = item->next;
    }

    if (all_successful && db_commit_txn(txn)) {
      _bump_flush_version(batch);
      successful_batches++;
      successful_entries += batch->count;
      LOG_ACTION_DEBUG(ACT_DB_WRITE, "entries_written=%u container=\"%s\"",
                       batch->count, batch->container_name);
    } else {
      if (all_successful) {
        LOG_ACTION_ERROR(ACT_TXN_COMMIT,
                         "err=\"failed\" container=\"%s\" entries=%u",
                         batch->container_name, batch->count);
      }
      db_abort_txn(txn);
    }

    container_release(c);
  }

  if (successful_batches > 0) {
    LOG_ACTION_INFO(
        ACT_PERF_FLUSH_COMPLETE, "entries_written=%u/%u containers=%u/%u",
        successful_entries, total_entries, successful_batches, total_batches);
  }

  if (successful_batches < total_batches) {
    LOG_ACTION_WARN(ACT_FLUSH_PARTIAL_FAILURE,
                    "batches_succeeded=%u/%u entries_written=%u/%u",
                    successful_batches, total_batches, successful_entries,
                    total_entries);
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
  uint64_t total_cycles = 0;
  uint64_t total_messages = 0;
  uint64_t total_entries = 0;

  log_init_writer();
  if (!LOG_CATEGORY) {
    fprintf(stderr, "FATAL: Failed to initialize logging for writer thread\n");
    return;
  }

  eng_writer_queue_init(&writer->queue);

  LOG_ACTION_INFO(ACT_THREAD_STARTED, "thread_type=writer");

  while (!writer->should_stop) {
    have_work = false;
    total_cycles++;

    write_batch_t *batch_hash = NULL;
    eng_writer_msg_t *msg;
    uint32_t dequeued = 0;

    for (int i = 0; i < MAX_DEQUEUE_MSG_COUNT; i++) {
      bool dq = _deque(writer, &msg);
      if (!dq)
        break;

      have_work = true;
      dequeued++;
      total_messages++;
      total_entries += msg->count;

      if (!_group_dirty_copies_by_container(&batch_hash, msg)) {
        LOG_ACTION_ERROR(ACT_BATCH_GROUPING_FAILED,
                         "entries=%u action=discarding", msg->count);
        eng_writer_queue_free_msg(msg);
        break;
      }
      eng_writer_queue_free_msg(msg);
    }

    if (have_work) {
      backoff = 1;
      spin_count = 0;

      LOG_ACTION_DEBUG(ACT_MSG_DEQUEUED, "msg_type=writer count=%u", dequeued);

      if (!batch_hash) {
        LOG_ACTION_WARN(ACT_BATCH_GROUPING_FAILED,
                        "err=\"no_batch_hash_created\"");
        continue;
      }

      _flush_dirty_snapshots_to_db(batch_hash);
      _free_batch_hash(batch_hash);
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

    // Periodic stats
    if (total_cycles % 10000 == 0 && total_messages > 0) {
      LOG_ACTION_INFO(ACT_WRITER_STATS,
                      "total_cycles=%llu total_messages=%llu "
                      "total_entries=%llu avg_entries_per_msg=%.1f",
                      total_cycles, total_messages, total_entries,
                      (double)total_entries / total_messages);
    }
  }

  LOG_ACTION_INFO(ACT_THREAD_STOPPED,
                  "thread_type=writer total_messages=%llu total_entries=%llu",
                  total_messages, total_entries);
}

bool eng_writer_start(eng_writer_t *writer, const eng_writer_config_t *config) {
  if (!writer || !config) {
    return false;
  }

  writer->config = *config;
  writer->should_stop = false;
  writer->entries_written = 0;

  if (uv_thread_create(&writer->thread, _eng_writer_thread_func, writer) != 0) {
    return false;
  }

  return true;
}

bool eng_writer_stop(eng_writer_t *writer) {
  if (!writer) {
    return false;
  }

  writer->should_stop = true;

  if (uv_thread_join(&writer->thread) != 0) {
    return false;
  }

  return true;
}