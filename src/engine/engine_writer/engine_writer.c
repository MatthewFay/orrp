#include "engine_writer.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/bitmap_cache/cache_entry.h"
#include "engine/container.h"
#include "engine/dc_cache.h"
#include "flush_msg.h"
#include "lmdb.h"
#include "uthash.h"
#include "uv.h"
#include <stdbool.h>
#include <stdlib.h>

#define MAX_ENQUEUE_ATTEMPTS 3
#define MAX_DEQUEUE_MSG_COUNT 32

typedef struct write_batch_s {
  UT_hash_handle hh;
  char *container_name;
  bm_cache_entry_t *head;
  bm_cache_entry_t *tail;
} write_batch_t;

// static void _free_batch_hash(write_batch_t *batch_hash) {
//   if (!batch_hash)
//     return;
//   write_batch_t *n, *tmp;
//   HASH_ITER(hh, batch_hash, n, tmp) {
//     write_batch_entry_t *entry = n->head;
//     while (entry) {
//       write_batch_entry_t *next_entry = entry->next;
//       free(entry);
//       entry = next_entry;
//     }
//     HASH_DEL(batch_hash, n);
//     free(n->container_name);
//     free(n);
//   }
// }

// static write_batch_t *_create_batch(char *container_name) {
//   write_batch_t *batch = calloc(1, sizeof(write_batch_t));
//   if (!batch) {
//     return NULL;
//   }
//   batch->container_name = strdup(container_name);
//   return batch;
// }

// static bool _add_dirty_node_to_batch(write_batch_t *batch,
//                                      eng_cache_node_t *dirty_node) {
//   write_batch_entry_t *entry = calloc(1, sizeof(write_batch_entry_t));
//   if (!entry) {
//     return false;
//   }
//   entry->dirty_node = dirty_node;
//   if (!batch->head) {
//     batch->head = entry;
//     batch->tail = entry;
//     return true;
//   }
//   batch->tail->next = entry;
//   batch->tail = entry;
//   return true;
// }

// static write_batch_t *
// _group_dirty_nodes_by_container(eng_cache_node_t *dirty_list_head) {
//   write_batch_t *batch_hash = NULL;
//   write_batch_t *batch = NULL;
//   eng_cache_node_t *dirty_node = dirty_list_head;

//   while (dirty_node) {
//     HASH_FIND_STR(batch_hash, dirty_node->container_name, batch);
//     if (batch) {
//       if (!_add_dirty_node_to_batch(batch, dirty_node)) {
//         _free_batch_hash(batch_hash);
//         return NULL;
//       }
//       dirty_node = dirty_node->dirty_next;
//       continue;
//     }

//     batch = _create_batch(dirty_node->container_name);
//     if (!batch) {
//       _free_batch_hash(batch_hash);
//       return NULL;
//     }

//     if (!_add_dirty_node_to_batch(batch, dirty_node)) {
//       _free_batch_hash(batch_hash);
//       return NULL;
//     }
//     dirty_node = dirty_node->dirty_next;
//   }

//   return batch_hash;
// }

// static bool _get_val(eng_cache_node_t *node, void **val_out,
//                      size_t *val_size_out) {
//   switch (node->type) {
//   case CACHE_TYPE_BITMAP:
//     *val_out = bitmap_serialize(node->data_object, val_size_out);
//     if (!*val_out) {
//       return false;
//     }
//     break;
//   case CACHE_TYPE_UINT32:
//     *val_out = node->data_object;
//     *val_size_out = sizeof(uint32_t);
//     break;
//   case CACHE_TYPE_STRING:
//     *val_out = node->data_object;
//     *val_size_out = strlen(node->data_object);
//     break;
//   }

//   return true;
// }

// // Flush dirty data to disk
// static bool _write_cached_node_to_db(eng_container_t *c, MDB_txn *txn,
//                                      eng_cache_node_t *node) {
//   MDB_dbi target_db;
//   size_t val_size;
//   void *val;
//   if (!eng_container_get_user_db(c, node->db_type, &target_db)) {
//     return false;
//   }
//   if (!_get_val(node, &val, &val_size)) {
//     return false;
//   }
//   if (!db_put(target_db, txn, &node->db_key, val, val_size, false)) {
//     return false;
//   }
//   return true;
// }

// // This function processes a list of dirty nodes, flushing them to DB.
// static void _flush_dirty_list_to_db(eng_cache_node_t *dirty_list_head) {
//   if (!dirty_list_head)
//     return;
//   write_batch_t *hash = _group_dirty_nodes_by_container(dirty_list_head);
//   if (!hash)
//     return;

//   write_batch_t *batch, *tmp;
//   HASH_ITER(hh, hash, batch, tmp) {
//     eng_container_t *c = eng_dc_cache_get(batch->container_name);
//     if (!c) {
//       // TODO: handle error case - retry next cycle?
//       continue;
//     }
//     MDB_txn *txn = db_create_txn(c->env, false);
//     if (!txn) {
//       // TODO: handle error case - retry next cycle?
//       eng_dc_cache_release_container(c);
//       continue;
//     }
//     write_batch_entry_t *entry = batch->head;
//     bool all_successful = true;
//     while (entry) {
//       if (!_write_cached_node_to_db(c, txn, entry->dirty_node)) {
//         all_successful = false;
//         break; // Stop processing this batch on first error
//       }
//       entry = entry->next;
//     }
//     if (all_successful) {
//       db_commit_txn(txn);
//       // NOW perform cleanup on all nodes in the batch (set is_dirty=false,
//       // etc.)
//     } else {
//       db_abort_txn(txn); // Roll back all changes for this batch
//     }
//     eng_dc_cache_release_container(c);
//   }
// }

// // 2. For each container in your grouped list:
// //    a. Start a single LMDB write transaction for that container.
// //    b. Iterate through all the dirty nodes for that container.
// //    c. Serialize the data_object (e.g., bitmap) and db_put() it.
// //    d. Commit the transaction.
// //    e. If the commit was successful:
// //       - For each node just written, set node->is_dirty = false.
// //       - If a node was marked for eviction, now is the time to
// //       _free_node(node). - How to tell if a node is marked for eviction??
// //       Ref count = 0 ? ? new flag ??
// //    f. If the commit failed, the nodes remain dirty and will be picked up
// //       in the next writer cycle.

static void _eng_writer_thread_func(void *arg) {
  eng_writer_t *writer = (eng_writer_t *)arg;
  const eng_writer_config_t *config = &writer->config;

  while (!writer->should_stop) {
    uv_sleep(config->flush_interval_ms);
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