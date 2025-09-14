#include "engine_writer.h"

// Run reclamation after every N flush cycles
// if (worker->flush_cycles % 5 == 0) {
//     perform_reclamation_cycle(worker);
// }

// static void maintenance_worker_thread(void *arg) {
//     maintenance_worker_t *worker = (maintenance_worker_t *)arg;

//     uint64_t last_flush = uv_now(uv_default_loop());
//     uint64_t last_reclaim = uv_now(uv_default_loop());

//     while (!worker->should_stop) {
//         uint64_t now = uv_now(uv_default_loop());

//         // Handle flushing
//         if (now - last_flush >= worker->config.flush_interval_ms) {
//             perform_flush_cycle(worker);
//             last_flush = now;
//         }

//         // Handle epoch reclamation
//         if (now - last_reclaim >= worker->config.reclaim_interval_ms) {
//             perform_reclamation_cycle(worker);
//             last_reclaim = now;
//         }

//         // Sleep briefly to avoid spinning
//         uv_sleep(10); // 10ms
//     }
// }

// static void perform_reclamation_cycle(maintenance_worker_t *worker) {
//     // Trigger epoch synchronization
//     ck_epoch_synchronize(&global_epoch_record);

//     // Reclaim memory from all shards
//     bm_cache_t *cache = worker->config.cache;
//     for (int i = 0; i < NUM_SHARDS; i++) {
//         uint32_t reclaimed =
//         ck_epoch_reclaim(&cache->shards[i].epoch_record);
//         worker->objects_reclaimed += reclaimed;
//     }

//     worker->reclaim_cycles++;
// }

// static void engine_writer_flush_cycle(engine_writer_t *writer) {
//     bm_cache_flush_batch_t batch;

//     // Get dirty data from cache
//     if (bm_cache_prepare_flush_batch(writer->config.cache, &batch) != 0) {
//         return;
//     }

//     if (batch.total_entries == 0) {
//         return; // Nothing to flush
//     }

//     bool success = true;

//     // Process each shard's dirty entries
//     for (int i = 0; i < NUM_SHARDS && success; i++) {
//         if (batch.shards[i].entry_count > 0) {
//             success = flush_shard_entries_to_lmdb(&batch.shards[i]);
//         }
//     }

//     // Report results back to cache
//     bm_cache_complete_flush_batch(writer->config.cache, &batch, success);

//     writer->entries_written += batch.total_entries;
// }

// #include "engine_writer.h"
// #include "container.h"
// #include "core/bitmaps.h"
// #include "core/db.h"
// #include "dc_cache.h"
// #include "engine_cache.h"
// #include "lmdb.h"
// #include "uthash.h"
// #include "uv.h"
// #include <stdbool.h>
// #include <stdlib.h>

// static uv_thread_t g_writer_thread;
// static bool g_shutdown_flag = false;

// typedef struct write_batch_entry_s {
//   eng_cache_node_t *dirty_node;
//   struct write_batch_entry_s *next;
// } write_batch_entry_t;

// typedef struct write_batch_s {
//   UT_hash_handle hh;
//   char *container_name;
//   write_batch_entry_t *head;
//   write_batch_entry_t *tail;
// } write_batch_t;

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

// // The main function for the background writer thread.
// static void background_writer_main(void *arg) {
//   ;
//   (void)arg;
//   while (!g_shutdown_flag) {
//     uv_sleep(100);

//     eng_cache_node_t *local_dirty_head = eng_cache_swap_dirty_list();
//     if (!local_dirty_head) {
//       continue;
//     }

//     _flush_dirty_list_to_db(local_dirty_head);
//   }
// }

static void _eng_writer_thread_func(void *arg) {
  eng_writer_t *writer = (eng_writer_t *)arg;
  const eng_writer_config_t *config = &writer->config;
}

bool eng_writer_start(eng_writer_t *worker, const eng_writer_config_t *config) {
  worker->config = *config;
  worker->should_stop = false;
  worker->entries_written = 0;
  worker->objects_reclaimed = 0;
  worker->reclaim_cycles = 0;

  if (uv_thread_create(&worker->thread, _eng_writer_thread_func, worker) != 0) {
    return false;
  }
  return true;
}
bool eng_writer_stop(eng_writer_t *worker) {
  worker->should_stop = true;
  if (uv_thread_join(&worker->thread) != 0) {
    return false;
  }
  return true;
}
