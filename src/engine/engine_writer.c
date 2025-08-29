#include "engine_writer.h"
#include "engine_cache.h"
#include "uv.h"
#include <stdbool.h>

static uv_thread_t g_writer_thread;
static bool g_shutdown_flag = false;

// This function processes a list of dirty nodes, flushing them to DB.
static void _flush_dirty_list_to_db(eng_cache_node_t *dirty_list_head) {
  // 1. Group the nodes by their container name to batch writes.
  //    (e.g., create a temporary hash map of container_name -> list_of_nodes).

  // 2. For each container in your grouped list:
  //    a. Start a single LMDB write transaction for that container.
  //    b. Iterate through all the dirty nodes for that container.
  //    c. Serialize the data_object (e.g., bitmap) and db_put() it.
  //    d. Commit the transaction.
  //    e. If the commit was successful:
  //       - For each node just written, set node->is_dirty = false.
  //       - If a node was marked for eviction, now is the time to
  //       _free_node(node).
  //    f. If the commit failed, the nodes remain dirty and will be picked up
  //       in the next writer cycle.
}

// The main function for the background writer thread.
static void background_writer_main(void *arg) {
  ;
  // while (!g_shutdown_flag) {
  //   // Sleep for a short interval (e.g., 100ms).
  //   uv_sleep(100);

  //   // --- The Lock-and-Swap Pattern ---
  //   uv_mutex_lock(&g_cache.dirty_list_lock);
  //   eng_cache_node_t *local_dirty_head = g_cache.dirty_head;
  //   g_cache.dirty_head = NULL;
  //   g_cache.dirty_tail = NULL;
  //   uv_mutex_unlock(&g_cache.dirty_list_lock);

  //   if (local_dirty_head) {
  //     _flush_dirty_list_to_ddb(local_dirty_head);
  //   }
  // }f
}

bool eng_writer_start() {
  if (uv_thread_create(&g_writer_thread, background_writer_main, NULL) == 0)
    return true;
  return false;
}

bool eng_writer_stop() {
  g_shutdown_flag = true;
  if (uv_thread_join(&g_writer_thread) != 0) {
    return false;
  }
  return true;
}

// commit global id directory txn first - if `usr_commit` fails, it's OK.
// bool sys_commit = db_commit_txn(sys_c_txn);
// if (!sys_commit) {
//   r->err_msg = ENG_TXN_COMMIT_ERR;

//   // cleanup
//   _eng_release_container(dc);

//   return;
// }
// bool usr_commit = db_commit_txn(usr_c_txn);
// if (!usr_commit) {
//   r->err_msg = ENG_TXN_COMMIT_ERR;

//   // cleanup
//   _eng_release_container(dc);

//   return;
// }