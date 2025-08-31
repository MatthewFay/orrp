#include "engine_writer.h"
#include "container.h"
#include "core/db.h"
#include "dc_cache.h"
#include "engine_cache.h"
#include "lmdb.h"
#include "uthash.h"
#include "uv.h"
#include <stdbool.h>
#include <stdlib.h>

static uv_thread_t g_writer_thread;
static bool g_shutdown_flag = false;

typedef struct write_batch_entry_s {
  eng_cache_node_t *dirty_node;
  struct write_batch_entry_s *next;
} write_batch_entry_t;

typedef struct write_batch_s {
  UT_hash_handle hh;
  char *container_name;
  write_batch_entry_t *head;
  write_batch_entry_t *tail;
} write_batch_t;

static void _free_batch_hash(write_batch_t *batch_hash) {
  if (!batch_hash)
    return;
  write_batch_t *n, *tmp;
  HASH_ITER(hh, batch_hash, n, tmp) {
    HASH_DEL(batch_hash, n);
    free(n->container_name);
    free(n);
  }
}

static write_batch_t *_create_batch(char *container_name) {
  write_batch_t *batch = calloc(1, sizeof(write_batch_t));
  if (!batch) {
    return NULL;
  }
  batch->container_name = strdup(container_name);
  return batch;
}

static bool _add_dirty_node_to_batch(write_batch_t *batch,
                                     eng_cache_node_t *dirty_node) {
  write_batch_entry_t *entry = calloc(1, sizeof(write_batch_entry_t));
  if (!entry) {
    return false;
  }
  entry->dirty_node = dirty_node;
  if (!batch->head) {
    batch->head = entry;
    batch->tail = entry;
    return true;
  }
  batch->tail->next = entry;
  batch->tail = entry;
  return true;
}

static write_batch_t *
_group_dirty_nodes_by_container(eng_cache_node_t *dirty_list_head) {
  write_batch_t *batch_hash = NULL;
  write_batch_t *batch = NULL;
  eng_cache_node_t *dirty_node = dirty_list_head;

  while (dirty_node) {
    HASH_FIND_STR(batch_hash, dirty_node->container_name, batch);
    if (batch) {
      if (!_add_dirty_node_to_batch(batch, dirty_node)) {
        _free_batch_hash(batch_hash);
        return NULL;
      }
      continue;
    }

    batch = _create_batch(dirty_node->container_name);
    if (!batch) {
      _free_batch_hash(batch_hash);
      return NULL;
    }

    if (!_add_dirty_node_to_batch(batch, dirty_node)) {
      _free_batch_hash(batch_hash);
      return NULL;
    }
    dirty_node = dirty_node->dirty_next;
  }

  return batch_hash;
}

static bool _write_cached_node_to_db(eng_container_t *c, MDB_txn *txn,
                                     eng_cache_node_t *node) {}

// This function processes a list of dirty nodes, flushing them to DB.
static void _flush_dirty_list_to_db(eng_cache_node_t *dirty_list_head) {
  if (!dirty_list_head)
    return;
  write_batch_t *hash = _group_dirty_nodes_by_container(dirty_list_head);
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
      continue;
    }
    write_batch_entry_t *entry = batch->head;
    while (entry) {
      eng_cache_node_t *dirty_node = entry->dirty_node;
      if (!_write_cached_node_to_db(c, txn, dirty_node)) {
      }
      entry = entry->next;
    }
  }

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
  (void)arg;
  eng_cache_node_t **dirty_head = NULL;
  eng_cache_node_t **dirty_tail = NULL;
  while (!g_shutdown_flag) {
    uv_sleep(100);

    // --- The Lock-and-Swap Pattern ---
    eng_cache_lock_dirty_list();
    eng_cache_get_dirty_list(dirty_head, dirty_tail);
    if (!*dirty_head) {
      eng_cache_unlock_dirty_list();
      continue;
    }
    eng_cache_node_t *local_dirty_head = *dirty_head;
    *dirty_head = NULL;
    *dirty_tail = NULL;
    eng_cache_unlock_dirty_list();

    _flush_dirty_list_to_db(local_dirty_head);
  }
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
