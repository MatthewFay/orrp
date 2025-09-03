#include "engine_cache.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include "uthash.h"
#include "uv.h"
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

// Global instance of our cache
static eng_cache_mgr_t g_cache;

// Moves a given node to the front of the LRU list.
static void _move_to_front(eng_cache_node_t *node) {
  if (!node || node == g_cache.lru_head)
    return;
  node->prev->next = node->next;
  if (node == g_cache.lru_tail) {
    g_cache.lru_tail = node->prev;
  } else {
    node->next->prev = node->prev;
  }
  node->prev = NULL;
  node->next = g_cache.lru_head;
  g_cache.lru_head->prev = node;
  g_cache.lru_head = node;
}

static void _free_data_obj(eng_cache_node_t *node) {
  if (!node)
    return;
  switch (node->type) {
  case CACHE_TYPE_BITMAP:
    bitmap_free(node->data_object);
    break;
  case CACHE_TYPE_UINT32:
  case CACHE_TYPE_STRING:
    free(node->data_object);
    break;
  default:
    break;
  }
}

void eng_cache_free_node(eng_cache_node_t *node) {
  uv_rwlock_destroy(&node->node_lock);
  _free_data_obj(node);
  if (node->db_key.type == DB_KEY_STRING) {
    free((char *)node->db_key.key.s);
  }
  free(node);
}

// Evicts the least recently used node if it's not in use.
// This function assumes the caller holds the global cache write lock.
static void _evict_lru_node() {
  eng_cache_node_t *node_to_evict = g_cache.lru_tail;
  if (!node_to_evict || node_to_evict->evict || node_to_evict->ref_count > 0) {
    return;
  }

  if (node_to_evict->is_dirty) {
    // If dirty, DO NOT remove from cache, else risk data corruption.
    // Background writer only removes after successful flush.
    node_to_evict->evict = true; // Mark for later eviction
    return;
  }

  if (node_to_evict->prev) {
    node_to_evict->prev->next = node_to_evict->next;
  } else {
    g_cache.lru_head = node_to_evict->next;
  }

  if (node_to_evict->next) {
    node_to_evict->next->prev = node_to_evict->prev;
  } else {
    g_cache.lru_tail = node_to_evict->prev;
  }

  HASH_DEL(g_cache.nodes_hash, node_to_evict);

  eng_cache_free_node(node_to_evict);

  g_cache.size--;
}

// --- Public API Implementations ---

void eng_cache_init(int capacity) {
  g_cache.capacity = capacity;
  g_cache.dirty_head = NULL;
  g_cache.dirty_tail = NULL;
  g_cache.lru_head = NULL;
  g_cache.lru_tail = NULL;
  g_cache.nodes_hash = NULL;
  g_cache.size = 0;
  uv_rwlock_init(&g_cache.lock);
  uv_mutex_init(&g_cache.dirty_list_lock);
}

void eng_cache_destroy() {
  uv_rwlock_wrlock(&g_cache.lock);
  eng_cache_node_t *n, *tmp_n;
  HASH_ITER(hh, g_cache.nodes_hash, n, tmp_n) {
    HASH_DEL(g_cache.nodes_hash, n);
    eng_cache_free_node(n);
  }
  uv_rwlock_wrunlock(&g_cache.lock);
  uv_rwlock_destroy(&g_cache.lock);
  uv_mutex_destroy(&g_cache.dirty_list_lock);
}

// Get unique cache key for node
static bool _get_cache_key(char *buffer, size_t buffer_size,
                           const char *container_name,
                           eng_user_dc_db_type_t db_type,
                           const db_key_t db_key) {
  int r = -1;
  if (db_key.type == DB_KEY_INTEGER) {
    r = snprintf(buffer, buffer_size, "%s:%d:%u", container_name, (int)db_type,
                 db_key.key.i);
  } else if (db_key.type == DB_KEY_STRING) {
    r = snprintf(buffer, buffer_size, "%s:%d:%s", container_name, (int)db_type,
                 db_key.key.s);
  } else {
    return false;
  }
  if (r < 0 || (size_t)r >= buffer_size) {
    return false;
  }
  return true;
}

eng_cache_node_t *
eng_cache_get_or_create(eng_container_t *c, eng_user_dc_db_type_t db_type,
                        db_key_t db_key, eng_cache_node_lock_type_t lock_type) {
  eng_cache_node_t *node = NULL;
  char cache_key[MAX_CACHE_KEY_SIZE];
  if (!_get_cache_key(cache_key, sizeof(cache_key), c->name, db_type, db_key)) {
    return NULL;
  }

  /*
  TODO: This is a bottleneck that serializes all cache access.
  Branch on lock_type?
  */
  uv_rwlock_wrlock(&g_cache.lock);

  HASH_FIND_STR(g_cache.nodes_hash, cache_key, node);

  if (node) {
    atomic_fetch_add(&node->ref_count, 1);
    _move_to_front(node);
    uv_rwlock_wrlock(
        &node->node_lock); // need this but then writes happen serially
    uv_rwlock_wrunlock(&g_cache.lock);
    return node;
  }

  if (g_cache.size >= g_cache.capacity) {
    // (Note: eviction might fail if the LRU node is in use)
    _evict_lru_node();
  }

  node = malloc(sizeof(eng_cache_node_t));
  if (!node) {
    uv_rwlock_wrunlock(&g_cache.lock);
    return NULL;
  }

  if (snprintf(node->container_name, sizeof(node->container_name), "%s",
               c->name) < 0 ||
      snprintf(node->cache_key, sizeof(node->cache_key), "%s", cache_key) < 0) {
    eng_cache_free_node(node);
    uv_rwlock_wrunlock(&g_cache.lock);
    return NULL;
  }
  node->db_type = db_type;
  node->db_key = db_key;
  if (db_key.type == DB_KEY_STRING) {
    node->db_key.key.s = strdup(db_key.key.s);
  }
  atomic_init(&node->ref_count, 1);
  node->current_version = 1;
  node->flush_version = 0;

  // Caller is responsible for loading data object!
  node->data_object = NULL;
  node->is_dirty = false;
  node->evict = false;
  node->is_flushing = false;
  uv_rwlock_init(&node->node_lock);

  HASH_ADD_KEYPTR(hh, g_cache.nodes_hash, node->cache_key,
                  strlen(node->cache_key), node);

  if (g_cache.lru_head) {
    node->next = g_cache.lru_head;
    g_cache.lru_head->prev = node;
    g_cache.lru_head = node;
  } else {
    g_cache.lru_head = node;
    g_cache.lru_tail = node;
  }

  g_cache.size++;

  // Important:
  // Before releasing the global lock, acquire the lock on the specific node.
  if (lock_type == CACHE_LOCK_WRITE) {
    uv_rwlock_wrlock(&node->node_lock);
  } else {
    uv_rwlock_rdlock(&node->node_lock);
  }

  // Unlock global cache lock
  uv_rwlock_wrunlock(&g_cache.lock);
  return node;
}

void eng_cache_unlock_and_release(eng_cache_node_t *node,
                                  eng_cache_node_lock_type_t lock_type) {
  if (!node)
    return;

  if (lock_type == CACHE_LOCK_WRITE) {
    uv_rwlock_wrunlock(&node->node_lock);
  } else {
    uv_rwlock_rdunlock(&node->node_lock);
  }

  atomic_fetch_sub(&node->ref_count, 1);
}

void eng_cache_cancel_and_release(eng_cache_node_t *node,
                                  eng_cache_node_lock_type_t lock_type) {
  if (!node)
    return;

  // Acquire the global lock first to ensure atomicity.
  uv_rwlock_wrlock(&g_cache.lock);

  if (lock_type == CACHE_LOCK_WRITE) {
    uv_rwlock_wrunlock(&node->node_lock);
  } else {
    uv_rwlock_rdunlock(&node->node_lock);
  }

  // Sanity check: only proceed if this is the only reference.
  if (node->ref_count == 1) {

    if (node->prev) {
      node->prev->next = node->next;
    } else {
      g_cache.lru_head = node->next;
    }

    if (node->next) {
      node->next->prev = node->prev;
    } else {
      g_cache.lru_tail = node->prev;
    }

    HASH_DEL(g_cache.nodes_hash, node);

    eng_cache_free_node(node);
  } else {
    atomic_fetch_sub(&node->ref_count, 1);
  }

  uv_rwlock_wrunlock(&g_cache.lock);
}

static void _eng_cache_add_to_dirty_list(eng_cache_node_t *node) {

  uv_mutex_lock(&g_cache.dirty_list_lock);

  node->dirty_prev = g_cache.dirty_tail;
  node->dirty_next = NULL;

  if (g_cache.dirty_tail) {
    g_cache.dirty_tail->dirty_next = node;
  }
  g_cache.dirty_tail = node;

  if (!g_cache.dirty_head) {
    g_cache.dirty_head = node;
  }

  uv_mutex_unlock(&g_cache.dirty_list_lock);
}

// Assumes caller has write lock on node
void eng_cache_mark_dirty(eng_cache_node_t *node) {
  if (!node)
    return;
  if (!node->is_dirty) {
    node->is_dirty = true;
    _eng_cache_add_to_dirty_list(node);
  }
  node->current_version++;
}

void eng_cache_remove_from_dirty_list(eng_cache_node_t *node) {
  uv_mutex_lock(&g_cache.dirty_list_lock);

  if (node->dirty_prev) {
    node->dirty_prev->dirty_next = node->dirty_next;
  } else {
    g_cache.dirty_head = node->dirty_next;
  }

  if (node->dirty_next) {
    node->dirty_next->dirty_prev = node->dirty_prev;
  } else {
    g_cache.dirty_tail = node->dirty_prev;
  }

  node->dirty_prev = NULL;
  node->dirty_next = NULL;

  uv_mutex_unlock(&g_cache.dirty_list_lock);
}

// Lock and swap pattern - very fast - used by background writer
eng_cache_node_t *eng_cache_swap_dirty_list() {
  uv_mutex_lock(&g_cache.dirty_list_lock);
  eng_cache_node_t *head = g_cache.dirty_head;
  g_cache.dirty_head = NULL;
  g_cache.dirty_tail = NULL;
  uv_mutex_unlock(&g_cache.dirty_list_lock);
  return head;
}
