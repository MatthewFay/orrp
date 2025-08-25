#include "engine_cache.h"
#include "core/bitmaps.h"
#include "uthash.h"
#include "uv.h"
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
  default:
    break;
  }
}

static void _free_node(eng_cache_node_t *node) {
  _free_data_obj(node);
  free(node->key);
  free(node);
}

// Evicts the least recently used node if it's not in use.
static void _evict_lru_node() {
  eng_cache_node_t *node_to_evict = g_cache.lru_tail;
  if (!node_to_evict || node_to_evict->ref_count > 0) {
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

  _free_node(node_to_evict);

  g_cache.size--;

  // (Phase 2 refinement) If the node is dirty, do not free it.
  //    Instead, just unlink it from the main cache and leave it
  //    on the dirty list for the background writer to handle.
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
}

void eng_cache_destroy() {
  uv_rwlock_wrlock(&g_cache.lock);
  eng_cache_node_t *n, *tmp_n;
  HASH_ITER(hh, g_cache.nodes_hash, n, tmp_n) {
    HASH_DEL(g_cache.nodes_hash, n);
    _free_node(n);
  }
  uv_rwlock_wrunlock(&g_cache.lock);
  uv_rwlock_destroy(&g_cache.lock);
}

eng_cache_node_t *eng_cache_get_or_create(const char *key) {
  /*
  Consider optimizing in future
  */
  uv_rwlock_wrlock(&g_cache.lock);

  eng_cache_node_t *node = NULL;

  HASH_FIND_STR(g_cache.nodes_hash, key, node);

  if (node) {
    node->ref_count++;
    _move_to_front(node);
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

  node->key = strdup(key);
  node->ref_count = 1;
  // Caller is responsible for loading data object!
  node->data_object = NULL;
  node->is_dirty = false;

  HASH_ADD_KEYPTR(hh, g_cache.nodes_hash, node->key, strlen(node->key), node);

  if (g_cache.lru_head) {
    node->next = g_cache.lru_head;
    g_cache.lru_head->prev = node;
    g_cache.lru_head = node;
  } else {
    g_cache.lru_head = node;
    g_cache.lru_tail = node;
  }

  g_cache.size++;
  uv_rwlock_wrunlock(&g_cache.lock);
  return node;
}

void eng_cache_release(eng_cache_node_t *node) {
  uv_rwlock_wrlock(&g_cache.lock);
  node->ref_count--;
  uv_rwlock_wrunlock(&g_cache.lock);
}