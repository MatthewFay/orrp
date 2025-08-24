#include "engine_cache.h"
#include <stdlib.h>
#include <string.h>

// Global instance of our cache
static eng_cache_mgr_t g_cache;

// Moves a given node to the front of the LRU list.
static void _move_to_front(eng_cache_node_t *node) {
  // 1. If node is already the head, do nothing.
  // 2. Unlink the node from its current position in the LRU list
  //    by connecting its prev and next nodes to each other.
  //    (Be careful to handle the case where node is the tail).
  // 3. Set the node's prev to NULL and its next to the current head.
  // 4. Update the old head's prev to point to the new node.
  // 5. Update the global g_cache.lru_head to be the new node.
}

// Evicts the least recently used node if it's not in use.
static void _evict_lru_node() {
  // 1. Get the LRU node from g_cache.lru_tail.
  // 2. If the tail exists AND its reference_count is 0:
  //    a. Unlink it from the LRU list.
  //    b. Remove it from the hash map using HASH_DEL.
  //    c. Free the data_object (e.g., bitmap_free(node->data_object)).
  //    d. Free the node's key string.
  //    e. Free the node struct itself.
  //    f. Decrement g_cache.size.
  // 3. (Phase 2 refinement) If the node is dirty, do not free it.
  //    Instead, just unlink it from the main cache and leave it
  //    on the dirty list for the background writer to handle.
}

// --- Public API Implementations ---

void cache_init(int capacity) {
  // 1. Initialize all pointers in g_cache to NULL.
  // 2. Set g_cache.size to 0 and g_cache.capacity to the provided capacity.
  // 3. Initialize the reader-writer lock with uv_rwlock_init(&g_cache.lock).
}

void cache_destroy() {
  // 1. Acquire a write lock.
  // 2. Iterate through the entire hash map using HASH_ITER.
  // 3. For each node, free its data_object, key, and the node struct itself.
  // 4. Destroy the reader-writer lock with uv_rwlock_destroy.
}

eng_cache_node_t *cache_get_or_create(const char *key) {
  // 1. Acquire a write lock with uv_rwlock_wrlock.
  // 2. Look up the node in the hash map using HASH_FIND_STR.

  // 3. --- CACHE HIT ---
  //    If the node is found:
  //    a. Increment its reference_count.
  //    b. Call _move_to_front(node).
  //    c. Release the write lock.
  //    d. Return the found node.

  // 4. --- CACHE MISS ---
  //    If the node is not found:
  //    a. Check if g_cache.size >= g_cache.capacity.
  //    b. If full, call _evict_lru_node(). (Note: eviction might fail if the
  //    LRU node is in use).
  //       If you still have no space, you might have to return NULL or handle
  //       the error.

  //    c. Allocate memory for a new eng_cache_node_t.
  //    d. Initialize its members:
  //       - key = strdup(key)
  //       - reference_count = 1
  //       - data_object = NULL (The caller is responsible for loading the data)
  //       - is_dirty = false
  //    e. Add the new node to the hash map using HASH_ADD_KEYPTR.
  //    f. Add the new node to the front of the LRU list (update head/tail
  //    pointers). g. Increment g_cache.size. h. Release the write lock. i.
  //    Return the new node.
}

void cache_release(eng_cache_node_t *node) {
  // 1. Acquire a write lock.
  // 2. Decrement the node's reference_count.
  // 3. Release the write lock.
}