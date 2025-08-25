#ifndef ENGINE_CACHE_H
#define ENGINE_CACHE_H

#include "uthash.h"
#include "uv.h" // IWYU pragma: keep
#include <stdbool.h>

typedef enum { CACHE_TYPE_BITMAP, CACHE_TYPE_UINT32 } eng_cache_node_type_t;

// The unified cache node. It's a member of a hash map, an LRU list,
// and a dirty list all at once.
typedef struct eng_cache_node_s {
  // --- Hash Map Fields ---
  char *key; // Key: "container:db:key". Must be heap-allocated.
  UT_hash_handle hh;

  // --- LRU List Fields ---
  struct eng_cache_node_s *prev;
  struct eng_cache_node_s *next;

  // --- Dirty List Fields ---
  struct eng_cache_node_s *dirty_prev;
  struct eng_cache_node_s *dirty_next;

  // --- Data Payload & State ---
  eng_cache_node_type_t type;
  void *data_object; // Pointer to the actual data (e.g., bitmap_t*).
  int ref_count;     // Tracks current users of the node.
  bool is_dirty;     // Has `data_object` been modified?
} eng_cache_node_t;

// The main manager struct for the entire cache.
typedef struct eng_cache_mgr_s {
  int size;
  int capacity;

  // --- Pointers to Heads/Tails of our data structures ---
  eng_cache_node_t *nodes_hash; // Head pointer for the hash map.

  eng_cache_node_t *lru_head; // Head of the LRU list (most recently used).
  eng_cache_node_t *lru_tail; // Tail of the LRU list (least recently used).

  eng_cache_node_t *dirty_head; // Head of the dirty list.
  eng_cache_node_t *dirty_tail; // Tail of the dirty list.

  // --- Concurrency Control ---
  uv_rwlock_t lock; // A single RW lock to protect all cache operations.

  uv_mutex_t dirty_list_lock;
} eng_cache_mgr_t;

// --- Public API Functions ---

// Initializes the global cache manager. Must be called once at startup.
void eng_cache_init(int capacity);

// Destroys the cache, freeing all nodes and data. Called on shutdown.
void eng_cache_destroy();

// Gets a node from the cache by key. If the node exists, its ref count
// is incremented. If it does not exist, a new node is created and returned,
// but its data_object will be NULL.
eng_cache_node_t *eng_cache_get_or_create(const char *key);

// Releases a node that was previously acquired with cache_get_or_create.
// This decrements the reference count.
void eng_cache_release(eng_cache_node_t *node);

// Appends a node to the tail of the dirty list.
// This function assumes the caller has already checked the is_dirty flag.
// It does not need to be called if the node is already on the list.
void eng_cache_add_to_dirty_list(eng_cache_node_t *node);

// Removes a node from the dirty list (e.g., after it's been flushed).
void eng_cache_remove_from_dirty_list(eng_cache_node_t *node);
#endif // ENGINE_CACHE_H