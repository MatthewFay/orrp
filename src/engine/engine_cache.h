#ifndef ENGINE_CACHE_H
#define ENGINE_CACHE_H

// --- ENGINE CACHE --- //

#include "container.h"
#include "core/db.h"
#include "uthash.h"
#include "uv.h" // IWYU pragma: keep
#include <stdatomic.h>
#include <stdbool.h>

#define MAX_CACHE_KEY_SIZE 640

typedef enum {
  CACHE_TYPE_BITMAP,
  CACHE_TYPE_UINT32,
  CACHE_TYPE_STRING
} eng_cache_node_type_t;

typedef enum { CACHE_LOCK_READ, CACHE_LOCK_WRITE } eng_cache_node_lock_type_t;

// The unified cache node. It's a member of a hash map, an LRU list,
// and a dirty list all at once.
typedef struct eng_cache_node_s {
  char container_name[128];
  eng_user_dc_db_type_t db_type;
  db_key_t db_key;

  // --- Hash Map Fields ---
  char cache_key[MAX_CACHE_KEY_SIZE]; // Unique Composite Cache Key:
                                      // "container_name/db_type/db_key".
                                      // Must be heap-allocated.

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
  atomic_uint_fast32_t ref_count; // Tracks current users of the node.
  bool is_dirty;                  // Has `data_object` been modified?
  bool is_flushing;               // Prevent double-flush
  bool evict;                     // Mark for post-flush eviction
  uint32_t current_version;       // Incremented on every modification
  uint32_t flush_version;

  uv_rwlock_t node_lock;
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
void eng_cache_destroy(void);

// Gets a node from the cache by key. If the node exists, its ref count
// is incremented. If it does not exist, a new node is created and returned,
// but its data_object will be NULL.
eng_cache_node_t *eng_cache_get_or_create(eng_container_t *c,
                                          eng_user_dc_db_type_t db_type,
                                          db_key_t db_key,
                                          eng_cache_node_lock_type_t lock_type);

// Releases a node that was previously acquired with cache_get_or_create.
// This unlocks the node and decrements the reference count.
void eng_cache_unlock_and_release(eng_cache_node_t *node,
                                  eng_cache_node_lock_type_t lock_type);

// Removes a node from the cache if it was newly created but could not be
// populated with data. This should only be called on a node with a
// reference count of 1.
void eng_cache_cancel_and_release(eng_cache_node_t *node,
                                  eng_cache_node_lock_type_t lock_type);

/**
 * @brief Marks a node as dirty and adds it to the dirty list if it's not
 * already there.
 *
 * This is the single, safe entry point for making a node eligible for
 * persistence.
 */
void eng_cache_mark_dirty(eng_cache_node_t *node);

// Removes a node from the dirty list (e.g., after it's been flushed).
void eng_cache_remove_from_dirty_list(eng_cache_node_t *node);

eng_cache_node_t *eng_cache_swap_dirty_list();

void eng_cache_free_node(eng_cache_node_t *node);

#endif // ENGINE_CACHE_H