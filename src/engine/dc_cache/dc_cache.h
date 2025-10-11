#ifndef DATA_CONTAINER_CACHE_H
#define DATA_CONTAINER_CACHE_H

#include "engine/container/container.h"
#include "uthash.h"
#include "uv.h" // IWYU pragma: keep

// Function pointer for container creation
typedef eng_container_t *(*create_container_func_t)(const char *name);

typedef struct eng_dc_cache_node_s {
  eng_container_t *c;
  _Atomic(int) reference_count; // Atomic for lock-free increment during reads
  struct eng_dc_cache_node_s *prev;
  struct eng_dc_cache_node_s *next;
  UT_hash_handle hh;
} eng_dc_cache_node_t;

typedef struct eng_dc_cache_s {
  int size;
  int capacity;
  create_container_func_t create_fn;
  // Hash map for O(1) lookups by name
  eng_dc_cache_node_t *nodes;
  // Doubly-linked list for LRU ordering
  eng_dc_cache_node_t *head;
  eng_dc_cache_node_t *tail;
  // RW lock: many readers, single writer
  uv_rwlock_t rwlock;
} eng_dc_cache_t;

// Initialize the data container cache
bool eng_dc_cache_init(int capacity, create_container_func_t create_fn);

// Get a data container either from the cache or disk
eng_container_t *eng_dc_cache_get(const char *name);

// Call this when done with container
void eng_dc_cache_release_container(eng_container_t *c);

// Destroy data container cache
void eng_dc_cache_destroy(void);

#endif // DATA_CONTAINER_CACHE_H