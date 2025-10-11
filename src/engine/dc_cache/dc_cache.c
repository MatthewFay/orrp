#include "dc_cache.h"
#include "engine/container/container.h"
#include <stdatomic.h>
#include <string.h>

static eng_dc_cache_t g_container_cache;

static void _reset_cache(int cap, create_container_func_t create_fn) {
  g_container_cache.nodes = NULL;
  g_container_cache.size = 0;
  g_container_cache.capacity = cap;
  g_container_cache.head = NULL;
  g_container_cache.tail = NULL;
  g_container_cache.create_fn = create_fn;
}

bool eng_dc_cache_init(int capacity, create_container_func_t create_fn) {
  _reset_cache(capacity, create_fn);
  if (uv_rwlock_init(&g_container_cache.rwlock) != 0) {
    return false;
  }
  return true;
}

// Move node to front of LRU list
// MUST be called with write lock held
static inline void _move_to_front(eng_dc_cache_node_t *n) {
  if (g_container_cache.head == n) {
    return;
  }

  // Remove from current position
  if (n->prev) {
    n->prev->next = n->next;
  }
  if (n->next) {
    n->next->prev = n->prev;
  }
  if (g_container_cache.tail == n) {
    g_container_cache.tail = n->prev;
  }

  // Insert at head
  n->next = g_container_cache.head;
  n->prev = NULL;
  if (g_container_cache.head) {
    g_container_cache.head->prev = n;
  }
  g_container_cache.head = n;
  if (!g_container_cache.tail) {
    g_container_cache.tail = n;
  }
}

// Evict LRU node with reference_count == 0
// MUST be called with write lock held
// Returns true if eviction happened
static bool _evict_lru(void) {
  eng_dc_cache_node_t *candidate = g_container_cache.tail;

  // Walk backwards from tail to find evictable node
  while (candidate) {
    if (atomic_load(&candidate->reference_count) == 0) {
      // Found evictable node

      // Remove from LRU list
      if (candidate->prev) {
        candidate->prev->next = candidate->next;
      } else {
        g_container_cache.head = candidate->next;
      }

      if (candidate->next) {
        candidate->next->prev = candidate->prev;
      } else {
        g_container_cache.tail = candidate->prev;
      }

      // Remove from hash table
      HASH_DEL(g_container_cache.nodes, candidate);

      // Close container and free node
      eng_container_close(candidate->c);
      free(candidate);

      g_container_cache.size--;
      return true;
    }
    candidate = candidate->prev;
  }

  return false; // No evictable nodes found
}

void eng_dc_cache_release_container(eng_container_t *c) {
  if (!c || !c->name)
    return;

  // Fast path: atomic decrement without lock
  // We only need read lock to find the node
  uv_rwlock_rdlock(&g_container_cache.rwlock);
  eng_dc_cache_node_t *n = NULL;
  HASH_FIND_STR(g_container_cache.nodes, c->name, n);
  uv_rwlock_rdunlock(&g_container_cache.rwlock);

  if (n) {
    atomic_fetch_sub(&n->reference_count, 1);
  }
}

eng_container_t *eng_dc_cache_get(const char *name) {
  if (!g_container_cache.create_fn || !name) {
    return NULL;
  }

  // Fast path: try read lock first for cache hit
  uv_rwlock_rdlock(&g_container_cache.rwlock);
  eng_dc_cache_node_t *n = NULL;
  HASH_FIND_STR(g_container_cache.nodes, name, n);

  if (n) {
    // Cache hit - increment ref count and return
    atomic_fetch_add(&n->reference_count, 1);
    uv_rwlock_rdunlock(&g_container_cache.rwlock);

    // Move to front requires write lock
    uv_rwlock_wrlock(&g_container_cache.rwlock);
    _move_to_front(n);
    uv_rwlock_wrunlock(&g_container_cache.rwlock);

    return n->c;
  }

  // Cache miss - upgrade to write lock
  uv_rwlock_rdunlock(&g_container_cache.rwlock);
  uv_rwlock_wrlock(&g_container_cache.rwlock);

  // Double-check after acquiring write lock (another thread may have loaded it)
  HASH_FIND_STR(g_container_cache.nodes, name, n);
  if (n) {
    atomic_fetch_add(&n->reference_count, 1);
    _move_to_front(n);
    uv_rwlock_wrunlock(&g_container_cache.rwlock);
    return n->c;
  }

  // Still not found - need to load from disk

  // Try to evict if at capacity
  if (g_container_cache.size >= g_container_cache.capacity) {
    if (!_evict_lru()) {
      // Could not evict - cache is full with all entries in use
      // Option 1: Fail the request
      // Option 2: Allow cache to grow beyond capacity
      // For now, we'll allow it to grow
    }
  }

  // Create new node
  n = malloc(sizeof(eng_dc_cache_node_t));
  if (!n) {
    uv_rwlock_wrunlock(&g_container_cache.rwlock);
    return NULL;
  }

  // Load container from disk
  eng_container_t *c = g_container_cache.create_fn(name);
  if (!c) {
    free(n);
    uv_rwlock_wrunlock(&g_container_cache.rwlock);
    return NULL;
  }

  // Initialize node
  n->c = c;
  atomic_init(&n->reference_count, 1);
  n->prev = NULL;
  n->next = g_container_cache.head;

  // Insert at head of LRU list
  if (g_container_cache.head) {
    g_container_cache.head->prev = n;
  }
  g_container_cache.head = n;
  if (!g_container_cache.tail) {
    g_container_cache.tail = n;
  }

  // Add to hash table
  HASH_ADD_KEYPTR(hh, g_container_cache.nodes, c->name, strlen(c->name), n);
  g_container_cache.size++;

  uv_rwlock_wrunlock(&g_container_cache.rwlock);
  return c;
}

void eng_dc_cache_destroy(void) {
  uv_rwlock_wrlock(&g_container_cache.rwlock);

  eng_dc_cache_node_t *n, *tmp;
  HASH_ITER(hh, g_container_cache.nodes, n, tmp) {
    eng_container_close(n->c);
    HASH_DEL(g_container_cache.nodes, n);
    free(n);
  }

  _reset_cache(0, NULL);
  uv_rwlock_wrunlock(&g_container_cache.rwlock);
  uv_rwlock_destroy(&g_container_cache.rwlock);
}