#include "dc_cache.h"
#include "engine/container/container.h"

// TODO: re-write this for performance

// LRU cache for data containers //
static eng_dc_cache_t g_container_cache;

static void _reset_cache(int cap, create_container_func_t create_fn) {
  g_container_cache.nodes = NULL;
  g_container_cache.size = 0;
  g_container_cache.capacity = cap;
  g_container_cache.head = NULL;
  g_container_cache.tail = NULL;
  g_container_cache.create_fn = create_fn;
}

void eng_dc_cache_init(int capacity, create_container_func_t create_fn) {
  _reset_cache(capacity, create_fn);
  uv_mutex_init(&g_container_cache.lock);
}

static void _move_to_front(eng_dc_cache_node_t *n) {
  if (g_container_cache.head == n) {
    return;
  }
  if (n->prev) {
    n->prev->next = n->next;
  }
  if (n->next) {
    n->next->prev = n->prev;
  }
  if (g_container_cache.tail == n) {
    g_container_cache.tail = n->prev;
  }
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

// Call this when done with container:
// Decrement ref count for container
void eng_dc_cache_release_container(eng_container_t *c) {
  if (!c->name)
    return;
  eng_dc_cache_node_t *n = NULL;
  ;
  uv_mutex_lock(&g_container_cache.lock);
  HASH_FIND_STR(g_container_cache.nodes, c->name, n);
  if (n) {
    n->reference_count--;
  }
  uv_mutex_unlock(&g_container_cache.lock);
}

eng_container_t *eng_dc_cache_get(const char *name) {
  if (!g_container_cache.create_fn) {
    return NULL;
  }

  eng_dc_cache_node_t *n;
  uv_mutex_lock(&g_container_cache.lock);
  HASH_FIND_STR(g_container_cache.nodes, name, n);
  if (n) {
    n->reference_count++;
    _move_to_front(n);
    uv_mutex_unlock(&g_container_cache.lock);
    return n->c;
  };
  if (g_container_cache.size >= g_container_cache.capacity) {
    eng_dc_cache_node_t *evict_candidate = g_container_cache.tail;
    // IMPORTANT: Only evict if no one is using it!
    if (evict_candidate && evict_candidate->reference_count <= 0) {
      eng_container_close(evict_candidate->c);
      if (evict_candidate->prev) {
        evict_candidate->prev->next = evict_candidate->next;
      }
      if (evict_candidate->next) {
        evict_candidate->next->prev = evict_candidate->prev;
      }

      g_container_cache.tail = evict_candidate->prev;
      if (g_container_cache.tail) {
        g_container_cache.tail->next = NULL;
      } else {
        // list now empty
        g_container_cache.head = NULL;
      }

      HASH_DEL(g_container_cache.nodes, n);
      free(evict_candidate);
      g_container_cache.size--;
    }
  }

  n = malloc(sizeof(eng_dc_cache_node_t));
  if (!n) {
    uv_mutex_unlock(&g_container_cache.lock);
    return NULL;
  }
  eng_container_t *c = g_container_cache.create_fn(name);
  if (!c) {
    uv_mutex_unlock(&g_container_cache.lock);
    return NULL;
  }

  n->reference_count = 1;
  n->c = c;
  n->prev = NULL;
  n->next = g_container_cache.head;
  if (g_container_cache.head)
    g_container_cache.head->prev = n;
  g_container_cache.head = n;
  if (!g_container_cache.tail)
    g_container_cache.tail = n;
  HASH_ADD_KEYPTR(hh, g_container_cache.nodes, c->name, strlen(c->name), n);
  g_container_cache.size++;
  uv_mutex_unlock(&g_container_cache.lock);
  return c;
}

void eng_dc_cache_destroy() {
  uv_mutex_lock(&g_container_cache.lock);
  eng_dc_cache_node_t *n, *tmp;
  if (g_container_cache.nodes) {
    HASH_ITER(hh, g_container_cache.nodes, n, tmp) {
      eng_container_close(n->c);

      HASH_DEL(g_container_cache.nodes, n);
      free(n);
    }
  }
  _reset_cache(0, NULL);
  uv_mutex_unlock(&g_container_cache.lock);
}
