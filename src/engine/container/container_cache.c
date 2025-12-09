#include "container_cache.h"
#include "engine/container/container_types.h"
#include "uthash.h"
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

container_cache_t *container_cache_create(size_t capacity) {
  container_cache_t *cache = calloc(1, sizeof(container_cache_t));
  if (!cache) {
    return NULL;
  }

  cache->capacity = capacity;
  cache->size = 0;
  cache->nodes = NULL;
  cache->head = NULL;
  cache->tail = NULL;

  return cache;
}

void container_cache_destroy(container_cache_t *cache) {
  if (!cache) {
    return;
  }

  container_cache_node_t *node, *tmp;
  HASH_ITER(hh, cache->nodes, node, tmp) {
    HASH_DEL(cache->nodes, node);
    free(node);
  }

  free(cache);
}

container_cache_node_t *container_cache_get(container_cache_t *cache,
                                            const char *name) {
  if (!cache || !name) {
    return NULL;
  }

  container_cache_node_t *node = NULL;
  HASH_FIND_STR(cache->nodes, name, node);

  return node;
}

bool container_cache_put(container_cache_t *cache,
                         container_cache_node_t *node) {
  if (!cache || !node || !node->container) {
    return false;
  }

  node->prev = NULL;
  node->next = cache->head;

  if (cache->head) {
    cache->head->prev = node;
  }
  cache->head = node;
  if (!cache->tail) {
    cache->tail = node;
  }

  HASH_ADD_KEYPTR(hh, cache->nodes, node->container->name,
                  strlen(node->container->name), node);
  cache->size++;

  return true;
}

void container_cache_move_to_front(container_cache_t *cache,
                                   container_cache_node_t *node) {
  if (!cache || !node) {
    return;
  }

  if (node->prev) {
    node->prev->next = node->next;
  }
  if (node->next) {
    node->next->prev = node->prev;
  }
  if (cache->tail == node) {
    cache->tail = node->prev;
  }

  node->next = cache->head;
  node->prev = NULL;
  if (cache->head) {
    cache->head->prev = node;
  }
  cache->head = node;
  if (!cache->tail) {
    cache->tail = node;
  }
}

// Frees the node
bool container_cache_remove(container_cache_t *cache,
                            container_cache_node_t *node) {
  if (!cache || !node) {
    return false;
  }

  if (node->prev) {
    node->prev->next = node->next;
  } else {
    cache->head = node->next;
  }

  if (node->next) {
    node->next->prev = node->prev;
  } else {
    cache->tail = node->prev;
  }

  HASH_DEL(cache->nodes, node);
  free(node);

  cache->size--;

  return true;
}
