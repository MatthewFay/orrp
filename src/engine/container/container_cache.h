#ifndef CONTAINER_CACHE_H
#define CONTAINER_CACHE_H

#include "container_types.h"
#include "uv.h" // IWYU pragma: keep
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

container_cache_t *container_cache_create(size_t capacity);

void container_cache_destroy(container_cache_t *cache);

container_cache_node_t *container_cache_get(container_cache_t *cache,
                                            const char *name);

bool container_cache_put(container_cache_t *cache,
                         container_cache_node_t *node);

void container_cache_move_to_front(container_cache_t *cache,
                                   container_cache_node_t *node);

// Frees the node
bool container_cache_remove(container_cache_t *cache,
                            container_cache_node_t *node);

#endif // CONTAINER_CACHE_H