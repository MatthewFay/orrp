#include "container.h"
#include "container_cache.h"
#include "container_db.h"
#include "engine/container/container_types.h"
#include "uv.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static struct {
  bool initialized;
  container_cache_t *cache;
  uv_rwlock_t cache_rwlock;
  eng_container_t *system_container;
  char *data_dir;
  size_t initial_container_size;
} g_container_state = {0};

static bool _ensure_data_dir_exists(const char *dir) {
  struct stat st = {0};
  if (stat(dir, &st) == -1) {
    if (mkdir(dir, 0755) != 0) {
      return false;
    }
  }
  return true;
}

// ============================================================================
// Public API Implementation
// ============================================================================

bool container_init(size_t cache_capacity, const char *data_dir,
                    size_t initial_container_size_bytes) {
  if (g_container_state.initialized) {
    return false;
  }

  if (!data_dir || cache_capacity == 0 || initial_container_size_bytes == 0) {
    return false;
  }

  if (uv_rwlock_init(&g_container_state.cache_rwlock) != 0) {
    return false;
  }

  if (!_ensure_data_dir_exists(data_dir)) {
    return false;
  }

  g_container_state.data_dir = strdup(data_dir);
  if (!g_container_state.data_dir) {
    return false;
  }

  g_container_state.initial_container_size = initial_container_size_bytes;

  g_container_state.cache = container_cache_create(cache_capacity);
  if (!g_container_state.cache) {
    free(g_container_state.data_dir);
    return false;
  }

  container_result_t sys_result = create_system_container(
      g_container_state.data_dir, g_container_state.initial_container_size);
  if (!sys_result.success) {
    container_cache_destroy(g_container_state.cache);
    free(g_container_state.data_dir);
    return false;
  }

  g_container_state.system_container = sys_result.container;
  g_container_state.initialized = true;

  return true;
}

static bool _container_evict_lru(container_cache_t *cache) {
  container_cache_node_t *node = cache->tail;
  while (node) {
    if (atomic_load(&node->reference_count) == 0) {
      // Future optimization: close container outside of lock
      container_close(node->container);
      container_cache_remove(cache, node);
      return true;
    }
    node = node->prev;
  }
  return false;
}

void container_shutdown(void) {
  if (!g_container_state.initialized) {
    return;
  }

  if (g_container_state.cache) {
    uv_rwlock_wrlock(&g_container_state.cache_rwlock);

    while (_container_evict_lru(g_container_state.cache)) {
    }

    uv_rwlock_wrunlock(&g_container_state.cache_rwlock);
    container_cache_destroy(g_container_state.cache);
    g_container_state.cache = NULL;
    uv_rwlock_destroy(&g_container_state.cache_rwlock);
  }

  if (g_container_state.system_container) {
    container_close(g_container_state.system_container);
    g_container_state.system_container = NULL;
  }

  free(g_container_state.data_dir);
  g_container_state.data_dir = NULL;
  g_container_state.initialized = false;
}

static container_cache_node_t *_create_cache_node(eng_container_t *c) {
  if (!c)
    return NULL;
  container_cache_node_t *node = calloc(1, sizeof(container_cache_node_t));
  if (!node) {
    return NULL;
  }
  node->container = c;
  atomic_init(&node->reference_count, 1);
  return node;
}

container_result_t container_get_or_create_user(const char *name) {
  container_result_t result = {0};

  if (!g_container_state.initialized) {
    result.error_code = CONTAINER_ERR_NOT_INITIALIZED;
    result.error_msg = "Container subsystem not initialized";
    return result;
  }

  if (!name || strlen(name) == 0 || strcmp(name, SYS_CONTAINER_NAME) == 0) {
    result.error_code = CONTAINER_ERR_INVALID_NAME;
    result.error_msg = "Invalid container name";
    return result;
  }

  uv_rwlock_rdlock(&g_container_state.cache_rwlock);
  container_cache_node_t *node =
      container_cache_get(g_container_state.cache, name);

  if (node) {
    atomic_fetch_add(&node->reference_count, 1);
    result.success = true;
    result.container = node->container;

    if (g_container_state.cache->head == node) {
      uv_rwlock_rdunlock(&g_container_state.cache_rwlock);
      return result;
    }
    uv_rwlock_rdunlock(&g_container_state.cache_rwlock);
    uv_rwlock_wrlock(&g_container_state.cache_rwlock);
    container_cache_move_to_front(g_container_state.cache, node);
    uv_rwlock_wrunlock(&g_container_state.cache_rwlock);
    return result;
  }

  uv_rwlock_rdunlock(&g_container_state.cache_rwlock);
  uv_rwlock_wrlock(&g_container_state.cache_rwlock);

  // Double-check after acquiring write lock (another thread may have loaded it)
  node = container_cache_get(g_container_state.cache, name);
  if (node) {
    atomic_fetch_add(&node->reference_count, 1);
    result.success = true;
    result.container = node->container;
    // Since node was just loaded by another thread, don't bother moving it to
    // front
    uv_rwlock_wrunlock(&g_container_state.cache_rwlock);
    return result;
  }

  if (g_container_state.cache->size >= g_container_state.cache->capacity) {
    _container_evict_lru(g_container_state.cache);
  }

  container_result_t create_result =
      create_user_container(name, g_container_state.data_dir,
                            g_container_state.initial_container_size);
  if (!create_result.success) {
    uv_rwlock_wrunlock(&g_container_state.cache_rwlock);
    return create_result;
  }

  node = _create_cache_node(create_result.container);
  if (!node) {
    uv_rwlock_wrunlock(&g_container_state.cache_rwlock);
    container_close(create_result.container);
    result.error_code = CONTAINER_ERR_ALLOC;
    result.error_msg = "Failed to add container to cache";
    return result;
  }

  if (!container_cache_put(g_container_state.cache, node)) {
    uv_rwlock_wrunlock(&g_container_state.cache_rwlock);
    container_close(create_result.container);
    free(node);
    result.error_code = CONTAINER_ERR_ALLOC;
    result.error_msg = "Failed to add container to cache";
    return result;
  }

  uv_rwlock_wrunlock(&g_container_state.cache_rwlock);

  create_result.container->_node = node;

  result.success = true;
  result.container = create_result.container;
  return result;
}

container_result_t container_get_system(void) {
  container_result_t result = {0};

  if (!g_container_state.initialized) {
    result.error_code = CONTAINER_ERR_NOT_INITIALIZED;
    result.error_msg = "Container subsystem not initialized";
    return result;
  }

  result.success = true;
  result.container = g_container_state.system_container;
  return result;
}

void container_release(eng_container_t *container) {
  if (!g_container_state.initialized || !container || !container->_node) {
    return;
  }

  // Only cached user containers need to be released
  // System container is never released
  if (container->type == CONTAINER_TYPE_USR) {
    atomic_fetch_sub(&container->_node->reference_count, 1);
  }
}

bool container_get_user_db_handle(eng_container_t *c,
                                  eng_dc_user_db_type_t db_type,
                                  MDB_dbi *db_out) {
  return cdb_get_user_db_handle(c, db_type, db_out);
}

bool container_get_system_db_handle(eng_container_t *c,
                                    eng_dc_sys_db_type_t db_type,
                                    MDB_dbi *db_out) {
  return cdb_get_system_db_handle(c, db_type, db_out);
}

bool container_get_db_handle(eng_container_t *c, eng_container_db_key_t *db_key,
                             MDB_dbi *db_out) {
  if (!c || !db_key || !db_out) {
    return false;
  }
  if (db_key->dc_type == CONTAINER_TYPE_SYS) {
    return cdb_get_system_db_handle(c, db_key->sys_db_type, db_out);
  }
  return cdb_get_user_db_handle(c, db_key->usr_db_type, db_out);
}

void container_free_db_key_contents(eng_container_db_key_t *db_key) {
  cdb_free_db_key_contents(db_key);
}
