#include "entity_resolver.h"
#include "core/db.h"
#include "id_manager.h"
#include "lmdb.h"
#include "uthash.h"
#include "uv.h"
// #include "wal.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// --- Internal Data Structures ---

typedef struct er_cache_node_s {
  // --- Hash Map Keys & Data ---
  char *string_id;
  uint32_t int_id;

  // --- Hash Map Handles ---
  UT_hash_handle
      hh_str; // Handle for the string-keyed map (ent string id -> ent int id)
  UT_hash_handle
      hh_int; // Handle for the integer-keyed map (ent int id -> ent string id)

  // --- LRU List Pointers ---
  struct er_cache_node_s *prev;
  struct er_cache_node_s *next;
} er_cache_node_t;

// A self-contained struct for the background writer.
// It holds copies of the data, completely decoupling it from the cache nodes.
typedef struct er_dirty_item_s {
  char *string_id; // A copy of the string id
  uint32_t int_id;
  struct er_dirty_item_s *next;
} er_dirty_item_t;

// The main manager for the resolver's state
typedef struct {
  u_int32_t size;
  u_int32_t capacity;

  // --- Hash Maps ---
  er_cache_node_t *str_to_int_map;
  er_cache_node_t *int_to_str_map;

  // --- LRU List ---
  er_cache_node_t *lru_head;
  er_cache_node_t *lru_tail;

  // --- Dirty List ---
  er_dirty_item_t *dirty_head;

  // --- Concurrency ---
  uv_rwlock_t cache_lock;     // Protects the hash maps and LRU list
  uv_mutex_t dirty_list_lock; // Protects the dirty list
} er_manager_t;

typedef struct er_dirty_list_s {
  er_cache_node_t *head;
} er_dirty_list_t;

// Global instance of the resolver's state
static er_manager_t g_resolver;

// --- Private LRU Cache Helpers ---

/**
 * @brief Unlinks a node from the LRU doubly-linked list.
 * Assumes a write lock is already held.
 */
static void _lru_remove(er_cache_node_t *node) {
  if (node->prev) {
    node->prev->next = node->next;
  } else {
    g_resolver.lru_head = node->next;
  }
  if (node->next) {
    node->next->prev = node->prev;
  } else {
    g_resolver.lru_tail = node->prev;
  }
}

/**
 * @brief Adds a node to the front of the LRU list (most recently used).
 * Assumes a write lock is already held.
 */
static void _lru_add_to_front(er_cache_node_t *node) {
  node->next = g_resolver.lru_head;
  node->prev = NULL;
  if (g_resolver.lru_head) {
    g_resolver.lru_head->prev = node;
  }
  g_resolver.lru_head = node;
  if (!g_resolver.lru_tail) {
    g_resolver.lru_tail = node;
  }
}

/**
 * @brief Evicts the least recently used item from the cache.
 * Assumes a write lock is already held.
 */
static void _lru_evict() {
  if (!g_resolver.lru_tail)
    return;

  er_cache_node_t *node_to_evict = g_resolver.lru_tail;

  _lru_remove(node_to_evict);

  HASH_DELETE(hh_str, g_resolver.str_to_int_map, node_to_evict);
  HASH_DELETE(hh_int, g_resolver.int_to_str_map, node_to_evict);

  free(node_to_evict->string_id);
  free(node_to_evict);
  g_resolver.size--;
}

/**
 * @brief Creates a new cache node and adds it to the cache (maps & LRU).
 * Handles eviction if the cache is at capacity.
 * Assumes a write lock is already held.
 */
static er_cache_node_t *_create_cache_node(const char *str_id,
                                           uint32_t int_id) {
  if (g_resolver.size >= g_resolver.capacity) {
    _lru_evict();
  }

  er_cache_node_t *node = calloc(1, sizeof(er_cache_node_t));
  if (!node)
    return NULL;

  node->string_id = strdup(str_id);
  if (!node->string_id) {
    free(node);
    return NULL;
  }
  node->int_id = int_id;

  HASH_ADD_KEYPTR(hh_str, g_resolver.str_to_int_map, node->string_id,
                  strlen(node->string_id), node);
  HASH_ADD(hh_int, g_resolver.int_to_str_map, int_id, sizeof(uint32_t), node);

  _lru_add_to_front(node);
  g_resolver.size++;

  return node;
}

// --- Public API Implementation ---

void entity_resolver_init(eng_context_t *ctx, int capacity) {
  memset(&g_resolver, 0, sizeof(er_manager_t));
  g_resolver.capacity = capacity;
  uv_rwlock_init(&g_resolver.cache_lock);
  uv_mutex_init(&g_resolver.dirty_list_lock);
  // TODO: "Warm" the cache by loading some or all
  // existing mappings from the system container
  (void)ctx;
}

void entity_resolver_destroy() {
  er_cache_node_t *n, *tmp;
  uv_rwlock_wrlock(&g_resolver.cache_lock);

  HASH_ITER(hh_str, g_resolver.str_to_int_map, n, tmp) {
    HASH_DELETE(hh_str, g_resolver.str_to_int_map, n);
    // No need to delete from hh_int, as uthash iterates and we free the node
    free(n->string_id);
    free(n);
  }
  uv_rwlock_wrunlock(&g_resolver.cache_lock);

  uv_rwlock_destroy(&g_resolver.cache_lock);

  entity_resolver_free_dirty_list(g_resolver.dirty_head);
  uv_mutex_destroy(&g_resolver.dirty_list_lock);
}

bool entity_resolver_resolve_id(eng_container_t *sys_c,
                                const char *entity_id_str,
                                uint32_t *int_id_out) {
  uv_rwlock_rdlock(&g_resolver.cache_lock);
  er_cache_node_t *node;
  size_t key_len = strlen(entity_id_str);
  HASH_FIND(hh_str, g_resolver.str_to_int_map, entity_id_str, key_len, node);
  if (node) {
    *int_id_out = node->int_id;
    // NOTE: Per our design, we don't move_to_front here to allow for
    // high read concurrency. This is an "approximate" LRU.
    uv_rwlock_rdunlock(&g_resolver.cache_lock);
    return true;
  }
  uv_rwlock_rdunlock(&g_resolver.cache_lock);

  // --- Acquire WRITE lock to potentially modify the cache
  uv_rwlock_wrlock(&g_resolver.cache_lock);

  // CRITICAL: Re-check the cache. Another thread might have created
  // the entry while we were switching locks.
  HASH_FIND(hh_str, g_resolver.str_to_int_map, entity_id_str, key_len, node);
  if (node) {
    *int_id_out = node->int_id;
    // It was just created, so it's already at the front of the LRU.
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return true;
  }

  // --- DB Lookup (Cache Miss) ---
  MDB_txn *sys_c_txn = db_create_txn(sys_c->env, true);
  if (!sys_c_txn) {
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return false;
  }

  db_get_result_t r = {0};
  db_key_t key = {.type = DB_KEY_STRING, .key.s = entity_id_str};

  if (!db_get(sys_c->data.sys->ent_id_to_int_db, sys_c_txn, &key, &r)) {
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    db_abort_txn(sys_c_txn);
    return false;
  }
  db_abort_txn(sys_c_txn);

  if (r.status == DB_GET_ERROR) {
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return false;
  }

  if (r.status == DB_GET_OK) {
    uint32_t val = *(uint32_t *)r.value;
    db_get_result_clear(&r);
    _create_cache_node(entity_id_str, val);
    *int_id_out = val;
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return true;
  }
  if (r.value)
    db_get_result_clear(&r);

  // --- Create New Entity (DB Miss) ---
  uint32_t new_id = id_manager_get_next_entity_id();
  if (new_id == 0) { // Error case
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return false;
  }

  // TODO: Log this creation to the WAL for durability.
  // wal_log_new_entity(entity_id_str, new_id);

  if (!_create_cache_node(entity_id_str, new_id)) {
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return false;
  }

  // Create a decoupled dirty item for the background writer
  er_dirty_item_t *dirty_item = malloc(sizeof(er_dirty_item_t));
  dirty_item->string_id = strdup(entity_id_str);
  dirty_item->int_id = new_id;

  uv_mutex_lock(&g_resolver.dirty_list_lock);
  dirty_item->next = g_resolver.dirty_head;
  g_resolver.dirty_head = dirty_item;
  uv_mutex_unlock(&g_resolver.dirty_list_lock);

  *int_id_out = new_id;
  uv_rwlock_wrunlock(&g_resolver.cache_lock);
  return true;
}

bool entity_resolver_resolve_string(eng_container_t *sys_c, uint32_t int_id,
                                    const char **str_id_out) {
  // --- Fast Path - Check cache with a READ lock ---
  uv_rwlock_rdlock(&g_resolver.cache_lock);
  er_cache_node_t *node;
  HASH_FIND(hh_int, g_resolver.int_to_str_map, &int_id, sizeof(uint32_t), node);
  if (node) {
    *str_id_out = node->string_id;
    uv_rwlock_rdunlock(&g_resolver.cache_lock);
    return true;
  }
  uv_rwlock_rdunlock(&g_resolver.cache_lock);

  // --- Slower Path - Acquire WRITE lock to potentially modify the cache
  uv_rwlock_wrlock(&g_resolver.cache_lock);

  // CRITICAL: Re-check the cache in case another thread populated it.
  HASH_FIND(hh_int, g_resolver.int_to_str_map, &int_id, sizeof(uint32_t), node);
  if (node) {
    *str_id_out = node->string_id;
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return true;
  }

  // --- DB Lookup (Cache Miss) ---
  MDB_txn *sys_c_txn = db_create_txn(sys_c->env, true);
  if (!sys_c_txn) {
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return false;
  }

  db_get_result_t r = {0};
  db_key_t key = {.type = DB_KEY_INTEGER, .key.i = int_id};

  if (!db_get(sys_c->data.sys->int_to_ent_id_db, sys_c_txn, &key, &r)) {
    db_abort_txn(sys_c_txn);
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return false;
  }
  db_abort_txn(sys_c_txn);

  if (r.status == DB_GET_ERROR) {
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return false;
  }

  if (r.status == DB_GET_OK) {
    char *string_from_db = (char *)r.value;
    er_cache_node_t *new_node = _create_cache_node(string_from_db, int_id);
    *str_id_out = new_node ? new_node->string_id : NULL;
    db_get_result_clear(&r);
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return new_node != NULL;
  }
  if (r.value)
    db_get_result_clear(&r);

  // --- Error Case ---
  // If an integer ID is not in the cache and not in the DB, it's a data
  // inconsistency error. An integer ID should never exist without its string
  // pair.
  *str_id_out = NULL;
  uv_rwlock_wrunlock(&g_resolver.cache_lock);
  return false;
}

er_dirty_item_t *entity_resolver_get_dirty_mappings() {
  // The "lock-and-swap" pattern
  uv_mutex_lock(&g_resolver.dirty_list_lock);
  er_dirty_item_t *list_to_process = g_resolver.dirty_head;
  g_resolver.dirty_head = NULL;
  uv_mutex_unlock(&g_resolver.dirty_list_lock);
  return list_to_process;
}

void entity_resolver_free_dirty_list(er_dirty_item_t *list) {
  er_dirty_item_t *current = list;
  while (current) {
    er_dirty_item_t *next = current->next;
    free(current->string_id);
    free(current);
    current = next;
  }
}
