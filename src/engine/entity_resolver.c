#include "entity_resolver.h"
#include "core/db.h"
#include "engine/engine.h"
#include "id_manager.h" // To get new integer IDs
#include "lmdb.h"
#include "uthash.h"
#include "uv.h"
#include "wal.h" // To log the creation of new entities
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

  // --- Dirty List Pointers ---
  struct er_cache_node_s *dirty_next;
} er_cache_node_t;

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
  er_cache_node_t *dirty_head;

  // --- Concurrency ---
  uv_rwlock_t cache_lock;     // Protects the hash maps and LRU list
  uv_mutex_t dirty_list_lock; // Protects the dirty list
} er_manager_t;

typedef struct er_dirty_list_s {
  er_cache_node_t *head;
} er_dirty_list_t;

// Global instance of the resolver's state
static er_manager_t g_resolver;

// --- Public API Implementation ---

void entity_resolver_init(eng_context_t *ctx, int capacity) {
  g_resolver.capacity = capacity;
  g_resolver.size = 0;
  uv_rwlock_init(&g_resolver.cache_lock);
  uv_mutex_init(&g_resolver.dirty_list_lock);
  g_resolver.dirty_head = NULL;
  g_resolver.lru_head = NULL;
  g_resolver.lru_tail = NULL;
  g_resolver.str_to_int_map = NULL;
  g_resolver.int_to_str_map = NULL;
  // TODO: "Warm" the cache by loading some or all
  //    existing mappings from the system container
  (void)ctx;
}

void entity_resolver_destroy() {
  er_cache_node_t *n, *tmp;
  uv_rwlock_wrlock(&g_resolver.cache_lock);

  HASH_ITER(hh_str, g_resolver.str_to_int_map, n, tmp) {
    HASH_DELETE(hh_str, g_resolver.str_to_int_map, n);
    HASH_DELETE(hh_int, g_resolver.int_to_str_map, n);
    free(n->string_id);
    free(n);
  }

  uv_mutex_destroy(&g_resolver.dirty_list_lock);
  uv_rwlock_destroy(&g_resolver.cache_lock);
}

bool entity_resolver_resolve_id(eng_container_t *sys_c,
                                const char *entity_id_str,
                                uint32_t *int_id_out) {
  uv_rwlock_rdlock(&g_resolver.cache_lock);
  er_cache_node_t *node;
  HASH_FIND(hh_str, g_resolver.str_to_int_map, entity_id_str,
            strlen(entity_id_str), node);
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
  HASH_FIND(hh_str, g_resolver.str_to_int_map, entity_id_str,
            strlen(entity_id_str), node);
  if (node) {
    *int_id_out = node->int_id;
    // It was just created, so it's already at the front of the LRU.
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return true;
  }

  // --- Check LMDB (Cache Miss) ---
  MDB_txn *sys_c_txn = db_create_txn(sys_c->env, true);
  if (!sys_c_txn) {
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return false;
  }

  db_get_result_t r;
  db_key_t key;
  key.type = DB_KEY_STRING;
  key.key.s = entity_id_str;

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
    uint32_t *val = r.value;
    *int_id_out = *val;
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return true;
  }
  free(r.value);

  // --- Create New Entity (DB Miss) ---
  // It's a brand new entity.
  uint32_t new_id = id_manager_get_next_entity_id();
  if (new_id == 0) { // Error case
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return false;
  }

  // TODO: Log this creation to the WAL for durability.
  // wal_log_new_entity(entity_id_str, new_id);

  // Create the new cache node.
  er_cache_node_t *new_node = calloc(1, sizeof(er_cache_node_t));
  new_node->string_id = strdup(entity_id_str);
  new_node->int_id = new_id;

  // Add it to the main cache (hash maps and LRU list).
  // ... HASH_ADD logic for both maps ...
  HASH_ADD_KEYPTR(hh_str, g_resolver.str_to_int_map, new_node->string_id,
                  strlen(new_node->string_id), new_node);
  HASH_ADD(hh_int, g_resolver.int_to_str_map, int_id, sizeof(new_node->int_id),
           new_node);
  // TODO: ... logic to add to front of LRU list ...

  // Add it to the dirty list for the background writer.
  uv_mutex_lock(&g_resolver.dirty_list_lock);
  new_node->dirty_next = g_resolver.dirty_head;
  g_resolver.dirty_head = new_node;
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

  // --- Check LMDB (Cache Miss) ---
  MDB_txn *sys_c_txn = db_create_txn(sys_c->env, true);
  if (!sys_c_txn) {
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return false;
  }

  db_get_result_t r;
  db_key_t key;
  key.type = DB_KEY_INTEGER;
  key.key.i = int_id;

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
    // We found the string in the DB. Now we need to "warm" the cache.
    // It's crucial to find the original string->int node to avoid duplicating
    // the string in memory and to link our new int->str mapping correctly.
    char *string_from_db = r.value;
    er_cache_node_t *original_node = NULL;
    HASH_FIND(hh_str, g_resolver.str_to_int_map, string_from_db,
              strlen(string_from_db), original_node);
    if (original_node) {
      // Link the existing node into the int->str map as well.
      HASH_ADD(hh_int, g_resolver.int_to_str_map, int_id, sizeof(int_id),
               original_node);
      *str_id_out = original_node->string_id;
    }
    free(string_from_db);
    uv_rwlock_wrunlock(&g_resolver.cache_lock);
    return (original_node != NULL); // Success if we found the original node
  }

  // --- Error Case ---
  // If an integer ID is not in the cache and not in the DB, it's a data
  // inconsistency error. An integer ID should never exist without its string
  // pair.
  *str_id_out = NULL;
  uv_rwlock_wrunlock(&g_resolver.cache_lock);
  return false;
}

er_dirty_list_t *entity_resolver_get_dirty_mappings() {
  // The "lock-and-swap" pattern
  uv_mutex_lock(&g_resolver.dirty_list_lock);
  er_cache_node_t *list_to_process = g_resolver.dirty_head;
  g_resolver.dirty_head = NULL; // The global list is now empty
  uv_mutex_unlock(&g_resolver.dirty_list_lock);

  if (!list_to_process)
    return NULL;

  er_dirty_list_t *result = malloc(sizeof(er_dirty_list_t));
  result->head = list_to_process;
  return result;
}

void entity_resolver_free_dirty_list(er_dirty_list_t *list) {
  // The background writer has already processed these nodes.
  if (list) {
    // The nodes in list->head are still part of the main cache.
    // We only free the container that was allocated in
    // entity_resolver_get_dirty_mappings().
    free(list);
  }
}
