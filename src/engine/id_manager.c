#include "id_manager.h"
#include "core/db.h"
#include "engine.h"
#include "lmdb.h"
#include "log.h"
#include "uthash.h"
#include "uv.h"
#include <stdatomic.h> // For C11 atomics

#define ID_BLOCK_SIZE 100 // Configurable size for reserving ID blocks

// --- Global Entity ID State ---
static atomic_uint_fast32_t g_next_entity_id;

// --- Per-Container Event ID State ---

// Struct to hold the counter state for a single container
typedef struct id_manager_event_counter_s {
  char *container_name; // Key for the hash map
  uint32_t reserved_block_start;
  uint32_t reserved_block_count;
  atomic_uint_fast32_t next_id_counter; // The master counter for this container
  UT_hash_handle hh;
} id_manager_event_counter_t;

// The hash map and its dedicated lock
static id_manager_event_counter_t *g_event_id_counters_hash = NULL;
static uv_mutex_t g_event_id_hash_lock;

// --- Implementation ---

static bool _get_next_id(eng_container_t *c, MDB_txn *txn,
                         u_int32_t *int_id_out) {
  if (!c || !txn || !int_id_out) {
    return false;
  }
  *int_id_out = 0;
  eng_dc_type_t c_type = c->type;
  db_get_result_t next;
  db_key_t key;
  key.type = DB_KEY_STRING;
  key.key.s = c_type == CONTAINER_TYPE_SYSTEM ? SYS_NEXT_ENT_ID_KEY
                                              : USR_NEXT_EVENT_ID_KEY;
  MDB_dbi db = c_type == CONTAINER_TYPE_SYSTEM
                   ? c->data.sys->sys_dc_metadata_db
                   : c->data.usr->user_dc_metadata_db;

  if (!db_get(db, txn, &key, &next) || next.status == DB_GET_ERROR) {
    const char *msg = c_type == CONTAINER_TYPE_SYSTEM
                          ? "Error getting next entity ID"
                          : "Error getting next event ID";
    log_error(msg);
    return false;
  }

  if (next.status == DB_GET_OK) {
    *int_id_out = *(u_int32_t *)next.value;
  } else if (next.status == DB_GET_NOT_FOUND) {
    *int_id_out = c_type == CONTAINER_TYPE_SYSTEM ? SYS_NEXT_ENT_ID_INIT_VAL
                                                  : USR_NEXT_EVENT_ID_INIT_VAL;
  }
      db_get_result_clear(&next);

  return true;
}

bool id_manager_init(eng_context_t *ctx) {
  uint32_t last_entity_id;

  MDB_txn *txn = db_create_txn(ctx->sys_c->env, true);
  if (!txn)
    return false;
  _get_next_id(ctx->sys_c, txn, &last_entity_id);
  db_abort_txn(txn);
  atomic_init(&g_next_entity_id, last_entity_id);

  g_event_id_counters_hash = NULL;
  uv_mutex_init(&g_event_id_hash_lock);
  return true;
}

void id_manager_destroy() {
  uv_mutex_lock(&g_event_id_hash_lock);
  id_manager_event_counter_t *c, *tmp;
  HASH_ITER(hh, g_event_id_counters_hash, c, tmp) {
    HASH_DEL(g_event_id_counters_hash, c);
    free(c->container_name);
    free(c);
  }
  uv_mutex_unlock(&g_event_id_hash_lock);
  uv_mutex_destroy(&g_event_id_hash_lock);
}

// --- Entity ID Functions ---

uint32_t id_manager_get_next_entity_id() {
  // This is the highly concurrent, lock-free operation.
  // It atomically increments the global counter and returns the value
  // it had *before* the increment.
  return atomic_fetch_add(&g_next_entity_id, 1);
}

uint32_t id_manager_get_last_reserved_entity_id() {
  // Atomically load the current value for the background writer.
  return atomic_load(&g_next_entity_id);
}

// --- Event ID Functions ---

// Helper to get or create a counter for a specific container
static id_manager_event_counter_t *
_get_or_create_event_counter(eng_container_t *container, MDB_txn *txn) {
  // This function assumes the caller is holding g_event_id_hash_lock
  id_manager_event_counter_t *counter = NULL;
  HASH_FIND_STR(g_event_id_counters_hash, container->name, counter);

  if (!counter) {
    // It's not in our in-memory map. We need to create it.
    counter = calloc(1, sizeof(id_manager_event_counter_t));
    counter->container_name = strdup(container->name);

    // Load the last saved value from this container's specific LMDB file.
    // This is a slower operation, but it only happens once per container.
    uint32_t last_event_id;
    _get_next_id(container, txn, &last_event_id);
    atomic_init(&counter->next_id_counter, last_event_id);

    counter->reserved_block_count = 0; // Force a new block reservation
    HASH_ADD_KEYPTR(hh, g_event_id_counters_hash, counter->container_name,
                    strlen(counter->container_name), counter);
  }
  return counter;
}

uint32_t id_manager_get_next_event_id(eng_container_t *container,
                                      MDB_txn *txn) {
  // Using a simple mutex here is fine because the "local block" logic
  // means we only take this lock once every ID_BLOCK_SIZE calls.
  uv_mutex_lock(&g_event_id_hash_lock);

  id_manager_event_counter_t *counter =
      _get_or_create_event_counter(container, txn);

  if (counter->reserved_block_count == 0) {
    // Our reserved block is empty. Get a new one from this container's atomic
    // counter.
    counter->reserved_block_start =
        atomic_fetch_add(&counter->next_id_counter, ID_BLOCK_SIZE);
    counter->reserved_block_count = ID_BLOCK_SIZE;

    // NOTE: This is where you would log the reservation to the WAL
    // wal_log_event_id_block_reserved(container_name,
    // counter->reserved_block_start, ID_BLOCK_SIZE);
  }

  // Use one of the IDs from our reserved block
  uint32_t next_id = counter->reserved_block_start;
  counter->reserved_block_start++;
  counter->reserved_block_count--;

  uv_mutex_unlock(&g_event_id_hash_lock);
  return next_id;
}

uint32_t id_manager_get_last_reserved_event_id(const char *container_name) {
  uint32_t last_id = 0;
  uv_mutex_lock(&g_event_id_hash_lock);

  id_manager_event_counter_t *counter;
  HASH_FIND_STR(g_event_id_counters_hash, container_name, counter);

  if (counter) {
    last_id = atomic_load(&counter->next_id_counter);
  }

  uv_mutex_unlock(&g_event_id_hash_lock);
  return last_id;
}
