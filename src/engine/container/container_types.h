#ifndef CONTAINER_TYPES_H
#define CONTAINER_TYPES_H

#include "core/db.h"
#include "lmdb.h"
#include "uthash.h"
#include <stdatomic.h>

// ============================================================================
// Constants - Container Names & Database Counts
// ============================================================================

#define SYS_CONTAINER_NAME "system"
#define NUM_SYS_DBS 3
#define NUM_USR_DBS 5

// ============================================================================
// Constants - System Database Names
// ============================================================================

#define SYS_DB_ENT_ID_TO_INT_NAME "ent_id_to_int_db"
#define SYS_DB_INT_TO_ENT_ID_NAME "int_to_ent_id_db"
#define SYS_DB_METADATA_NAME "sys_dc_metadata_db"

// ============================================================================
// Constants - User Database Names
// ============================================================================

#define USR_DB_INVERTED_EVENT_INDEX_NAME "inverted_event_index_db"
#define USR_DB_EVENT_TO_ENT_NAME "event_to_entity_db"
#define USR_DB_COUNTER_STORE_NAME "counter_store_db"
#define USR_DB_COUNT_INDEX_NAME "count_index_db"
#define USR_DB_METADATA_NAME "user_dc_metadata_db"

// ============================================================================
// Constants - Metadata Keys & Initial Values
// ============================================================================

#define SYS_NEXT_ENT_ID_KEY "next_ent_id"
#define SYS_NEXT_ENT_ID_INIT_VAL 1

#define USR_NEXT_EVENT_ID_KEY "next_event_id"
#define USR_NEXT_EVENT_ID_INIT_VAL 1

#define MAX_CONTAINER_PATH_LENGTH 256

// ============================================================================
// Enums - Container & Database Types
// ============================================================================

typedef enum { CONTAINER_TYPE_SYSTEM, CONTAINER_TYPE_USER } eng_dc_type_t;

typedef enum {
  SYS_DB_ENT_ID_TO_INT = 0,
  SYS_DB_INT_TO_ENT_ID,
  SYS_DB_METADATA,
  SYS_DB_COUNT
} eng_dc_sys_db_type_t;

typedef enum {
  USER_DB_INVERTED_EVENT_INDEX = 0,
  USER_DB_EVENT_TO_ENTITY,
  USER_DB_METADATA,
  USER_DB_COUNTER_STORE,
  USER_DB_COUNT_INDEX,
  USER_DB_COUNT
} eng_dc_user_db_type_t;

// ============================================================================
// Structs - Container Data Structures
// ============================================================================

/**
 * System data container (Global Directory)
 * Stores entity ID mappings and metadata
 */
typedef struct {
  // Forward mapping from string entity id to integer ID
  MDB_dbi ent_id_to_int_db;

  // Reverse mapping for resolving results
  MDB_dbi int_to_ent_id_db;

  // Contains atomic counter for generating new entity integer IDs
  MDB_dbi sys_dc_metadata_db;
} eng_sys_dc_t;

/**
 * User data container (Event Data)
 * Stores events, indexes, and aggregations
 */
typedef struct {
  // The Event Index:
  // Key: The tag (e.g., `loc:ca`)
  // Value: A Roaring Bitmap of all local `event_id`s that have this tag
  MDB_dbi inverted_event_index_db;

  // Event-to-Entity Map
  // Key: The local `event_id` (`uint32_t`)
  // Value: The global `entity_id` (`uint32_t`) associated with the event
  MDB_dbi event_to_entity_db;

  // Contains atomic counter for generating new event integer IDs
  MDB_dbi user_dc_metadata_db;

  // Stores the raw counts for countable tags
  // Key: A composite of `(tag, entity_id)`
  // Value: The `uint32_t` count
  MDB_dbi counter_store_db;

  // The Count Index: An inverted index for fast count-based threshold queries
  // This index uses a CUMULATIVE model for efficiency
  //
  // Key: A composite of `(tag, count)`
  // Value: A Roaring Bitmap of `entity_id`s that have a count >= this count
  MDB_dbi count_index_db;
} eng_user_dc_t;

typedef struct container_cache_node_s container_cache_node_t;

/**
 * Container structure - abstraction over an LMDB database file/env
 */
typedef struct eng_container_s {
  char *name;
  MDB_env *env;
  eng_dc_type_t type;

  union {
    eng_sys_dc_t *sys;
    eng_user_dc_t *usr;
  } data;

  container_cache_node_t *_node; // internal use only
} eng_container_t;

/**
 * Database key structure for container operations
 */
typedef struct {
  eng_dc_type_t dc_type;
  union {
    eng_dc_sys_db_type_t sys_db_type;
    eng_dc_user_db_type_t user_db_type;
  };
  char *container_name; // NULL for system DBs
  db_key_t db_key;
} eng_container_db_key_t;

typedef struct container_cache_node_s {
  eng_container_t *container;
  _Atomic(uint32_t) reference_count;
  struct container_cache_node_s *prev;
  struct container_cache_node_s *next;
  UT_hash_handle hh;
} container_cache_node_t;

typedef struct container_cache_s {
  size_t size;
  size_t capacity;
  container_cache_node_t *nodes; // Hash table
  container_cache_node_t *head;  // LRU list head (most recently used)
  container_cache_node_t *tail;  // LRU list tail (least recently used)
} container_cache_t;

typedef enum {
  CONTAINER_OK = 0,
  CONTAINER_ERR_NOT_INITIALIZED,
  CONTAINER_ERR_ALREADY_INITIALIZED,
  CONTAINER_ERR_INVALID_NAME,
  CONTAINER_ERR_INVALID_TYPE,
  CONTAINER_ERR_ALLOC,
  CONTAINER_ERR_PATH_TOO_LONG,
  CONTAINER_ERR_ENV_CREATE,
  CONTAINER_ERR_DB_OPEN,
  CONTAINER_ERR_CACHE_FULL,
} container_error_code_t;

typedef struct {
  bool success;
  eng_container_t *container; // NULL on failure
  const char *error_msg;      // NULL on success
  container_error_code_t error_code;
} container_result_t;

#endif // CONTAINER_TYPES_H