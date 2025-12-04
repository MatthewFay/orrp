#ifndef CONTAINER_TYPES_H
#define CONTAINER_TYPES_H

#include "core/db.h"
#include "core/mmap_array.h"
#include "lmdb.h"
#include "uthash.h"
#include <stdatomic.h>

// ============================================================================
// Constants - Container Names & Database Counts
// ============================================================================

#define SYS_CONTAINER_NAME "system"

// ============================================================================
// Constants - System Database Names
// ============================================================================

#define SYS_DB_ENT_ID_TO_INT_NAME "ent_id_to_int_db"
#define SYS_DB_METADATA_NAME "sys_dc_metadata_db"

// ============================================================================
// Constants - User Database Names
// ============================================================================

#define USR_DB_INVERTED_EVENT_INDEX_NAME "inverted_event_index_db"
#define USR_DB_METADATA_NAME "user_dc_metadata_db"
#define USR_DB_EVENTS_NAME "events_db"

// ============================================================================
// Constants - Metadata Keys & Initial Values
// ============================================================================

#define SYS_NEXT_ENT_ID_KEY "next_ent_id"
#define SYS_NEXT_ENT_ID_INIT_VAL 1

#define USR_NEXT_EVENT_ID_KEY "next_event_id"
#define USR_NEXT_EVENT_ID_INIT_VAL 1
#define USR_ENTITIES_KEY "entities"

#define MAX_CONTAINER_PATH_LENGTH 128

// ============================================================================
// Enums - Container & Database Types
// ============================================================================

typedef enum { CONTAINER_TYPE_SYS, CONTAINER_TYPE_USR } eng_dc_type_t;

typedef enum {
  SYS_DB_ENT_ID_TO_INT = 0,
  SYS_DB_METADATA,
  SYS_DB_COUNT
} eng_dc_sys_db_type_t;

typedef enum {
  USR_DB_INVERTED_EVENT_INDEX = 0,
  USR_DB_METADATA,
  USR_DB_EVENTS,
  USR_DB_COUNT,
} eng_dc_user_db_type_t;

// ============================================================================
// Structs - Container Data Structures
// ============================================================================

/**
 * System data container (Global Directory)
 * Stores entity ID mappings and metadata
 */
typedef struct {
  // B-Tree to find ID from String efficiently.
  // Key: "user-123", Value: uint32_t (e.g. 100)
  MDB_dbi ent_id_to_int_db;

  // MMap Array: Index 100 -> "user-123"
  mmap_array_t entity_id_map;

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
  // Used for filtering (WHERE)
  MDB_dbi inverted_event_index_db;

  // Data retrieval (SELECT)
  // Key = event id, Value = MsgPack Blob
  MDB_dbi events_db;

  // Metadata:
  // Contains 1) atomic counter for generating new event integer IDs,
  // and 2) bitmap of all entity ids present in this container (used for
  // negation logic).
  MDB_dbi user_dc_metadata_db;

  // Aggregation (GROUP BY)
  // MMap Array: Index EventID -> EntityID
  mmap_array_t event_to_entity_map;
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
    eng_dc_user_db_type_t usr_db_type;
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
  CONTAINER_ERR_MMAP,
} container_error_code_t;

typedef struct {
  bool success;
  eng_container_t *container; // NULL on failure
  const char *error_msg;      // NULL on success
  container_error_code_t error_code;
} container_result_t;

#endif // CONTAINER_TYPES_H