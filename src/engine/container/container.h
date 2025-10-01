#ifndef CONTAINER_H
#define CONTAINER_H

#include "core/db.h"
#include "lmdb.h"
#include <stdbool.h>

#define NUM_SYS_DBS 3
#define SYS_DB_ENT_ID_TO_INT_NAME "ent_id_to_int_db"
#define SYS_DB_INT_TO_ENT_ID_NAME "int_to_ent_id_db"
#define SYS_DB_METADATA_NAME "sys_dc_metadata_db"

#define SYS_NEXT_ENT_ID_KEY "next_ent_id"
#define SYS_NEXT_ENT_ID_INIT_VAL 1

#define NUM_USR_DBS 5
#define USR_DB_INVERTED_EVENT_INDEX_NAME "inverted_event_index_db"
#define USR_DB_EVENT_TO_ENT_NAME "event_to_entity_db"
#define USR_DB_COUNTER_STORE_NAME "counter_store_db"
#define USR_DB_COUNT_INDEX_NAME "count_index_db"
#define USR_DB_METADATA_NAME "user_dc_metadata_db"

#define USR_NEXT_EVENT_ID_KEY "next_event_id"
#define USR_NEXT_EVENT_ID_INIT_VAL 1

typedef enum { CONTAINER_TYPE_SYSTEM, CONTAINER_TYPE_USER } eng_dc_type_t;

// System data container
// (Global Directory)
typedef struct eng_sys_dc_s {
  // Forward mapping from string entity id to integer ID.
  MDB_dbi ent_id_to_int_db;

  // Reverse mapping for resolving results.
  MDB_dbi int_to_ent_id_db;

  // Contains atomic counter for generating new entity integer IDs.
  MDB_dbi sys_dc_metadata_db;
} eng_sys_dc_t;

// User data container
// (Event Data)
typedef struct eng_user_dc_s {
  // The Event Index:
  // This is for finding events with a specific combination of tags.
  // Key: The tag (e.g., `loc:ca`).
  // Value: A Roaring Bitmap of all local `event_id`s that have this tag.
  MDB_dbi inverted_event_index_db;

  // Event-to-Entity Map.
  // Key: The local `event_id` (`uint32_t`).
  // Value: The global `entity_id` (`uint32_t`) associated with the event.
  MDB_dbi event_to_entity_db;

  // Contains atomic counter for generating new event integer IDs.
  MDB_dbi user_dc_metadata_db;

  // Stores the raw counts for countable tags.
  // Key: A composite of `(tag, entity_id)`.
  // Value: The `uint32_t` count.
  MDB_dbi counter_store_db;

  // The Count Index: An inverted index for fast count-based threshold queries.
  // This index uses a CUMULATIVE model for efficiency.
  //
  // Key: A composite of `(tag, count)`.
  // Value: A Roaring Bitmap of `entity_id`s that have a count GREATER THAN OR
  // EQUAL TO this key's count for this tag.
  //
  // Example: The bitmap for ("purchase:prod123", 3) contains all entities
  // that have purchased the product 3 or more times.
  MDB_dbi count_index_db;
} eng_user_dc_t;

typedef struct eng_container_s {
  char *name;
  MDB_env *env;
  eng_dc_type_t type;

  union {
    eng_sys_dc_t *sys;
    eng_user_dc_t *usr;
  } data;

} eng_container_t;

typedef enum {
  SYS_DB_ENT_ID_TO_INT = 0,
  SYS_DB_INT_TO_ENT_ID,
  SYS_DB_METADATA,
} eng_dc_sys_db_type_t;

typedef enum {
  USER_DB_INVERTED_EVENT_INDEX = 0,
  USER_DB_EVENT_TO_ENTITY,
  USER_DB_METADATA,
  USER_DB_COUNTER_STORE,
  USER_DB_COUNT_INDEX,
  USER_DB_COUNT
} eng_dc_user_db_type_t;

typedef struct eng_container_db_key_s {
  eng_dc_type_t dc_type;
  union {
    eng_dc_sys_db_type_t sys_db_type;
    eng_dc_user_db_type_t user_db_type;
  };
  char *container_name; // NULL for sys DBs
  db_key_t db_key;
} eng_container_db_key_t;

eng_container_t *eng_container_create(eng_dc_type_t type);

void eng_container_close(eng_container_t *c);

// Map DB type to actual DB handle
bool eng_container_get_db_handle(eng_container_t *c,
                                 eng_dc_user_db_type_t db_type,
                                 MDB_dbi *db_out);

#endif // CONTAINER_H
