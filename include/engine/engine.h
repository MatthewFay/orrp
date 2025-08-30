#ifndef ENG_H
#define ENG_H

#include "lmdb.h"
#include "query/ast.h"

#define MAX_CUSTOM_TAGS 10
// TODO: db_constants.h
extern const char *SYS_NEXT_ENT_ID_KEY;
extern const u_int32_t SYS_NEXT_ENT_ID_INIT_VAL;
extern const char *SYS_DB_METADATA_NAME;
extern const char *USR_NEXT_EVENT_ID_KEY;
extern const u_int32_t USR_NEXT_EVENT_ID_INIT_VAL;
extern const char *USR_DB_METADATA_NAME;

struct api_response_s;

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

typedef struct eng_context_s {
  eng_container_t *sys_c;
} eng_context_t;

eng_context_t *eng_init(void);
void eng_close_ctx(eng_context_t *ctx);
void eng_event(struct api_response_s *r, eng_context_t *ctx, ast_node_t *ast);
#endif