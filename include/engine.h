#ifndef ENG_H
#define ENG_H

#include "lmdb.h"

struct api_response_s;

typedef enum { CONTAINER_TYPE_SYSTEM, CONTAINER_TYPE_USER } eng_dc_type_t;

// System data container
typedef struct eng_sys_dc_s {
  // Forward mapping from string to integer ID.
  MDB_dbi id_to_int_db;
  // Reverse mapping for resolving results.
  MDB_dbi int_to_id_db;
  // Atomic counter for generating new integer IDs.
  MDB_dbi metadata_db;
} eng_sys_dc_t;

// User data container
typedef struct eng_user_dc_s {
  // Stores the specific event count for an id.
  MDB_dbi event_counters_db;
  // Stores the set of ids for a given count.
  MDB_dbi bitmaps_db;
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
void eng_close_sys_container(eng_container_t *sys_c);
void eng_add(struct api_response_s *r, eng_context_t *ctx, char *ns, char *key,
             char *id);
#endif