#ifndef ENG_H
#define ENG_H

#include "lmdb.h"

struct api_response_s;

typedef struct eng_db_s {
  MDB_env *env;

  // Forward mapping from string to integer ID.
  MDB_dbi id_to_int_db;
  // Reverse mapping for resolving results.
  MDB_dbi int_to_id_db;
  // Atomic counter for generating new integer IDs.
  MDB_dbi metadata_db;
  // Stores the specific event count for an id.
  MDB_dbi event_counters_db;
  // Stores the set of ids for a given count.
  MDB_dbi bitmaps_db;

} eng_db_t;

eng_db_t *eng_init_dbs(void);
void eng_add(struct api_response_s *r, eng_db_t *db, char *ns, char *key,
             char *id);

#endif