#ifndef INDEX_H
#define INDEX_H

#include "core/db.h"
#include "khash.h"
#include "lmdb.h"
#include <stdbool.h>
#include <stdint.h>

typedef enum { INDEX_TYPE_I64 } index_type_t;

// Persisted index definition
typedef struct index_def_s {
  char *key;
  index_type_t type;
} index_def_t;

// Runtime index definition
typedef struct index_s {
  index_def_t index_def;
  MDB_dbi index_db;
} index_t;

KHASH_MAP_INIT_STR(key_index, index_t)

// Returns true/false for index existence.
// If true, `index_out` will be set to valid index.
bool index_get(const char *key, khash_t(key_index) * key_to_index,
               index_t *index_out);

// Returns false on error. Get count of indexes in `key_to_index`.
bool index_get_count(khash_t(key_index) * key_to_index, uint32_t *count_out);

typedef enum {
  INDEX_WRITE_FROM_DB,
  INDEX_WRITE_DEFAULTS
} index_write_reg_src_t;

typedef struct {
  index_write_reg_src_t src;
  MDB_env *src_env;
  MDB_dbi src_dbi;
} index_write_reg_opts_t;

// Write a new index registry - either from another db or using defaults
bool index_write_registry(MDB_env *env, MDB_dbi dbi,
                          index_write_reg_opts_t *opts);

// Open index dbs for use
bool index_open_registry(MDB_env *env, MDB_dbi dbi,
                         khash_t(key_index) * *key_to_index);

/**
 * Adds an index to the registry db
 */
db_put_result_t index_add(const index_def_t *index_def, MDB_env *env,
                          MDB_dbi dbi);

// Destroy the key index map and close registry
void index_close_registry(MDB_env *env, MDB_dbi registry_db,
                          khash_t(key_index) * *key_to_index);
#endif