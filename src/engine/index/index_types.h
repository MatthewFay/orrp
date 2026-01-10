#ifndef INDEX_TYPES
#define INDEX_TYPES

#include "lmdb.h"
typedef enum { INDEX_TYPE_I64 } index_type_t;

// Persisted index definition
typedef struct {
  char *key;
  index_type_t type;
} index_def_t;

// Runtime index definition
typedef struct {
  index_def_t index_def;
  MDB_dbi index_db;
} index_t;

#endif