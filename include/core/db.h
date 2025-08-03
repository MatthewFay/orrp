#ifndef CORE_DB_H
#define CORE_DB_H

#include "lmdb.h"
#include <stdbool.h>

typedef enum { DB_KEY_STRING, DB_KEY_INTEGER } db_key_type_t;

typedef struct {
  db_key_type_t type;
  union {
    const char *s; // For string keys
    uint32_t i;    // For integer keys
  } key;
} db_key_t;

typedef enum { DB_GET_OK, DB_GET_NOT_FOUND, DB_GET_ERROR } db_get_status_t;
typedef struct db_get_result_s {
  db_get_status_t status;
  void *value;      // pointer to the value if found, NULL otherwise
  size_t value_len; // length of the value (if found)
} db_get_result_t;

// --- Transactions --- //
MDB_txn *db_create_txn(MDB_env *env, bool is_read_only);

void db_abort_txn(MDB_txn *txn);

bool db_commit_txn(MDB_txn *txn);

// --- DB Operations --- //

MDB_env *db_create_env(const char *path, size_t map_size, int max_num_dbs);

// Function to open a database
bool db_open(MDB_env *env, const char *db_name, MDB_dbi *db_out);

// Function to put a key-value pair into the database
bool db_put(MDB_dbi db, MDB_txn *txn, db_key_t *key, const void *value,
            bool auto_commit);

// Function to get a value by key from the database. Remember to free memory
// returned by db_get using `db_free_get_result`.
db_get_result_t *db_get(MDB_dbi db, MDB_txn *txn, db_key_t *key);

void db_free_get_result(db_get_result_t *r);

// Function to close and free the database environment
void db_close(MDB_env *env, MDB_dbi db);

void db_env_close(MDB_env *env);

#endif // CORE_DB_H