#ifndef CORE_DB_H
#define CORE_DB_H

#include "lmdb.h"
#include <stdbool.h>

typedef enum { DB_KEY_STRING, DB_KEY_U32, DB_KEY_I64 } db_key_type_t;

typedef struct {
  db_key_type_t type;
  union {
    char *s;
    uint32_t u32;
    int64_t i64;
  } key;
} db_key_t;

typedef enum { DB_GET_OK, DB_GET_NOT_FOUND, DB_GET_ERROR } db_get_status_t;

typedef struct db_get_result_s {
  db_get_status_t status;
  void *value;      // pointer to the value if found, NULL otherwise
  size_t value_len; // length of the value (if found)
} db_get_result_t;

// Cursor iteration result
typedef struct db_cursor_entry_s {
  void *key;
  size_t key_len;
  void *value;
  size_t value_len;
} db_cursor_entry_t;

// --- Transactions --- //
MDB_txn *db_create_txn(MDB_env *env, bool is_read_only);

void db_abort_txn(MDB_txn *txn);

bool db_commit_txn(MDB_txn *txn);

// --- DB Operations --- //

// For `path`: Caller needs to ensure that directory is created before calling.
MDB_env *db_create_env(const char *path, size_t map_size, int max_num_dbs);

// Database duplicate key config
typedef enum {
  // No duplicate keys. keys must be unique and may have only a single data
  // item.
  DB_DUP_NONE,
  // Duplicate keys allowed. keys may have multiple data items, stored in sorted
  // order
  DB_DUP_KEYS,
  // Duplicate keys allowed, and data items are all the same size, which allows
  // further optimizations in storage and retrieval
  DB_DUP_KEYS_FIXED_SIZE_VALS
} db_dup_key_config_t;

// Function to open a database
bool db_open(MDB_env *env, const char *db_name, bool int_only_keys,
             db_dup_key_config_t dup_key_config, MDB_dbi *db_out);

typedef enum {
  // Key-value entry added into database
  DB_PUT_OK,
  DB_PUT_ERR,
  // If `no_overwrite` is true and key already exists
  DB_PUT_KEY_EXISTS
} db_put_result_t;

// Function to put a key-value pair into the database
// replacing any previously existing key if duplicates are disallowed, or adding
// a duplicate data item if duplicates are allowed
db_put_result_t db_put(MDB_dbi db, MDB_txn *txn, db_key_t *key,
                       const void *value, size_t value_size, bool auto_commit,
                       bool no_overwrite);

// Function to get a value by key from the database. Caller: Remember to free
// memory returned by db_get.
bool db_get(MDB_dbi db, MDB_txn *txn, db_key_t *key,
            db_get_result_t *result_out);

void db_get_result_clear(db_get_result_t *res);

// Function to close and free the database environment
void db_close(MDB_env *env, MDB_dbi db);

void db_env_close(MDB_env *env);

// --- Cursor Operations --- //

// Create a cursor for iterating over database entries
// Returns NULL on failure
MDB_cursor *db_cursor_open(MDB_txn *txn, MDB_dbi db);

// Close and free the cursor
void db_cursor_close(MDB_cursor *cursor);

typedef enum {
  DB_CURSOR_OK,
  DB_CURSOR_NOTFOUND,
  DB_CURSOR_ERR
} db_cursor_get_result_t;

// Retrieve by cursor.
// `entry_out` key and value pointers are valid only until next cursor
// operation or txn end.
// `db_key`: Optional. Can be used to set starting key of cursor.
db_cursor_get_result_t db_cursor_get(MDB_cursor *cursor,
                                     db_cursor_entry_t *entry_out,
                                     MDB_cursor_op op, db_key_t *db_key);

// Iterate over all entries in database and call callback for each
// callback should return true to continue iteration, false to stop
// Returns true if iteration completed successfully, false on error
typedef bool (*db_foreach_cb)(const db_cursor_entry_t *entry, void *user_data);
bool db_foreach(MDB_txn *txn, MDB_dbi db, db_foreach_cb callback,
                void *user_data);

#endif // CORE_DB_H