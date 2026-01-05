#include "core/db.h"
#include "lmdb.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

MDB_env *db_create_env(const char *path, size_t map_size, int max_num_dbs) {
  if (!path || !map_size || !max_num_dbs)
    return NULL;
  int rc;
  MDB_env *env;

  // Create environment
  rc = mdb_env_create(&env);
  if (rc != 0) {
    fprintf(stderr, "mdb_env_create failed: %s\n", mdb_strerror(rc));
    return NULL;
  }

  // Set map size (maximum size of the database before resize required)
  rc = mdb_env_set_mapsize(env, map_size);
  if (rc != 0) {
    fprintf(stderr, "mdb_env_set_mapsize failed: %s\n", mdb_strerror(rc));
    mdb_env_close(env);
    return NULL;
  }

  rc = mdb_env_set_maxdbs(env, max_num_dbs);
  if (rc != 0) {
    fprintf(stderr, "mdb_env_set_maxdbs failed: %s\n", mdb_strerror(rc));
    mdb_env_close(env);
    return NULL;
  }

  // Open environment
  // MDB_NOSUBDIR: The environment path is a file, not a directory.
  // 0664: Permissions for the directory/file
  rc = mdb_env_open(env, path, MDB_NOSUBDIR, 0664);
  if (rc != 0) {
    fprintf(stderr, "mdb_env_open failed: %s\n", mdb_strerror(rc));
    mdb_env_close(env);
    return NULL;
  }
  return env;
}

bool db_open(MDB_env *env, const char *db_name, bool int_only_keys,
             db_dup_key_config_t dup_key_config, MDB_dbi *db_out) {
  if (!env || !db_name || !db_out)
    return false;
  int rc;
  MDB_txn *txn;
  MDB_dbi db;
  rc = mdb_txn_begin(env, NULL, 0, &txn);
  if (rc != 0) {
    fprintf(stderr, "mdb_txn_begin failed: %s\n", mdb_strerror(rc));
    return false;
  }

  // DB will be created if it doesn't exist
  unsigned int flags = MDB_CREATE;

  if (int_only_keys) {
    // we use MDB_INTEGERKEY for performance
    flags |= MDB_INTEGERKEY;
  }

  if (dup_key_config == DB_DUP_KEYS) {
    flags |= MDB_DUPSORT;
  } else if (dup_key_config == DB_DUP_KEYS_FIXED_SIZE_VALS) {
    flags = flags | MDB_DUPSORT | MDB_DUPFIXED;
  }

  rc = mdb_dbi_open(txn, db_name, flags, &db);
  if (rc != 0) {
    fprintf(stderr, "mdb_dbi_open failed: %s\n", mdb_strerror(rc));
    mdb_txn_abort(txn);
    return false;
  }

  rc = mdb_txn_commit(txn);
  if (rc != 0) {
    fprintf(stderr, "mdb_txn_commit failed: %s\n", mdb_strerror(rc));
    return false;
  }

  *db_out = db;
  return true;
}

db_put_result_t db_put(MDB_dbi db, MDB_txn *txn, db_key_t *key,
                       const void *value, size_t value_size, bool auto_commit,
                       bool no_overwrite) {
  if (txn == NULL || key == NULL || value == NULL || value_size == 0)
    return DB_PUT_ERR;

  MDB_val mdb_key, mdb_value;
  int rc;

  switch (key->type) {
  case DB_KEY_STRING:
    // For strings, the data is the string itself and size is its length.
    mdb_key.mv_data = (void *)key->key.s;
    mdb_key.mv_size = strlen(key->key.s);
    break;

  case DB_KEY_U32:
    mdb_key.mv_data = &key->key.u32;
    mdb_key.mv_size = sizeof(uint32_t);
    break;

  case DB_KEY_I64:
    mdb_key.mv_data = &key->key.i64;
    mdb_key.mv_size = sizeof(int64_t);
    break;

  default:
    return DB_PUT_ERR;
  }

  mdb_value.mv_size = value_size;
  mdb_value.mv_data = (void *)value;

  uint flags = 0;
  if (no_overwrite) {
    flags |= MDB_NOOVERWRITE;
  }

  rc = mdb_put(txn, db, &mdb_key, &mdb_value, flags);
  if (rc == MDB_KEYEXIST) {
    return DB_PUT_KEY_EXISTS;
  }
  if (rc != 0) {
    fprintf(stderr, "db_put: mdb_put failed: %s\n", mdb_strerror(rc));
    return DB_PUT_ERR;
  }

  if (auto_commit) {
    return db_commit_txn(txn) ? DB_PUT_OK : DB_PUT_ERR;
  }

  return DB_PUT_OK;
}

void db_get_result_clear(db_get_result_t *res) {
  if (res && res->value) {
    free(res->value);
    res->value = NULL;
    res->value_len = 0;
  }
}

// Returns false on error, true if found or not found
bool db_get(MDB_dbi db, MDB_txn *txn, db_key_t *key,
            db_get_result_t *result_out) {
  if (txn == NULL || key == NULL || result_out == NULL)
    return false;

  memset(result_out, 0, sizeof(db_get_result_t));

  result_out->status = DB_GET_ERROR;

  MDB_val mdb_key, mdb_value;
  int rc;
  void *result = NULL;

  switch (key->type) {
  case DB_KEY_STRING:
    mdb_key.mv_data = (void *)key->key.s;
    mdb_key.mv_size = strlen(key->key.s);
    break;

  case DB_KEY_U32:
    mdb_key.mv_data = &key->key.u32;
    mdb_key.mv_size = sizeof(uint32_t);
    break;

  case DB_KEY_I64:
    mdb_key.mv_data = &key->key.i64;
    mdb_key.mv_size = sizeof(int64_t);
    break;

  default:
    return false;
  }

  rc = mdb_get(txn, db, &mdb_key, &mdb_value);
  if (rc == MDB_NOTFOUND) {
    // Key not found, which is not an error for get
    result_out->status = DB_GET_NOT_FOUND;
    return true;
  } else if (rc != 0) {
    fprintf(stderr, "db_get: mdb_get failed: %s\n", mdb_strerror(rc));
    return false;
  }

  // Duplicate the data as it's only valid within the transaction
  result = malloc(mdb_value.mv_size);
  if (!result) {
    return false;
  }
  memcpy(result, mdb_value.mv_data, mdb_value.mv_size);
  result_out->status = DB_GET_OK;
  result_out->value = result;
  result_out->value_len = mdb_value.mv_size;

  return true;
}

void db_close(MDB_env *env, MDB_dbi db) {
  if (env && db) {
    mdb_dbi_close(env, db);
  }
}

void db_env_close(MDB_env *env) {
  if (env) {
    mdb_env_close(env);
  }
}

MDB_txn *db_create_txn(MDB_env *env, bool is_read_only) {
  if (!env)
    return NULL;
  MDB_txn *txn;
  int rc = mdb_txn_begin(env, NULL, is_read_only ? MDB_RDONLY : 0, &txn);
  if (rc == MDB_SUCCESS)
    return txn;
  fprintf(stderr, "db_create_txn: mdb_txn_begin failed: %s\n",
          mdb_strerror(rc));
  return NULL;
}

// Abandon all the operations of the transaction instead of saving them
void db_abort_txn(MDB_txn *txn) {
  if (!txn)
    return;
  mdb_txn_abort(txn);
  txn = NULL;
}

bool db_commit_txn(MDB_txn *txn) {
  int rc = mdb_txn_commit(txn);
  if (rc != 0) {
    fprintf(stderr, "db_commit_txn: mdb_txn_commit failed: %s\n",
            mdb_strerror(rc));
    return false;
  }
  return true;
}

MDB_cursor *db_cursor_open(MDB_txn *txn, MDB_dbi db) {
  if (!txn)
    return NULL;

  MDB_cursor *cursor;
  int rc = mdb_cursor_open(txn, db, &cursor);
  if (rc != 0) {
    fprintf(stderr, "db_cursor_open: mdb_cursor_open failed: %s\n",
            mdb_strerror(rc));
    return NULL;
  }

  return cursor;
}

void db_cursor_close(MDB_cursor *cursor) {
  if (cursor) {
    mdb_cursor_close(cursor);
  }
}

bool db_cursor_next(MDB_cursor *cursor, db_cursor_entry_t *entry_out) {
  if (!cursor || !entry_out)
    return false;

  MDB_val key, value;
  int rc = mdb_cursor_get(cursor, &key, &value, MDB_NEXT);

  if (rc == MDB_NOTFOUND) {
    return false; // End of iteration
  }

  if (rc != 0) {
    fprintf(stderr, "db_cursor_next: mdb_cursor_get failed: %s\n",
            mdb_strerror(rc));
    return false;
  }

  // Note: These pointers are only valid until next cursor op or txn end
  entry_out->key = key.mv_data;
  entry_out->key_len = key.mv_size;
  entry_out->value = value.mv_data;
  entry_out->value_len = value.mv_size;

  return true;
}

bool db_foreach(MDB_txn *txn, MDB_dbi db, db_foreach_cb callback,
                void *user_data) {
  if (!txn || !callback)
    return false;

  MDB_cursor *cursor = db_cursor_open(txn, db);
  if (!cursor)
    return false;

  db_cursor_entry_t entry;
  bool success = true;

  // Position cursor at first entry
  MDB_val key, value;
  int rc = mdb_cursor_get(cursor, &key, &value, MDB_FIRST);

  if (rc == MDB_NOTFOUND) {
    // Empty database, not an error
    db_cursor_close(cursor);
    return true;
  }

  if (rc != 0) {
    fprintf(stderr, "db_foreach: mdb_cursor_get failed: %s\n",
            mdb_strerror(rc));
    db_cursor_close(cursor);
    return false;
  }

  // Process first entry
  entry.key = key.mv_data;
  entry.key_len = key.mv_size;
  entry.value = value.mv_data;
  entry.value_len = value.mv_size;

  if (!callback(&entry, user_data)) {
    // Callback requested early termination
    db_cursor_close(cursor);
    return true;
  }

  // Process remaining entries
  while (db_cursor_next(cursor, &entry)) {
    if (!callback(&entry, user_data)) {
      // Callback requested early termination
      break;
    }
  }

  db_cursor_close(cursor);
  return success;
}
