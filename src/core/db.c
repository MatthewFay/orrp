#include "core/db.h"
#include "lmdb.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * On POSIX systems, this header provides htonl.
 * Windows requires Winsock2.h for this function.
 */
#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#endif

MDB_env *db_create_env(const char *path, size_t map_size, int max_num_dbs) {
  int rc;
  MDB_env *env;

  // Create environment
  rc = mdb_env_create(&env);
  if (rc != 0) {
    fprintf(stderr, "mdb_env_create failed: %s\n", mdb_strerror(rc));
    return NULL;
  }

  // Set map size (maximum size of the database)
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

bool db_open(MDB_env *env, const char *db_name, MDB_dbi *db_out) {
  int rc;
  MDB_txn *txn;
  MDB_dbi db;
  rc = mdb_txn_begin(env, NULL, 0, &txn);
  if (rc != 0) {
    fprintf(stderr, "mdb_txn_begin failed: %s\n", mdb_strerror(rc));
    return false;
  }

  // Open a database (dbi) - will be created if it doesn't exist
  // 0: Default flags
  rc = mdb_dbi_open(txn, db_name, 0, &db);
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

bool db_put(MDB_dbi db, MDB_txn *txn, db_key_t *key, const void *value,
            size_t value_size, bool auto_commit) {
  if (txn == NULL)
    return false;

  MDB_val mdb_key, mdb_value;
  int rc;
  // For int keys: This variable holds the 32-bit integer key in a consistent
  // network byte order (big-endian) for storage and sorting.
  uint32_t key_in_network_order;

  switch (key->type) {
  case DB_KEY_STRING:
    // For strings, the data is the string itself and size is its length.
    mdb_key.mv_data = (void *)key->key.s;
    mdb_key.mv_size = strlen(key->key.s);
    break;

  case DB_KEY_INTEGER:
    /*
     * For 32-bit integers, we use htonl() (Host To Network Long) to
     * convert from the host machine's byte order to big-endian.
     * This is the standard and portable way to ensure that LMDB's
     * lexicographical sort matches the numerical sort order.
     */
    key_in_network_order = htonl(key->key.i);

    // The data points to our converted integer, and the size is fixed.
    mdb_key.mv_data = &key_in_network_order;
    mdb_key.mv_size = sizeof(key_in_network_order);
    break;

  default:
    return false;
  }

  mdb_value.mv_size = value_size;
  mdb_value.mv_data = (void *)value;

  rc = mdb_put(txn, db, &mdb_key, &mdb_value, 0);
  if (rc != 0) {
    fprintf(stderr, "db_put: mdb_put failed: %s\n", mdb_strerror(rc));
    return false;
  }

  if (auto_commit) {
    return db_commit_txn(txn);
  }

  return true;
}

void db_free_get_result(db_get_result_t *r) {
  if (r) {
    free(r->value);
    free(r);
  }
}

db_get_result_t *db_get(MDB_dbi db, MDB_txn *txn, db_key_t *key) {
  db_get_result_t *r = malloc(sizeof(db_get_result_t));
  r->status = DB_GET_ERROR;
  r->value = NULL;
  r->value_len = 0;
  if (txn == NULL)
    return r;

  MDB_val mdb_key, mdb_value;
  int rc;
  void *result = NULL;
  uint32_t key_in_network_order;

  switch (key->type) {
  case DB_KEY_STRING:
    mdb_key.mv_data = (void *)key->key.s;
    mdb_key.mv_size = strlen(key->key.s);
    break;

  case DB_KEY_INTEGER:
    key_in_network_order = htonl(key->key.i);
    mdb_key.mv_data = &key_in_network_order;
    mdb_key.mv_size = sizeof(key_in_network_order);
    break;

  default:
    return r;
  }

  rc = mdb_get(txn, db, &mdb_key, &mdb_value);
  if (rc == MDB_NOTFOUND) {
    // Key not found, which is not an error for get
    r->status = DB_GET_NOT_FOUND;
    return r;
  } else if (rc != 0) {
    fprintf(stderr, "db_get: mdb_get failed: %s\n", mdb_strerror(rc));
    return r;
  }

  // Duplicate the data as it's only valid within the transaction
  result = (void *)malloc(mdb_value.mv_size);
  if (result) {
    memcpy(result, mdb_value.mv_data, mdb_value.mv_size);
  }
  r->status = DB_GET_OK;
  r->value = result;
  r->value_len = mdb_value.mv_size;

  return r;
}

void db_close(MDB_env *env, MDB_dbi db) { mdb_dbi_close(env, db); }

void db_env_close(MDB_env *env) {
  if (env) {
    mdb_env_close(env);
    free(env);
  }
}

MDB_txn *db_create_txn(MDB_env *env, bool is_read_only) {
  MDB_txn *txn;
  int rc = mdb_txn_begin(env, NULL, is_read_only ? MDB_RDONLY : 0, &txn);
  if (rc == MDB_SUCCESS)
    return txn;
  fprintf(stderr, "db_create_txn: mdb_txn_begin failed: %s\n",
          mdb_strerror(rc));
  return NULL;
}

// Abandon all the operations of the transaction instead of saving them
void db_abort_txn(MDB_txn *txn) { mdb_txn_abort(txn); }

bool db_commit_txn(MDB_txn *txn) {
  int rc = mdb_txn_commit(txn);
  if (rc != 0) {
    fprintf(stderr, "db_commit_txn: mdb_txn_commit failed: %s\n",
            mdb_strerror(rc));
    return false;
  }
  return true;
}