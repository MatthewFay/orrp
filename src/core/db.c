#include "core/db.h"
#include "lmdb.h" // Include the LMDB library header
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Define your opaque database type, wrapping LMDB's environment and database handles
struct db_s
{
  MDB_env *env;
  MDB_dbi dbi;
};

db_t *db_open(const char *path, size_t map_size)
{
  db_t *db = (db_t *)malloc(sizeof(db_t));
  if (db == NULL)
  {
    perror("Failed to allocate db_t");
    return NULL;
  }

  int rc;

  // Create environment
  rc = mdb_env_create(&db->env);
  if (rc != 0)
  {
    fprintf(stderr, "mdb_env_create failed: %s\n", mdb_strerror(rc));
    free(db);
    return NULL;
  }

  // Set map size (maximum size of the database)
  rc = mdb_env_set_mapsize(db->env, map_size);
  if (rc != 0)
  {
    fprintf(stderr, "mdb_env_set_mapsize failed: %s\n", mdb_strerror(rc));
    mdb_env_close(db->env);
    free(db);
    return NULL;
  }

  // Open environment
  // MDB_NOSUBDIR: The environment path is a file, not a directory.
  // 0664: Permissions for the directory/file
  rc = mdb_env_open(db->env, path, MDB_NOSUBDIR, 0664);
  if (rc != 0)
  {
    fprintf(stderr, "mdb_env_open failed: %s\n", mdb_strerror(rc));
    mdb_env_close(db->env);
    free(db);
    return NULL;
  }

  MDB_txn *txn;
  rc = mdb_txn_begin(db->env, NULL, 0, &txn);
  if (rc != 0)
  {
    fprintf(stderr, "mdb_txn_begin failed: %s\n", mdb_strerror(rc));
    mdb_env_close(db->env);
    free(db);
    return NULL;
  }

  // Open a database (dbi) - will be created if it doesn't exist
  // 0: Default flags
  rc = mdb_dbi_open(txn, NULL, 0, &db->dbi);
  if (rc != 0)
  {
    fprintf(stderr, "mdb_dbi_open failed: %s\n", mdb_strerror(rc));
    mdb_txn_abort(txn);
    mdb_env_close(db->env);
    free(db);
    return NULL;
  }

  rc = mdb_txn_commit(txn);
  if (rc != 0)
  {
    fprintf(stderr, "mdb_txn_commit failed: %s\n", mdb_strerror(rc));
    mdb_env_close(db->env);
    free(db);
    return NULL;
  }

  return db;
}

bool db_put(db_t *db, const char *key, const char *value)
{
  if (db == NULL || db->env == NULL)
    return false;

  MDB_txn *txn;
  MDB_val mdb_key, mdb_value;
  int rc;

  rc = mdb_txn_begin(db->env, NULL, 0, &txn);
  if (rc != 0)
  {
    fprintf(stderr, "db_put: mdb_txn_begin failed: %s\n", mdb_strerror(rc));
    return false;
  }

  mdb_key.mv_size = strlen(key);
  mdb_key.mv_data = (void *)key;
  mdb_value.mv_size = strlen(value);
  mdb_value.mv_data = (void *)value;

  rc = mdb_put(txn, db->dbi, &mdb_key, &mdb_value, 0);
  if (rc != 0)
  {
    fprintf(stderr, "db_put: mdb_put failed: %s\n", mdb_strerror(rc));
    mdb_txn_abort(txn);
    return false;
  }

  rc = mdb_txn_commit(txn);
  if (rc != 0)
  {
    fprintf(stderr, "db_put: mdb_txn_commit failed: %s\n", mdb_strerror(rc));
    return false;
  }
  return true;
}

char *db_get(db_t *db, const char *key)
{
  if (db == NULL || db->env == NULL)
    return NULL;

  MDB_txn *txn;
  MDB_val mdb_key, mdb_value;
  int rc;
  char *result = NULL;

  rc = mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn); // Read-only transaction
  if (rc != 0)
  {
    fprintf(stderr, "db_get: mdb_txn_begin failed: %s\n", mdb_strerror(rc));
    return NULL;
  }

  mdb_key.mv_size = strlen(key);
  mdb_key.mv_data = (void *)key;

  rc = mdb_get(txn, db->dbi, &mdb_key, &mdb_value);
  if (rc == MDB_NOTFOUND)
  {
    // Key not found, which is not an error for get
    mdb_txn_abort(txn);
    return NULL;
  }
  else if (rc != 0)
  {
    fprintf(stderr, "db_get: mdb_get failed: %s\n", mdb_strerror(rc));
    mdb_txn_abort(txn);
    return NULL;
  }

  // Duplicate the data as it's only valid within the transaction
  result = (char *)malloc(mdb_value.mv_size + 1);
  if (result)
  {
    memcpy(result, mdb_value.mv_data, mdb_value.mv_size);
    result[mdb_value.mv_size] = '\0';
  }

  mdb_txn_abort(txn); // Abort read-only transaction (or commit, doesn't matter for read-only)
  return result;
}

void db_close(db_t *db)
{
  if (db)
  {
    if (db->env)
    {
      mdb_dbi_close(db->env, db->dbi); // Close the DBI
      mdb_env_close(db->env);          // Close the environment
    }
    free(db);
  }
}