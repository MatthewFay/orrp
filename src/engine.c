#include <string.h>
#include <stdlib.h>
#include "engine.h"
#include "core/bitmaps.h"
#include "core/db.h"

const char *MDB_PATH = "data/database.mdb";
const size_t DB_SIZE = 1048576;
const int MAX_NUM_DBS = 5;

const char *ENG_TXN_ERR = "Transaction error";
const char *ENG_ID_TRANSL_ERR = "Id translation error";

const char *DB_ID_TO_INT_NAME = "id_to_int";
const char *DB_INT_TO_ID_NAME = "int_to_id";
const char *DB_METADATA_NAME = "metadata";
const char *NEXT_ID_KEY = "next_id";
const u_int32_t NEXT_ID_INIT_VAL = 1;
const char *DB_EVENT_COUNTERS_NAME = "event_counters";
const char *DB_BITMAPS_NAME = "bitmaps";

// n_id will be 0 on failure
static void _get_next_n_id(eng_db_t *db, MDB_txn *txn, char *id, u_int32_t *n_id)
{
  bool r;
  db_get_result_t *next = db_get(db->metadata_db, txn, NEXT_ID_KEY);
  if (next->status == DB_GET_OK)
  {
    r = db_put(db->metadata_db, txn, NEXT_ID_KEY, *(u_int32_t *)next->value + 1, false);
    *n_id = r ? *(u_int32_t *)next : 0;
  }
  *n_id = 0; // Should only happen if DB has not been initialized!
  db_free_get_result(next);
}

// Initialize the databases
eng_db_t *eng_init_dbs()
{
  eng_db_t *db = malloc(sizeof(eng_db_t));
  if (!db)
    return NULL;

  MDB_env *env = db_create_env(MDB_PATH, DB_SIZE, MAX_NUM_DBS);
  if (!env)
    return NULL;
  MDB_dbi *id_to_int_db = db_open(env, DB_ID_TO_INT_NAME);
  MDB_dbi *int_to_id_db = db_open(env, DB_INT_TO_ID_NAME);
  MDB_dbi *metadata_db = db_open(env, DB_METADATA_NAME);
  MDB_dbi *event_counters_db = db_open(env, DB_EVENT_COUNTERS_NAME);
  MDB_dbi *bitmaps_db = db_open(env, DB_BITMAPS_NAME);

  if (!id_to_int_db || !int_to_id_db || !metadata_db || !event_counters_db || !bitmaps_db)
    return NULL;
  db->env = env;
  db->id_to_int_db = id_to_int_db;
  db->int_to_id_db = int_to_id_db;
  db->metadata_db = metadata_db;
  db->event_counters_db = event_counters_db;
  db->bitmaps_db = bitmaps_db;

  MDB_txn *txn = db_create_txn(db->env, false);
  if (!txn)
    return NULL;

  // Make sure next_id counter does not exist before creating
  db_get_result_t *r = db_get(metadata_db, txn, NEXT_ID_KEY);

  if (r->status == DB_GET_NOT_FOUND)
  {
    bool put_r = db_put(metadata_db, txn, NEXT_ID_KEY, &NEXT_ID_INIT_VAL, false);
    bool committed = put_r && db_commit_txn(txn);
    db_free_get_result(r);
    return db;
  }

  db_abort_txn(txn);
  bool ok = r->status == DB_GET_OK;

  db_free_get_result(r);
  return ok ? db : NULL;
}

static void
_map_str_id_to_numeric(eng_db_t *db, MDB_txn *txn, char *id, uint32_t *n_id)
{
  db_get_result_t *r = db_get(db->id_to_int_db, txn, id);

  if (r->status == DB_GET_NOT_FOUND)
  {
    _get_next_n_id(db, txn, id, n_id);
  }
  else if (r->status == DB_GET_OK)
  {
    *n_id = *(uint32_t *)r->value;
  }

  db_free_get_result(r);
}

void eng_set(api_response_t *r, eng_db_t *db, char *ns, char *bitmap, char *id)
{
  uint32_t n_id = 0;
  MDB_txn *txn = db_create_txn(db->env, false);
  if (!txn)
  {
    r->err_msg = ENG_TXN_ERR;
    return;
  }
  _map_str_id_to_numeric(db, txn, id, &n_id);
  if (!n_id)
  {
    r->err_msg = ENG_ID_TRANSL_ERR;
    return;
  }
}