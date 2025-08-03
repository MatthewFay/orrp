#include "engine.h"
#include "api.h"
#include "core/bitmaps.h"
#include "core/db.h"
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char *MDB_PATH = "data/database.mdb";
const size_t DB_SIZE = 1048576;
const int MAX_NUM_DBS = 5;

const char *ENG_TXN_ERR = "Transaction error";
const char *ENG_ID_TRANSL_ERR = "Id translation error";
const char *ENG_COUNTER_ERR = "Counter error";
const char *ENG_BITMAP_ERR = "Bitmap error";
const char *ENG_TXN_COMMIT_ERR = "Transaction Commit error";

const char *DB_ID_TO_INT_NAME = "id_to_int";
const char *DB_INT_TO_ID_NAME = "int_to_id";
const char *DB_METADATA_NAME = "metadata";
const char *NEXT_ID_KEY = "next_id";
const u_int32_t NEXT_ID_INIT_VAL = 1;
const char *DB_EVENT_COUNTERS_NAME = "event_counters";
const char *DB_BITMAPS_NAME = "bitmaps";

// n_id will be 0 on failure
static void _get_next_n_id(eng_db_t *db, MDB_txn *txn, u_int32_t *n_id) {
  bool r;
  db_key_t key;
  key.type = DB_KEY_STRING;
  key.key.s = NEXT_ID_KEY;
  db_get_result_t *next = db_get(db->metadata_db, txn, &key);
  if (next->status == DB_GET_OK) {
    u_int32_t next_id_val = *(u_int32_t *)next->value + 1;
    r = db_put(db->metadata_db, txn, &key, &next_id_val, sizeof(u_int32_t),
               false);
    *n_id = r ? *(u_int32_t *)next : 0;
  }
  *n_id = 0; // Should only happen if DB has not been initialized!
  db_free_get_result(next);
}

// Initialize the databases
eng_db_t *eng_init_dbs() {
  MDB_dbi id_to_int_db, int_to_id_db, metadata_db, event_counters_db,
      bitmaps_db;

  eng_db_t *db = malloc(sizeof(eng_db_t));
  if (!db)
    return NULL;

  MDB_env *env = db_create_env(MDB_PATH, DB_SIZE, MAX_NUM_DBS);
  if (!env)
    return NULL;
  bool id_to_int_db_r = db_open(env, DB_ID_TO_INT_NAME, &id_to_int_db);
  bool int_to_id_db_r = db_open(env, DB_INT_TO_ID_NAME, &int_to_id_db);
  bool metadata_db_r = db_open(env, DB_METADATA_NAME, &metadata_db);
  bool event_counters_db_r =
      db_open(env, DB_EVENT_COUNTERS_NAME, &event_counters_db);
  bool bitmaps_db_r = db_open(env, DB_BITMAPS_NAME, &bitmaps_db);

  if (!id_to_int_db_r || !int_to_id_db_r || !metadata_db_r ||
      !event_counters_db_r || !bitmaps_db_r)
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
  db_key_t key;
  key.type = DB_KEY_STRING;
  key.key.s = NEXT_ID_KEY;
  db_get_result_t *r = db_get(metadata_db, txn, &key);

  if (r->status == DB_GET_NOT_FOUND) {
    bool put_r = db_put(metadata_db, txn, &key, &NEXT_ID_INIT_VAL,
                        sizeof(u_int32_t), false);
    bool committed = put_r && db_commit_txn(txn);
    db_free_get_result(r);
    return committed ? db : NULL;
  }

  db_abort_txn(txn);
  bool ok = r->status == DB_GET_OK;

  db_free_get_result(r);
  return ok ? db : NULL;
}

static bool _map(eng_db_t *db, MDB_txn *txn, const char *id, uint32_t n_id) {
  db_key_t id_key;
  id_key.type = DB_KEY_STRING;
  id_key.key.s = id;
  bool put_r =
      db_put(db->id_to_int_db, txn, &id_key, &n_id, sizeof(u_int32_t), false);
  if (!put_r) {
    return false;
  }
  db_key_t n_id_key;
  n_id_key.type = DB_KEY_INTEGER;
  n_id_key.key.i = n_id;
  put_r = db_put(db->int_to_id_db, txn, &n_id_key, id, strlen(id) + 1, false);
  if (!put_r) {
    return false;
  }
  return true;
}

// n_id (numeric id) will be 0 on error
static void _map_str_id_to_numeric(eng_db_t *db, MDB_txn *txn, char *id,
                                   uint32_t *n_id) {
  bool map_r;
  db_key_t key;
  key.type = DB_KEY_STRING;
  key.key.s = id;
  db_get_result_t *r = db_get(db->id_to_int_db, txn, &key);

  switch (r->status) {
  case DB_GET_NOT_FOUND:
    _get_next_n_id(db, txn, n_id);
    if (!n_id)
      break;
    map_r = _map(db, txn, id, *n_id);
    if (!map_r)
      *n_id = 0;
    break;
  case DB_GET_OK:
    *n_id = *(uint32_t *)r->value;

    break;
  case DB_GET_ERROR:
    *n_id = 0;
    break;
  }

  db_free_get_result(r);
}

typedef struct incr_result_s {
  bool ok;
  u_int32_t count;
} incr_result_t;

static void _construct_counter_key_into(char *out_buf, size_t size, char *ns,
                                        char *key, char *id) {
  snprintf(out_buf, size, "%s:%s:%s", ns ? ns : "", key ? key : "",
           id ? id : "");
}

static void _construct_bitmap_key_into(char *out_buf, size_t size, char *ns,
                                       char *key, incr_result_t *r) {
  snprintf(out_buf, size, "%" PRIu32 ":%s:%s", r->count, ns ? ns : "",
           key ? key : "");
}

incr_result_t *_incr(eng_db_t *db, MDB_txn *txn, char *ns, char *key,
                     char *id) {
  bool put_r;
  uint32_t init_val = 1;
  uint32_t new_count;

  char counter_buffer[512];
  incr_result_t *r = malloc(sizeof(incr_result_t));
  r->ok = false;
  r->count = 0;
  _construct_counter_key_into(counter_buffer, sizeof(counter_buffer), ns, key,
                              id);
  db_key_t c_key;
  c_key.type = DB_KEY_STRING;
  c_key.key.s = counter_buffer;
  db_get_result_t *counter = db_get(db->event_counters_db, txn, &c_key);

  switch (counter->status) {
  case DB_GET_NOT_FOUND:
    put_r = db_put(db->event_counters_db, txn, &c_key, &init_val,
                   sizeof(uint32_t), false);
    if (put_r) {
      r->ok = true;
      r->count = 1;
    }
    break;
  case DB_GET_OK:
    new_count = *(uint32_t *)counter->value + 1;
    put_r = db_put(db->event_counters_db, txn, &c_key, &new_count,
                   sizeof(uint32_t), false);
    if (put_r) {
      r->ok = true;
      r->count = new_count;
    }
    break;
  case DB_GET_ERROR:
    break;
  }
  db_free_get_result(counter);
  return r;
}

static bool _upsert_bitmap(eng_db_t *db, MDB_txn *txn, char *ns, char *key,
                           uint32_t n_id, incr_result_t *counter_r) {
  bool r = false;
  char bitmap_buffer[512];
  bitmap_t *bm;
  _construct_bitmap_key_into(bitmap_buffer, sizeof(bitmap_buffer), ns, key,
                             counter_r);
  db_key_t b_key;
  size_t serialized_size;

  b_key.type = DB_KEY_STRING;
  b_key.key.s = bitmap_buffer;
  db_get_result_t *get_r = db_get(db->bitmaps_db, txn, &b_key);
  switch (get_r->status) {
  case DB_GET_OK:
    break;
  case DB_GET_NOT_FOUND:
    bm = bitmap_create();
    bitmap_add(bm, n_id);
    void *buffer = bitmap_serialize(bm, &serialized_size);
    if (!buffer) {
      bitmap_free(bm);
      break;
    }
    r = db_put(db->bitmaps_db, txn, &b_key, buffer, serialized_size, false);
    if (!r) {
      bitmap_free(bm);
      free(buffer);
    }
    break;
  case DB_GET_ERROR:
    break;
  }
  db_free_get_result(get_r);
  return r;
}

void eng_add(api_response_t *r, eng_db_t *db, char *ns, char *key, char *id) {
  uint32_t n_id = 0;
  MDB_txn *txn = db_create_txn(db->env, false);
  if (!txn) {
    r->err_msg = ENG_TXN_ERR;
    return;
  }
  _map_str_id_to_numeric(db, txn, id, &n_id);
  if (!n_id) {
    db_abort_txn(txn);
    r->err_msg = ENG_ID_TRANSL_ERR;
    return;
  }
  incr_result_t *counter_r = _incr(db, txn, ns, key, id);
  if (!counter_r->ok) {
    db_abort_txn(txn);
    free(counter_r);
    r->err_msg = ENG_COUNTER_ERR;
    return;
  }
  bool bm_r = _upsert_bitmap(db, txn, ns, key, n_id, counter_r);
  if (!bm_r) {
    db_abort_txn(txn);
    free(counter_r);
    r->err_msg = ENG_BITMAP_ERR;
    return;
  }
  free(counter_r);

  bool txn_r = db_commit_txn(txn);
  if (!txn_r) {
    r->err_msg = ENG_TXN_COMMIT_ERR;
    return;
  }
  r->is_ok = true;
}