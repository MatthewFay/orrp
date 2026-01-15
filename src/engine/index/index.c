#include "index.h"
#include "core/data_constants.h"
#include "core/db.h"
#include "lmdb.h"
#include "mpack.h"
#include <stdbool.h>

static const index_def_t DEFAULT_INDEXES[] = {
    {.key = "ts", .type = INDEX_TYPE_I64}, {0}};

static void _format_index_db_name(const char *key, char *out, size_t size) {
  snprintf(out, size, "index_%s_db", key);
}

static bool _decode_index_def(const char *data, size_t len, index_def_t *out) {
  mpack_reader_t reader;
  mpack_reader_init_data(&reader, data, len);

  out->key = NULL;
  out->type = 0;

  size_t count = mpack_expect_map(&reader);
  if (mpack_reader_error(&reader) != mpack_ok)
    return false;

  for (size_t i = 0; i < count; i++) {
    char key_buf[8];
    mpack_expect_cstr(&reader, key_buf, sizeof(key_buf));

    if (strcmp(key_buf, "key") == 0) {
      // mpack_expect_cstr_alloc mallocs and copies the string.
      out->key = mpack_expect_cstr_alloc(&reader, MAX_TEXT_VAL_LEN);
    } else if (strcmp(key_buf, "type") == 0) {
      out->type = (index_type_t)mpack_expect_int(&reader);
    } else {
      // UNKNOWN FIELD: Safely skip it (allows forward compatibility)
      mpack_discard(&reader);
    }
  }

  if (mpack_reader_destroy(&reader) != mpack_ok) {
    if (out->key)
      free(out->key); // Prevent leak on partial failure
    return false;
  }

  // Validation: Did we get the mandatory fields?
  if (out->key == NULL) {
    return false;
  }

  return true;
}

static db_put_result_t _index_put(MDB_dbi db, MDB_txn *txn,
                                  const index_def_t *index_def) {
  mpack_writer_t writer;
  char *data;
  size_t size;

  mpack_writer_init_growable(&writer, &data, &size);
  mpack_start_map(&writer, 2); // Map count: 2 (key, type)

  mpack_write_cstr(&writer, "key");
  mpack_write_cstr(&writer, index_def->key);

  mpack_write_cstr(&writer, "type");
  mpack_write_u32(&writer, index_def->type);

  mpack_finish_map(&writer);

  if (mpack_writer_destroy(&writer) != mpack_ok) {
    free(data);
    return DB_PUT_ERR;
  }

  db_key_t db_key = {.type = DB_KEY_STRING, .key.s = index_def->key};

  db_put_result_t res = db_put(db, txn, &db_key, data, size, false, true);

  free(data);
  return res;
}

// Syncs DB -> Registry
// This is a blind copy of MsgPack bytes (Zero-Copy logic)
static bool _write_from_db(MDB_dbi dbi, index_write_reg_opts_t *opts,
                           MDB_txn *txn) {
  db_cursor_entry_t cursor_entry;
  db_key_t db_key;
  MDB_cursor *src_cursor = db_cursor_open(opts->src_read_txn, opts->src_dbi);
  if (!src_cursor) {
    return false;
  }

  db_key.type = DB_KEY_STRING;

  while (db_cursor_next(src_cursor, &cursor_entry)) {
    db_key.key.s = cursor_entry.key;
    // we can simply copy bytes since value is MessagePack
    if (db_put(dbi, txn, &db_key, cursor_entry.value, cursor_entry.value_len,
               false, false) != DB_PUT_OK) {
      continue;
    }
  }

  db_cursor_close(src_cursor);

  return true;
}

bool index_write_registry(MDB_env *env, MDB_dbi dbi,
                          index_write_reg_opts_t *opts) {
  if (!env || !opts ||
      (opts->src == INDEX_WRITE_FROM_DB && opts->src_read_txn == NULL)) {
    return false;
  }
  MDB_txn *txn = db_create_txn(env, false);
  if (!txn) {
    return false;
  }
  bool result = false;
  if (opts->src == INDEX_WRITE_FROM_DB) {
    result = _write_from_db(dbi, opts, txn);
  } else {
    const index_def_t *ptr = DEFAULT_INDEXES;
    while (ptr->key != NULL) {
      if (_index_put(dbi, txn, ptr) != DB_PUT_OK) {
        db_abort_txn(txn);
        return false;
      }
      ptr++;
    }
    result = true;
  }

  if (result) {
    return db_commit_txn(txn);
  }
  db_abort_txn(txn);
  return false;
}

bool index_open_registry(MDB_env *env, MDB_dbi dbi,
                         khash_t(key_index) * *key_to_index) {
  if (!env || !key_to_index)
    return false;

  index_def_t defs[MAX_NUM_INDEXES];
  int def_count = 0;

  MDB_txn *__readtxn = db_create_txn(env, true);
  if (!__readtxn) {
    return false;
  }

  MDB_cursor *cursor = db_cursor_open(__readtxn, dbi);
  if (!cursor) {
    db_abort_txn(__readtxn);
    return false;
  }

  db_cursor_entry_t cursor_entry;

  while (db_cursor_next(cursor, &cursor_entry) && def_count < MAX_NUM_INDEXES) {
    // Allocates memory for def.key
    if (_decode_index_def(cursor_entry.value, cursor_entry.value_len,
                          &defs[def_count])) {
      def_count++;
    }
  }

  db_cursor_close(cursor);
  // Must close Read Txn before opening DBs below
  db_abort_txn(__readtxn);

  int ret;
  *key_to_index = kh_init(key_index);

  for (int i = 0; i < def_count; i++) {
    index_t index = {0};
    index.index_def = defs[i]; // Moves ownership of `def.key` pointer

    char db_name[MAX_TEXT_VAL_LEN];
    _format_index_db_name(index.index_def.key, db_name, sizeof(db_name));

    // db_open handles its own internal transaction
    // setting `int_only_keys` to true since we only support int64 index for
    // now
    if (!db_open(env, db_name, true, DB_DUP_KEYS_FIXED_SIZE_VALS,
                 &index.index_db)) {
      // Failed to open DB, free the key to avoid leak
      free(index.index_def.key);
      continue;
    }

    // Using `index_def.key` as map key ptr
    khiter_t k = kh_put(key_index, *key_to_index, index.index_def.key, &ret);

    kh_value(*key_to_index, k) = index;
  }

  return true;
}

db_put_result_t index_add(const index_def_t *index_def, MDB_env *env,
                          MDB_dbi dbi) {
  if (!index_def || !env)
    return DB_PUT_ERR;

  MDB_txn *txn = db_create_txn(env, false);
  if (!txn) {
    return DB_PUT_ERR;
  }

  db_put_result_t pr = _index_put(dbi, txn, index_def);
  if (pr != DB_PUT_OK) {
    db_abort_txn(txn);
    return pr;
  }
  return db_commit_txn(txn) ? DB_PUT_OK : DB_PUT_ERR;
}

bool index_get(const char *key, khash_t(key_index) * key_to_index,
               index_t *index_out) {
  if (!key || !key_to_index)
    return false;
  memset(index_out, 0, sizeof(index_t));
  khint_t k = kh_get(key_index, key_to_index, key);
  if (k != kh_end(key_to_index)) {
    *index_out = kh_value(key_to_index, k);
    return true;
  }
  return false;
}

bool index_get_count(khash_t(key_index) * key_to_index, uint32_t *count_out) {
  if (!key_to_index)
    return false;
  khint_t count = kh_size(key_to_index);
  *count_out = count;
  return true;
}

void index_close_registry(MDB_env *env, khash_t(key_index) * *key_to_index) {
  if (key_to_index && *key_to_index) {
    khint_t k;
    for (k = kh_begin(*key_to_index); k != kh_end(*key_to_index); ++k) {
      if (kh_exist(*key_to_index, k)) {
        const char *key_ptr = kh_key(*key_to_index, k);
        if (key_ptr) {
          free((char *)key_ptr);
        }
        index_t ci = kh_val(*key_to_index, k);
        db_close(env, ci.index_db);
        // Don't free `ci.index_def.key`, already freed as `key_ptr`
      }
    }
    kh_destroy(key_index, *key_to_index);
    *key_to_index = NULL;
  }
}