#include "index.h"
#include "core/data_constants.h"
#include "engine/container/container.h"
#include "engine/container/container_types.h"
#include "mpack.h"

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

// Syncs System Global Registry -> User Local Registry
// This is a blind copy of MsgPack bytes (Zero-Copy logic)
static bool _init_user_local_index_registry(eng_container_t *user_container,
                                            eng_container_t *sys_c) {
  db_cursor_entry_t cursor_entry;
  db_key_t db_key;
  MDB_txn *sys_txn = db_create_txn(sys_c->env, true);
  if (!sys_txn) {
    return false;
  }
  MDB_cursor *sys_cursor =
      db_cursor_open(sys_txn, sys_c->data.sys->index_registry_global_db);
  if (!sys_cursor) {
    db_abort_txn(sys_txn);
    return false;
  }

  MDB_txn *usr_txn = db_create_txn(user_container->env, false);
  if (!usr_txn) {
    db_cursor_close(sys_cursor);
    db_abort_txn(sys_txn);
    return false;
  }

  db_key.type = DB_KEY_STRING;

  while (db_cursor_next(sys_cursor, &cursor_entry)) {
    db_key.key.s = cursor_entry.key;
    // we can simply copy bytes since value is MessagePack
    if (db_put(user_container->data.usr->index_registry_local_db, usr_txn,
               &db_key, cursor_entry.value, cursor_entry.value_len, false,
               false) != DB_PUT_OK) {
      continue;
    }
  }

  db_cursor_close(sys_cursor);
  db_abort_txn(sys_txn);

  if (!db_commit_txn(usr_txn)) {
    return false;
  }
  return true;
}

bool init_user_indexes(eng_container_t *user_container, bool is_new_container,
                       eng_container_t *sys_c) {
  if (!user_container || !sys_c)
    return false;

  if (is_new_container &&
      !_init_user_local_index_registry(user_container, sys_c)) {
    return false;
  }

  index_def_t defs[USR_CONTAINER_MAX_NUM_INDEXES];
  int def_count = 0;

  MDB_txn *usr__readtxn = db_create_txn(user_container->env, true);
  if (!usr__readtxn) {
    return false;
  }

  MDB_cursor *cursor = db_cursor_open(
      usr__readtxn, user_container->data.usr->index_registry_local_db);
  if (!cursor) {
    db_abort_txn(usr__readtxn);
    return false;
  }

  db_cursor_entry_t cursor_entry;

  while (db_cursor_next(cursor, &cursor_entry) &&
         def_count < USR_CONTAINER_MAX_NUM_INDEXES) {
    // Allocates memory for def.key
    if (_decode_index_def(cursor_entry.value, cursor_entry.value_len,
                          &defs[def_count])) {
      def_count++;
    }
  }

  db_cursor_close(cursor);
  // Must close Read Txn before opening DBs below
  db_abort_txn(usr__readtxn);

  int ret;
  user_container->data.usr->key_to_index = kh_init(key_index);

  for (int i = 0; i < def_count; i++) {
    index_t index = {0};
    index.index_def = defs[i]; // Moves ownership of `def.key` pointer

    char db_name[MAX_TEXT_VAL_LEN];
    _format_index_db_name(index.index_def.key, db_name, sizeof(db_name));

    // db_open handles its own internal transaction
    // setting `int_only_keys` to true since we only support int64 index for
    // now
    if (!db_open(user_container->env, db_name, true,
                 DB_DUP_KEYS_FIXED_SIZE_VALS, &index.index_db)) {
      // Failed to open DB, free the key to avoid leak
      free(index.index_def.key);
      continue;
    }

    // Using `index_def.key` as key ptr
    khiter_t k = kh_put(key_index, user_container->data.usr->key_to_index,
                        index.index_def.key, &ret);

    kh_value(user_container->data.usr->key_to_index, k) = index;
  }

  return true;
}

static db_put_result_t _sys_index_put(eng_container_t *sys_c, MDB_txn *sys_txn,
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

  db_put_result_t res = db_put(sys_c->data.sys->index_registry_global_db,
                               sys_txn, &db_key, data, size, false, true);

  free(data);
  return res;
}

db_put_result_t index_add_sys(const index_def_t *index_def) {
  if (!index_def)
    return DB_PUT_ERR;

  container_result_t sys_cr = container_get_system();
  if (!sys_cr.success) {
    return DB_PUT_ERR;
  }
  eng_container_t *sys_c = sys_cr.container;
  MDB_txn *sys_txn = db_create_txn(sys_c->env, false);
  if (!sys_txn) {
    return DB_PUT_ERR;
  }

  db_put_result_t pr = _sys_index_put(sys_c, sys_txn, index_def);
  if (pr != DB_PUT_OK) {
    db_abort_txn(sys_txn);
    return pr;
  }
  return db_commit_txn(sys_txn) ? DB_PUT_OK : DB_PUT_ERR;
}

void index_destroy_key_index(eng_container_t *usr_c) {
  kh_key_index_t *key_to_index = usr_c->data.usr->key_to_index;
  if (key_to_index) {
    khint_t k;
    for (k = kh_begin(key_to_index); k != kh_end(key_to_index); ++k) {
      if (kh_exist(key_to_index, k)) {
        const char *key_ptr = kh_key(key_to_index, k);
        if (key_ptr) {
          free((char *)key_ptr);
        }
        index_t ci = kh_val(key_to_index, k);
        db_close(usr_c->env, ci.index_db);
        // Don't free `ci.index_def.key`, already freed as `key_ptr`
      }
    }

    kh_destroy(key_index, key_to_index);
    usr_c->data.usr->key_to_index = NULL;
  }
}

 bool init_sys_index_registry(eng_container_t *sys_c) {
  MDB_txn *sys_txn = db_create_txn(sys_c->env, false);
  if (!sys_txn)
    return false;

  const index_def_t *ptr = DEFAULT_INDEXES;
  while (ptr->key != NULL) {
    if (!_sys_index_put(sys_c, sys_txn, ptr)) {
      db_abort_txn(sys_txn);
      return false;
    }
    ptr++;
  }

  return db_commit_txn(sys_txn);
}
