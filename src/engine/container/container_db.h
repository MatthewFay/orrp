#ifndef CONTAINER_DB_H
#define CONTAINER_DB_H

#include "container_types.h"
#include "lmdb.h"

void container_close(eng_container_t *c);

// `sys_read_txn` is optional. If needed, Will be created if
// none passed in.
container_result_t create_user_container(const char *name, const char *data_dir,
                                         size_t max_container_size,
                                         eng_container_t *sys_c,
                                         MDB_txn *sys_read_txn);

container_result_t create_system_container(const char *data_dir,
                                           size_t max_container_size);

bool cdb_get_db_handle(eng_container_t *c, eng_container_db_key_t *db_key,
                       MDB_dbi *db_out);

void cdb_free_db_key_contents(eng_container_db_key_t *db_key);

#endif