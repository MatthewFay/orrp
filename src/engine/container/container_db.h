#ifndef CONTAINER_DB_H
#define CONTAINER_DB_H

#include "container_types.h"

void container_close(eng_container_t *c);

container_result_t create_user_container(const char *name, const char *data_dir,
                                         size_t max_container_size,
                                         eng_container_t *sys_c);

container_result_t create_system_container(const char *data_dir,
                                           size_t max_container_size);

bool cdb_get_user_db_handle(eng_container_t *c, eng_dc_user_db_type_t db_type,
                            MDB_dbi *db_out);

bool cdb_get_system_db_handle(eng_container_t *c, eng_dc_sys_db_type_t db_type,
                              MDB_dbi *db_out);

void cdb_free_db_key_contents(eng_container_db_key_t *db_key);

#endif