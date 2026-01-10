#ifndef INDEX_H
#define INDEX_H

#include "core/db.h"
#include "engine/container/container_types.h"
#include "engine/index/index_types.h"

bool init_user_indexes(eng_container_t *user_container, bool is_new_container,
                       eng_container_t *sys_c);

bool init_sys_index_registry(eng_container_t *sys_c);

/**
 * Adds an index to the system registry
 */
db_put_result_t index_add_sys(const index_def_t *index_def);

void index_destroy_key_index(eng_container_t *usr_c);
#endif