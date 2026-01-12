#ifndef INDEX_H
#define INDEX_H

#include "core/db.h"
#include "engine/container/container_types.h"
#include "engine/index/index_types.h"
#include <stdint.h>

// Returns true/false for index existence.
// If true, `index_out` will be set to valid index.
bool index_get(const char *key, eng_container_t *user_container,
               index_t *index_out);

// Returns false on error. Get count of indexes in user container.
bool index_get_count(eng_container_t *user_container, uint32_t *count_out);

bool index_init_user_registry(eng_container_t *user_container,
                              bool is_new_container, eng_container_t *sys_c);

bool index_init_sys_registry(eng_container_t *sys_c);

/**
 * Adds an index to the system registry
 */
db_put_result_t index_add_sys(const index_def_t *index_def);

void index_destroy_key_index(eng_container_t *usr_c);
#endif