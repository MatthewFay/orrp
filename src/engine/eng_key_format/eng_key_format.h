#ifndef ENG_KEY_FORMAT_H
#define ENG_KEY_FORMAT_H

#include "engine/container/container_types.h"
#include "query/ast.h"
#include <stdbool.h>
#include <stddef.h>

// Turn custom tag AST node into a string representation
bool custom_tag_into(char *out_buf, size_t size, ast_node_t *custom_tag);

bool db_key_into(char *buffer, size_t buffer_size,
                 eng_container_db_key_t *db_key);

#endif
