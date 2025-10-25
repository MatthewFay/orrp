#ifndef ENG_KEY_FORMAT_H
#define ENG_KEY_FORMAT_H

#include "engine/container/container_types.h"
#include "query/ast.h"
#include <stdbool.h>
#include <stddef.h>

// Turn custom tag AST node into a string representation
bool custom_tag_into(char *out_buf, size_t size, ast_node_t *custom_tag);

// Turn db key into a serialized string
bool db_key_into(char *buffer, size_t buffer_size,
                 eng_container_db_key_t *db_key);

// turn custom tag string + entity id into a serialized string
bool tag_str_entity_id_into(char *out_buf, size_t size, const char *custom_tag,
                            uint32_t entity_id);

// turn custom tag ast node + entity id into a serialized string
bool tag_entity_id_into(char *out_buf, size_t size, ast_node_t *custom_tag,
                        uint32_t entity_id);
// turn custom tag string + count into a serialized string
bool tag_count_into(char *out_buf, size_t size, const char *custom_tag,
                    uint32_t count);
#endif
