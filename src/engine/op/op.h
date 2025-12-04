#ifndef op_H
#define op_H

/**
 * Operation types for database modifications
 */

#include "core/bitmaps.h"
#include "engine/container/container_types.h"
#include <stddef.h>
#include <stdint.h>

typedef enum {
  OP_TYPE_NONE = 0,
  OP_TYPE_ADD_VALUE, // Generic add operation
  OP_TYPE_PUT,       // Insert/update
  OP_TYPE_COND_PUT,  // Conditional insert/update
  OP_TYPE_CACHE,     // Cache operation
} parser_op_type_t;

typedef enum {
  OP_VALUE_NONE = 0,
  OP_VALUE_INT32,
  OP_VALUE_STRING,
  OP_VALUE_BITMAP,
  OP_VALUE_MSGPACK
} op_value_type_t;

typedef enum {
  COND_PUT_NONE = 0,
  COND_PUT_IF_EXISTING_LESS_THAN // Only put if existing value is less than new
                                 // value
} cond_put_type_t;

typedef struct {
  parser_op_type_t op_type;
  cond_put_type_t cond_type; // only relevant if op_type is OP_COND_PUT

  eng_container_db_key_t db_key;

  op_value_type_t value_type;
  union {
    char *str;
    uint32_t int32;
    bitmap_t *bitmap;
    char *msgpack;
  } value;

  // Currently only set for MessagePack
  size_t val_size;
} op_t;

op_t *op_create(parser_op_type_t op_type);
void op_destroy(op_t *op);

void op_set_target(op_t *op, const eng_container_db_key_t *key);
void op_set_condition(op_t *op, cond_put_type_t cond_type);
void op_set_value_int32(op_t *op, uint32_t val);
void op_set_value_str(op_t *op, const char *val);

parser_op_type_t op_get_type(const op_t *op);
op_value_type_t op_get_value_type(const op_t *op);
cond_put_type_t op_get_condition_type(const op_t *op);
const eng_container_db_key_t *op_get_db_key(const op_t *op);
uint32_t op_get_value_int32(const op_t *op);
const char *op_get_value_str(const op_t *op);

// Convenience / helper functions
op_t *op_create_str_val(const eng_container_db_key_t *db_key,
                        parser_op_type_t op_type, cond_put_type_t cond_type,
                        const char *val);

op_t *op_create_int32_val(const eng_container_db_key_t *db_key,
                          parser_op_type_t op_type, cond_put_type_t cond_type,
                          uint32_t val);

op_t *op_create_msgpack_val(const eng_container_db_key_t *db_key, char *msgpack,
                            size_t msgpack_size);

#endif