#ifndef op_H
#define op_H

/**
 * Operation types for bitmaps
 */

#include "engine/container/container_types.h"
#include <stddef.h>
#include <stdint.h>

typedef enum {
  OP_TYPE_NONE = 0,
  OP_TYPE_ADD,
} op_type_t;

typedef struct {
  op_type_t op_type;
  eng_container_db_key_t db_key;
  uint32_t value;
} op_t;

op_t *op_create(op_type_t op_type, const eng_container_db_key_t *db_key,
                uint32_t value);
void op_destroy(op_t *op);

#endif