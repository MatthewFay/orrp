#ifndef op_MSG_H
#define op_MSG_H

/**
 * Operation types for database modifications
 */

#include "engine/container/container.h"
#include <stdint.h>

typedef enum {
  BM_ADD_VALUE,       // Add value to bitmap
  BM_CACHE,           // Cache bitmap
  PUT,                // Put key/value in db
  COND_PUT,           // Conditional put key/value in db
  INT_ADD_VALUE,      // Add value to existing integer value
  COUNT_TAG_INCREMENT // Increment tag count + update cumulative bitmap index
} op_type;

typedef enum {
  IF_EXISTING_LESS_THAN // Only put if existing value is less than new value
} cond_put_type;

// Data for COUNT_TAG_INCREMENT operation
typedef struct {
  char *container_name;
  char *tag; // e.g., "purchase:prod123"
  uint32_t entity_id;
  uint32_t increment; // Usually 1
} op_count_tag_data_t;

// An operation
typedef struct op_s {
  op_type op_type;
  cond_put_type cond_type; // Only relevant if op_type is COND_PUT

  // For simple key/value operations (PUT, COND_PUT, INT_ADD_VALUE,
  // BM_ADD_VALUE)
  eng_container_db_key_t db_key;

  // Operation-specific data
  union {
    char *str_value;
    uint32_t int32_value;
    // For COUNT_TAG_INCREMENT
    op_count_tag_data_t count_tag_data;
  } data;
} op_t;

op_t *op_create_str_val(eng_container_db_key_t db_key, op_type op_type,
                        cond_put_type cond_type, const char *val);

op_t *op_create_int32_val(eng_container_db_key_t db_key, op_type op_type,
                          cond_put_type cond_type, uint32_t val);

op_t *op_create_count_tag_increment(const char *container_name, const char *tag,
                                    uint32_t entity_id, uint32_t increment);

void op_destroy(op_t *op);

#endif