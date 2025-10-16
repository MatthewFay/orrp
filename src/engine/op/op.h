#ifndef op_H
#define op_H

/**
 * Operation types for database modifications
 */

#include "core/bitmaps.h"
#include "engine/container/container_types.h"
#include <stdint.h>

typedef enum {
  OP_TYPE_NONE = 0,
  OP_ADD_VALUE,            // Generic add operation
  OP_PUT,                  // Insert/update
  OP_COND_PUT,             // Conditional insert/update
  OP_CACHE,                // Cache operation
  OP_INCREMENT_TAG_COUNTER // Increment tag counter + update cumulative bitmap
                           // index
} op_type_t;

// what the operation acts on
typedef enum {
  OP_TARGET_NONE = 0,
  OP_TARGET_INT32,
  OP_TARGET_BITMAP,
  OP_TARGET_TAG_COUNTER,
  OP_TARGET_STRING
} op_target_type_t;

// what kind of data the operation carries or uses
typedef enum {
  OP_VALUE_NONE = 0,
  OP_VALUE_INT32,
  OP_VALUE_STRING,
  OP_VALUE_BITMAP,
  OP_VALUE_TAG_COUNTER_DATA
} op_value_type_t;

typedef enum {
  COND_PUT_NONE = 0,
  COND_PUT_IF_EXISTING_LESS_THAN // Only put if existing value is less than new
                                 // value
} cond_put_type_t;

// Data for OP_INCREMENT_TAG_COUNTER operation
typedef struct op_tag_counter_data_s {
  char *tag; // e.g., "purchase:prod123"
  uint32_t entity_id;
  uint32_t increment; // Usually 1
} op_tag_counter_data_t;

typedef struct {
  op_type_t op_type;
  op_target_type_t target_type; // where the op applies
  op_value_type_t value_type;   // what data the op uses
  cond_put_type_t cond_type;    // only relevant if op_type is OP_COND_PUT

  eng_container_db_key_t db_key;

  union {
    char *str_value;
    uint32_t int32_value;
    op_tag_counter_data_t *tag_counter_data;
    bitmap_t *bitmap_value;
  } value;
} op_t;

op_t *op_create(op_type_t op_type);
void op_destroy(op_t *op);

void op_set_target(op_t *op, op_target_type_t target_type,
                   const eng_container_db_key_t *key);
void op_set_condition(op_t *op, cond_put_type_t cond_type);
void op_set_value_int32(op_t *op, uint32_t val);
void op_set_value_str(op_t *op, const char *val);
void op_set_value_tag_counter(op_t *op, const char *tag, uint32_t entity_id,
                              uint32_t increment);

op_type_t op_get_type(const op_t *op);
op_target_type_t op_get_target_type(const op_t *op);
op_value_type_t op_get_value_type(const op_t *op);
cond_put_type_t op_get_condition_type(const op_t *op);
const eng_container_db_key_t *op_get_db_key(const op_t *op);
uint32_t op_get_value_int32(const op_t *op);
const char *op_get_value_str(const op_t *op);
const op_tag_counter_data_t *op_get_value_tag_counter(const op_t *op);

// Convenience / helper functions
op_t *op_create_str_val(const eng_container_db_key_t *db_key, op_type_t op_type,
                        op_target_type_t target_type, cond_put_type_t cond_type,
                        const char *val);

op_t *op_create_int32_val(const eng_container_db_key_t *db_key,
                          op_type_t op_type, op_target_type_t target_type,
                          cond_put_type_t cond_type, uint32_t val);

op_t *op_create_tag_counter_increment(const eng_container_db_key_t *db_key,
                                      const char *tag, uint32_t entity_id,
                                      uint32_t increment);

#endif