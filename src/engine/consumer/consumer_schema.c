#include "consumer_schema.h"
#include <string.h>

// ============================================================================
// Internal Schema Definitions
// ============================================================================

// Map each database to its value type
static consumer_cache_entry_val_type_t
_get_sys_db_value_type(eng_dc_sys_db_type_t db_type) {
  switch (db_type) {
  case SYS_DB_ENT_ID_TO_INT:
    // string entity_id -> uint32_t internal ID
    return CONSUMER_CACHE_ENTRY_VAL_INT32;

  case SYS_DB_INT_TO_ENT_ID:
    // uint32_t internal ID -> string entity_id
    return CONSUMER_CACHE_ENTRY_VAL_STR;

  case SYS_DB_METADATA:
    // TODO: Add uint64_t support
    return CONSUMER_CACHE_ENTRY_VAL_INT32;

  default:
    return CONSUMER_CACHE_ENTRY_VAL_UNKNOWN;
  }
}

static consumer_cache_entry_val_type_t
_get_user_db_value_type(eng_dc_user_db_type_t db_type) {
  switch (db_type) {
  case USER_DB_INVERTED_EVENT_INDEX:
    // tag string -> roaring bitmap of event_ids
    return CONSUMER_CACHE_ENTRY_VAL_BM;

  case USER_DB_EVENT_TO_ENTITY:
    // event_id (uint32_t) -> entity_id (uint32_t)
    return CONSUMER_CACHE_ENTRY_VAL_INT32;

  case USER_DB_METADATA:
    // Heterogeneous DB - metadata values
    return CONSUMER_CACHE_ENTRY_VAL_INT32;

  case USER_DB_COUNTER_STORE:
    // composite key (tag + entity_id) -> count (uint32_t)
    return CONSUMER_CACHE_ENTRY_VAL_INT32;

  case USER_DB_COUNT_INDEX:
    // composite key (tag + count) -> bitmap of entity_ids
    return CONSUMER_CACHE_ENTRY_VAL_BM;

  default:
    return CONSUMER_CACHE_ENTRY_VAL_UNKNOWN;
  }
}

// ============================================================================
// Internal Validation Helpers
// ============================================================================

// Validate operation type and value type combination
static schema_validation_result_t
_validate_op_value_combination(op_type_t op_type, op_value_type_t value_type,
                               consumer_cache_entry_val_type_t expected_db_type,
                               cond_put_type_t cond_type) {

  if (op_type == OP_TYPE_NONE) {
    return (schema_validation_result_t){
        .valid = false, .error_msg = "Invalid operation type: OP_TYPE_NONE"};
  }

  if (value_type == OP_VALUE_NONE) {
    return (schema_validation_result_t){
        .valid = false, .error_msg = "Invalid value type: OP_VALUE_NONE"};
  }

  switch (op_type) {
  case OP_TYPE_PUT:
    // PUT operations must match the database's expected type
    switch (expected_db_type) {
    case CONSUMER_CACHE_ENTRY_VAL_BM:
      if (value_type != OP_VALUE_BITMAP) {
        return (schema_validation_result_t){
            .valid = false,
            .error_msg = "PUT to bitmap database requires bitmap value"};
      }
      break;
    case CONSUMER_CACHE_ENTRY_VAL_INT32:
      if (value_type != OP_VALUE_INT32) {
        return (schema_validation_result_t){
            .valid = false,
            .error_msg = "PUT to int32 database requires int32 value"};
      }
      break;
    case CONSUMER_CACHE_ENTRY_VAL_STR:
      if (value_type != OP_VALUE_STRING) {
        return (schema_validation_result_t){
            .valid = false,
            .error_msg = "PUT to string database requires string value"};
      }
      break;
    default:
      return (schema_validation_result_t){
          .valid = false, .error_msg = "PUT to unknown database type"};
    }
    break;

  case OP_TYPE_ADD_VALUE:
    // ADD operations work differently based on target database type
    switch (expected_db_type) {
    case CONSUMER_CACHE_ENTRY_VAL_BM:
      // Adding to a bitmap requires an int32 value (the element to add)
      if (value_type != OP_VALUE_INT32) {
        return (schema_validation_result_t){
            .valid = false, .error_msg = "ADD to bitmap requires int32 value"};
      }
      break;
    case CONSUMER_CACHE_ENTRY_VAL_INT32:
      // Adding to an int32 requires an int32 value (increment amount)
      if (value_type != OP_VALUE_INT32) {
        return (schema_validation_result_t){
            .valid = false, .error_msg = "ADD to int32 requires int32 value"};
      }
      break;
    case CONSUMER_CACHE_ENTRY_VAL_STR:
      return (schema_validation_result_t){
          .valid = false,
          .error_msg = "ADD operation not supported for string databases"};
    default:
      return (schema_validation_result_t){
          .valid = false, .error_msg = "ADD to unknown database type"};
    }
    break;

  case OP_TYPE_COND_PUT:
    // Conditional puts only work with integers
    if (expected_db_type != CONSUMER_CACHE_ENTRY_VAL_INT32) {
      return (schema_validation_result_t){
          .valid = false,
          .error_msg = "Conditional put only supported for int32 databases"};
    }
    if (value_type != OP_VALUE_INT32) {
      return (schema_validation_result_t){
          .valid = false, .error_msg = "Conditional put requires int32 value"};
    }
    if (cond_type == COND_PUT_NONE) {
      return (schema_validation_result_t){
          .valid = false,
          .error_msg = "Conditional put missing condition type"};
    }
    break;

  case OP_TYPE_CACHE:
    // Cache operations should match the database type
    switch (expected_db_type) {
    case CONSUMER_CACHE_ENTRY_VAL_BM:
      if (value_type != OP_VALUE_BITMAP) {
        return (schema_validation_result_t){
            .valid = false,
            .error_msg = "CACHE to bitmap database requires bitmap value"};
      }
      break;
    case CONSUMER_CACHE_ENTRY_VAL_INT32:
      if (value_type != OP_VALUE_INT32) {
        return (schema_validation_result_t){
            .valid = false,
            .error_msg = "CACHE to int32 database requires int32 value"};
      }
      break;
    case CONSUMER_CACHE_ENTRY_VAL_STR:
      if (value_type != OP_VALUE_STRING) {
        return (schema_validation_result_t){
            .valid = false,
            .error_msg = "CACHE to string database requires string value"};
      }
      break;
    default:
      return (schema_validation_result_t){
          .valid = false, .error_msg = "CACHE to unknown database type"};
    }
    break;

  case OP_TYPE_NONE:
  default:
    return (schema_validation_result_t){.valid = false,
                                        .error_msg = "Unknown operation type"};
  }

  return (schema_validation_result_t){.valid = true, .error_msg = NULL};
}

// ============================================================================
// Public API - Type Mapping
// ============================================================================

consumer_cache_entry_val_type_t
consumer_schema_get_value_type(const eng_container_db_key_t *db_key) {
  if (!db_key) {
    return CONSUMER_CACHE_ENTRY_VAL_UNKNOWN;
  }

  switch (db_key->dc_type) {
  case CONTAINER_TYPE_SYSTEM:
    return _get_sys_db_value_type(db_key->sys_db_type);

  case CONTAINER_TYPE_USER:
    return _get_user_db_value_type(db_key->user_db_type);

  default:
    return CONSUMER_CACHE_ENTRY_VAL_UNKNOWN;
  }
}

// ============================================================================
// Public API - Validation
// ============================================================================

schema_validation_result_t consumer_schema_validate_op(const op_t *op) {
  if (!op) {
    return (schema_validation_result_t){.valid = false,
                                        .error_msg = "Operation is NULL"};
  }

  consumer_cache_entry_val_type_t expected_db_type =
      consumer_schema_get_value_type(&op->db_key);

  if (expected_db_type == CONSUMER_CACHE_ENTRY_VAL_UNKNOWN) {
    return (schema_validation_result_t){
        .valid = false, .error_msg = "Unknown database type for operation"};
  }

  return _validate_op_value_combination(op->op_type, op->value_type,
                                        expected_db_type, op->cond_type);
}

schema_validation_result_t
consumer_schema_validate_msg(const op_queue_msg_t *msg) {
  if (!msg) {
    return (schema_validation_result_t){.valid = false,
                                        .error_msg = "Message is NULL"};
  }

  if (!msg->op) {
    return (schema_validation_result_t){
        .valid = false, .error_msg = "Message operation is NULL"};
  }

  if (!msg->ser_db_key) {
    return (schema_validation_result_t){
        .valid = false, .error_msg = "Message serialized key is NULL"};
  }

  return consumer_schema_validate_op(msg->op);
}