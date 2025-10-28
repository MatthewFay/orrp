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

consumer_cache_entry_val_type_t
consumer_schema_op_to_cache_type(op_value_type_t op_val_type) {
  switch (op_val_type) {
  case OP_VALUE_BITMAP:
    return CONSUMER_CACHE_ENTRY_VAL_BM;
  case OP_VALUE_INT32:
    return CONSUMER_CACHE_ENTRY_VAL_INT32;
  case OP_VALUE_STRING:
    return CONSUMER_CACHE_ENTRY_VAL_STR;
  case OP_VALUE_NONE:
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

  consumer_cache_entry_val_type_t expected_type =
      consumer_schema_get_value_type(&op->db_key);

  consumer_cache_entry_val_type_t actual_type =
      consumer_schema_op_to_cache_type(op->value_type);

  if (expected_type == CONSUMER_CACHE_ENTRY_VAL_UNKNOWN ||
      actual_type == CONSUMER_CACHE_ENTRY_VAL_UNKNOWN) {
    return (schema_validation_result_t){
        .valid = false, .error_msg = "Expected or actual type unknown"};
  }

  if (expected_type != actual_type) {
    static char err_buf[256];
    const char *expected_str = "unknown";
    const char *actual_str = "unknown";

    switch (expected_type) {
    case CONSUMER_CACHE_ENTRY_VAL_BM:
      expected_str = "bitmap";
      break;
    case CONSUMER_CACHE_ENTRY_VAL_INT32:
      expected_str = "int32";
      break;
    case CONSUMER_CACHE_ENTRY_VAL_STR:
      expected_str = "string";
      break;
    case CONSUMER_CACHE_ENTRY_VAL_UNKNOWN:
      break;
    }

    switch (actual_type) {
    case CONSUMER_CACHE_ENTRY_VAL_BM:
      actual_str = "bitmap";
      break;
    case CONSUMER_CACHE_ENTRY_VAL_INT32:
      actual_str = "int32";
      break;
    case CONSUMER_CACHE_ENTRY_VAL_STR:
      actual_str = "string";
      break;
    case CONSUMER_CACHE_ENTRY_VAL_UNKNOWN:
      break;
    }

    snprintf(err_buf, sizeof(err_buf),
             "Value type mismatch: expected %s, got %s", expected_str,
             actual_str);

    return (schema_validation_result_t){.valid = false, .error_msg = err_buf};
  }

  switch (op->op_type) {
  case OP_TYPE_NONE:
    return (schema_validation_result_t){.valid = false,
                                        .error_msg = "Invalid operation type"};

  case OP_TYPE_COND_PUT:
    if (op->cond_type == COND_PUT_NONE) {
      return (schema_validation_result_t){
          .valid = false,
          .error_msg = "Conditional put missing condition type"};
    }
    // Conditional puts only work with integers
    if (actual_type != CONSUMER_CACHE_ENTRY_VAL_INT32) {
      return (schema_validation_result_t){
          .valid = false,
          .error_msg = "Conditional put only supports int32 values"};
    }
    break;

  case OP_TYPE_ADD_VALUE:
    // Add operations typically work with bitmaps or integers
    if (actual_type == CONSUMER_CACHE_ENTRY_VAL_STR) {
      return (schema_validation_result_t){
          .valid = false,
          .error_msg = "Add operation not supported for strings"};
    }
    break;

  case OP_TYPE_PUT:
  case OP_TYPE_CACHE:
    // These are generally permissive
    break;
  }

  return (schema_validation_result_t){.valid = true, .error_msg = NULL};
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