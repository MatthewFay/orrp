#include "consumer_validate.h"
#include "engine/op/op.h"
#include <string.h>

bool consumer_validate_op(const op_t *op) {
  if (!op) {
    return false;
  }

  switch (op->op_type) {
  case OP_ADD_VALUE:
  case OP_PUT:
  case OP_CACHE:
  case OP_COND_PUT:
  case OP_INCREMENT_TAG_COUNTER:
    break;
  default:
    return false;
  }

  if (op->target_type == OP_TARGET_NONE) {
    return false;
  }

  if (op->value_type == OP_VALUE_NONE) {
    return false;
  }

  if (!op->db_key.container_name || strlen(op->db_key.container_name) == 0) {
    return false;
  }

  switch (op->db_key.db_key.type) {
  case DB_KEY_STRING:
    if (!op->db_key.db_key.key.s || strlen(op->db_key.db_key.key.s) == 0) {
      return false;
    }
    break;
  case DB_KEY_INTEGER:
    // Integer keys are always valid
    break;
  default:
    return false;
  }

  switch (op->op_type) {
  case OP_COND_PUT:
    if (op->cond_type == COND_PUT_NONE) {
      return false;
    }
    // Fall through to validate target/value compatibility
  case OP_ADD_VALUE:
  case OP_PUT:
  case OP_CACHE:
    switch (op->target_type) {
    case OP_TARGET_INT32:
    case OP_TARGET_BITMAP:
    case OP_TARGET_STRING:
      break;
    default:
      return false;
    }

    switch (op->value_type) {
    case OP_VALUE_INT32:
    case OP_VALUE_STRING:
      break;
    default:
      return false;
    }

    if (op->value_type == OP_VALUE_STRING) {
      if (!op->value.str_value || strlen(op->value.str_value) == 0) {
        return false;
      }
    }

    if (op->op_type == OP_ADD_VALUE && !(op->target_type == OP_TARGET_INT32 ||
                                         op->target_type == OP_TARGET_BITMAP)) {
      return false;
    }
    if (op->op_type == OP_ADD_VALUE && op->value_type != OP_VALUE_INT32) {
      return false;
    }
    break;

  case OP_INCREMENT_TAG_COUNTER:
    if (op->target_type != OP_TARGET_TAG_COUNTER) {
      return false;
    }
    if (op->value_type != OP_VALUE_TAG_COUNTER_DATA) {
      return false;
    }
    if (!op->value.tag_counter_data) {
      return false;
    }
    if (!op->value.tag_counter_data->tag ||
        strlen(op->value.tag_counter_data->tag) == 0) {
      return false;
    }
    if (op->value.tag_counter_data->increment == 0) {
      return false; // Incrementing by 0 doesn't make sense
    }
    break;

  default:
    return false;
  }

  return true;
}