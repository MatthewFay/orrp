#include "op.h"
#include "engine/container/container.h"
#include <stdlib.h>
#include <string.h>

op_t *op_create(op_type_t op_type) {
  op_t *op = (op_t *)calloc(1, sizeof(op_t));
  if (!op) {
    return NULL;
  }

  op->op_type = op_type;
  op->cond_type = COND_PUT_NONE;
  op->value_type = OP_VALUE_NONE;

  memset(&op->db_key, 0, sizeof(eng_container_db_key_t));
  memset(&op->value, 0, sizeof(op->value));

  return op;
}

void op_destroy(op_t *op) {
  if (!op) {
    return;
  }

  container_free_db_key_contents(&op->db_key);

  switch (op->value_type) {
  case OP_VALUE_STRING:
    if (op->value.str) {
      free(op->value.str);
      op->value.str = NULL;
    }
    break;
  case OP_VALUE_BITMAP:
    bitmap_free(op->value.bitmap);
    break;
  case OP_VALUE_INT32:
  case OP_VALUE_NONE:
  default:
    // No cleanup needed
    break;
  }

  free(op);
}

void op_set_target(op_t *op, const eng_container_db_key_t *key) {
  if (!op || !key) {
    return;
  }
  memcpy(&op->db_key, key, sizeof(eng_container_db_key_t));
}

void op_set_condition(op_t *op, cond_put_type_t cond_type) {
  if (!op) {
    return;
  }

  op->cond_type = cond_type;
}

static void _clean_up_prev_val(op_t *op) {
  if (op->value_type == OP_VALUE_STRING && op->value.str) {
    free(op->value.str);
    op->value.str = NULL;
  } else if (op->value_type == OP_VALUE_BITMAP && op->value.bitmap) {
    bitmap_free(op->value.bitmap);
    op->value.bitmap = NULL;
  }
}

void op_set_value_int32(op_t *op, uint32_t val) {
  if (!op) {
    return;
  }

  _clean_up_prev_val(op);

  op->value_type = OP_VALUE_INT32;
  op->value.int32 = val;
}

void op_set_value_str(op_t *op, const char *val) {
  if (!op || !val) {
    return;
  }

  _clean_up_prev_val(op);

  op->value_type = OP_VALUE_STRING;
  op->value.str = strdup(val);
}

op_type_t op_get_type(const op_t *op) {
  return op ? op->op_type : OP_TYPE_NONE;
}

op_value_type_t op_get_value_type(const op_t *op) {
  return op ? op->value_type : OP_VALUE_NONE;
}

cond_put_type_t op_get_condition_type(const op_t *op) {
  return op ? op->cond_type : COND_PUT_NONE;
}

const eng_container_db_key_t *op_get_db_key(const op_t *op) {
  return op ? &op->db_key : NULL;
}

uint32_t op_get_value_int32(const op_t *op) {
  if (!op || op->value_type != OP_VALUE_INT32) {
    return 0;
  }
  return op->value.int32;
}

const char *op_get_value_str(const op_t *op) {
  if (!op || op->value_type != OP_VALUE_STRING) {
    return NULL;
  }
  return op->value.str;
}

op_t *op_create_str_val(const eng_container_db_key_t *db_key, op_type_t op_type,
                        cond_put_type_t cond_type, const char *val) {
  if (!db_key || !val) {
    return NULL;
  }

  op_t *op = op_create(op_type);
  if (!op) {
    return NULL;
  }

  op_set_target(op, db_key);
  op_set_condition(op, cond_type);
  op_set_value_str(op, val);

  return op;
}

op_t *op_create_int32_val(const eng_container_db_key_t *db_key,
                          op_type_t op_type, cond_put_type_t cond_type,
                          uint32_t val) {
  if (!db_key) {
    return NULL;
  }

  op_t *op = op_create(op_type);
  if (!op) {
    return NULL;
  }

  op_set_target(op, db_key);
  op_set_condition(op, cond_type);
  op_set_value_int32(op, val);

  return op;
}
