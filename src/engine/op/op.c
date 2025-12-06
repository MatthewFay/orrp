#include "op.h"
#include "engine/container/container.h"
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

op_t *op_create(op_type_t op_type, const eng_container_db_key_t *db_key,
                uint32_t value) {
  op_t *op = malloc(sizeof(op_t));
  if (!op) {
    return NULL;
  }

  op->op_type = op_type;
  memcpy(&op->db_key, db_key, sizeof(eng_container_db_key_t));
  op->value = value;

  return op;
}

void op_destroy(op_t *op) {
  if (!op) {
    return;
  }

  container_free_db_key_contents(&op->db_key);
  free(op);
}
