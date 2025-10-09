#include "op.h"
#include "core/db.h"
#include <stdlib.h>
#include <string.h>

op_t *op_create_str_val(eng_container_db_key_t *db_key, op_type op_type,
                        cond_put_type cond_type, const char *val) {
  if (!db_key || !val)
    return NULL;
  op_t *op = calloc(1, sizeof(op_t));
  if (!op)
    return NULL;
  op->op_type = op_type;
  op->data_type = OP_STR_DATA;
  op->cond_type = cond_type;
  op->db_key = *db_key;
  op->db_key.container_name = strdup(db_key->container_name);
  if (db_key->db_key.type == DB_KEY_STRING) {
    op->db_key.db_key.key.s = strdup(db_key->db_key.key.s);
  }
  op->data.str_value = strdup(val);
  return op;
}

op_t *op_create_int32_val(eng_container_db_key_t *db_key, op_type op_type,
                          cond_put_type cond_type, uint32_t val) {
  if (!db_key)
    return NULL;
  op_t *op = calloc(1, sizeof(op_t));
  if (!op)
    return NULL;
  op->op_type = op_type;
  op->data_type = OP_INT32_DATA;
  op->cond_type = cond_type;
  op->db_key = *db_key;
  op->db_key.container_name = strdup(db_key->container_name);
  if (db_key->db_key.type == DB_KEY_STRING) {
    op->db_key.db_key.key.s = strdup(db_key->db_key.key.s);
  }
  op->data.int32_value = val;
  return op;
}

op_t *op_create_count_tag_increment(eng_container_db_key_t *db_key,
                                    const char *tag, uint32_t entity_id,
                                    uint32_t increment) {
  if (!db_key || !tag)
    return NULL;
  op_t *op = calloc(1, sizeof(op_t));
  if (!op)
    return NULL;
  op_count_tag_data_t *data = calloc(1, sizeof(op_count_tag_data_t));
  if (!data) {
    free(op);
    return NULL;
  }
  op->op_type = COUNT_TAG_INCREMENT;
  op->data_type = OP_COUNT_TAG_DATA;
  op->cond_type = COND_PUT_NONE;
  op->db_key = *db_key;
  op->db_key.container_name = strdup(db_key->container_name);
  if (db_key->db_key.type == DB_KEY_STRING) {
    op->db_key.db_key.key.s = strdup(db_key->db_key.key.s);
  }
  data->tag = strdup(tag);
  data->entity_id = entity_id;
  data->increment = increment;
  op->data.count_tag_data = data;
  return op;
}

void op_destroy(op_t *op) {
  if (!op)
    return;
  eng_Container_free_contents_db_key(&op->db_key);
  switch (op->data_type) {
  case OP_STR_DATA:
    free(op->data.str_value);
    break;
  case OP_COUNT_TAG_DATA:
    free(op->data.count_tag_data->tag);
    free(op->data.count_tag_data);
    break;
  default:
    break;
  }
  free(op);
}
