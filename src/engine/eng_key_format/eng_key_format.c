#include "eng_key_format.h"
#include <stdio.h>

bool custom_tag_into(char *out_buf, size_t size, ast_node_t *custom_tag) {
  if (custom_tag == NULL || custom_tag->tag.custom_key == NULL ||
      custom_tag->tag.value == NULL) {
    return false;
  }

  if (custom_tag->tag.value->type == LITERAL_NODE) {
    ast_literal_node_t *literal = &custom_tag->tag.value->literal;
    int r = -1;
    if (literal->type == LITERAL_STRING) {
      r = snprintf(out_buf, size, "%s:%s", custom_tag->tag.custom_key,
                   literal->string_value);
    } else if (literal->type == LITERAL_NUMBER) {
      r = snprintf(out_buf, size, "%s:%d", custom_tag->tag.custom_key,
                   (int)literal->number_value);
    }

    if (r < 0 || (size_t)r >= size) {
      return false;
    }
    return true;
  }

  return false;
}

bool db_key_into(char *buffer, size_t buffer_size,
                 eng_container_db_key_t *db_key) {
  int r = -1;
  int db_type = 0;
  char *container_name;
  if (db_key->dc_type == CONTAINER_TYPE_SYSTEM) {
    db_type = db_key->sys_db_type;
    container_name = SYS_CONTAINER_NAME;
  } else {
    db_type = db_key->user_db_type;
    container_name = db_key->container_name;
  }
  if (db_key->db_key.type == DB_KEY_INTEGER) {
    r = snprintf(buffer, buffer_size, "%s|%d|%u", container_name, (int)db_type,
                 db_key->db_key.key.i);
  } else if (db_key->db_key.type == DB_KEY_STRING) {
    if (db_key->db_key.key.s == NULL) {
      return false;
    }
    r = snprintf(buffer, buffer_size, "%s|%d|%s", container_name, (int)db_type,
                 db_key->db_key.key.s);
  } else {
    return false;
  }
  if (r < 0 || (size_t)r >= buffer_size) {
    return false;
  }
  return true;
}
