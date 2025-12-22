#include "eng_key_format.h"
#include "core/db.h"
#include <stdint.h>
#include <stdio.h>

bool custom_tag_into(char *out_buf, size_t size, ast_node_t *custom_tag) {
  if (custom_tag == NULL || custom_tag->tag.custom_key == NULL ||
      custom_tag->tag.value == NULL) {
    return false;
  }

  if (custom_tag->tag.value->type == AST_LITERAL_NODE) {
    ast_literal_node_t *literal = &custom_tag->tag.value->literal;
    int r = -1;
    if (literal->type == AST_LITERAL_STRING) {
      r = snprintf(out_buf, size, "%s:%s", custom_tag->tag.custom_key,
                   literal->string_value);
    } else if (literal->type == AST_LITERAL_NUMBER) {
      r = snprintf(out_buf, size, "%s:%lld", custom_tag->tag.custom_key,
                   (long long)literal->number_value);
    }

    if (r < 0 || (size_t)r >= size) {
      return false;
    }
    return true;
  }

  return false;
}

bool tag_str_entity_id_into(char *out_buf, size_t size, const char *custom_tag,
                            uint32_t entity_id) {
  if (!out_buf || !custom_tag) {
    return false;
  }
  int r = snprintf(out_buf, size, "%s|%d", custom_tag, entity_id);
  if (r < 0 || (size_t)r >= size) {
    return false;
  }
  return true;
}

bool tag_entity_id_into(char *out_buf, size_t size, ast_node_t *custom_tag,
                        uint32_t entity_id) {
  if (!out_buf || !custom_tag) {
    return false;
  }
  char cus_tag[512];
  bool tr = custom_tag_into(cus_tag, sizeof(cus_tag), custom_tag);
  if (!tr)
    return false;
  return tag_str_entity_id_into(out_buf, size, cus_tag, entity_id);
}

bool db_key_into(char *buffer, size_t buffer_size,
                 eng_container_db_key_t *db_key) {
  if (!buffer || !db_key)
    return false;
  int r = -1;
  int db_type = 0;
  char *container_name;
  if (db_key->dc_type == CONTAINER_TYPE_SYS) {
    db_type = db_key->sys_db_type;
    container_name = SYS_CONTAINER_NAME;
  } else {
    db_type = db_key->usr_db_type;
    container_name = db_key->container_name;
  }
  if (db_key->db_key.type == DB_KEY_U32) {
    r = snprintf(buffer, buffer_size, "%s|%d|%u", container_name, (int)db_type,
                 db_key->db_key.key.u32);
  } else if (db_key->db_key.type == DB_KEY_I64) {
    r = snprintf(buffer, buffer_size, "%s|%d|%lld", container_name,
                 (int)db_type, (long long)db_key->db_key.key.i64);
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

bool tag_count_into(char *out_buf, size_t size, const char *custom_tag,
                    uint32_t count) {
  if (!out_buf || !custom_tag) {
    return false;
  }
  int r = snprintf(out_buf, size, "%s|%d", custom_tag, count);
  if (r < 0 || (size_t)r >= size) {
    return false;
  }
  return true;
}