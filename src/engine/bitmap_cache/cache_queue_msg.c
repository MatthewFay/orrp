#include "cache_queue_msg.h"
#include "core/db.h"

bm_cache_queue_msg_t *
bm_cache_create_msg(bm_cache_queue_op_type op_type,
                    const bitmap_cache_key_t *bm_cache_key, uint32_t value,
                    const char *cache_key) {
  size_t key_len = strlen(cache_key) + 1;

  bm_cache_queue_msg_t *msg = malloc(sizeof(bm_cache_queue_msg_t) + key_len);
  if (snprintf(msg->key, key_len, "%s", cache_key) < 0) {
    free(msg);
    return NULL;
  }
  msg->container_name = strdup(bm_cache_key->container_name);
  msg->db_type = bm_cache_key->db_type;
  msg->db_key.type = bm_cache_key->db_key->type;
  if (bm_cache_key->db_key->type == DB_KEY_STRING) {
    msg->db_key.key.s = strdup(bm_cache_key->db_key->key.s);
  } else {
    msg->db_key.key.i = bm_cache_key->db_key->key.i;
  }
  msg->op_type = op_type;
  msg->value = value;

  return msg;
}

void bm_cache_free_msg(bm_cache_queue_msg_t *msg) {
  if (!msg)
    return;
  if (msg->db_key.type == DB_KEY_STRING) {
    free((char *)msg->db_key.key.s);
  }
  free((char *)msg->container_name);
  free(msg);
}