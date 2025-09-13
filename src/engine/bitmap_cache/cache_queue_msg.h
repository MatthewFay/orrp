#ifndef BM_CACHE_Q_MSG_H
#define BM_CACHE_Q_MSG_H

#include "core/db.h"
#include "engine/bitmap_cache/bitmap_cache.h"
#include "engine/container.h"
#include <stdint.h>
typedef enum {
  BM_CACHE_ADD_VALUE, // Add value to bitmap
  BM_CACHE_BITMAP     // Cache bitmap
} bm_cache_queue_op_type;

typedef struct bm_cache_queue_msg_s {
  bm_cache_queue_op_type op_type;
  char *container_name;
  eng_user_dc_db_type_t db_type;
  db_key_t db_key;
  uint32_t value;

  char key[]; // Flexible array member
} bm_cache_queue_msg_t;

bm_cache_queue_msg_t *
bm_cache_create_msg(bm_cache_queue_op_type op_type,
                    const bitmap_cache_key_t *bm_cache_key, uint32_t value,
                    const char *cache_key);

void bm_cache_free_msg_contents(bm_cache_queue_msg_t *msg);
void bm_cache_free_msg(bm_cache_queue_msg_t *msg);

#endif