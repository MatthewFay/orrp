#ifndef BM_CACHE_KEY_H
#define BM_CACHE_KEY_H

#include "core/db.h"
#include "engine/container.h"
typedef struct bitmap_cache_key_s {
  const char *container_name;
  eng_user_dc_db_type_t db_type;
  db_key_t db_key;
} bitmap_cache_key_t;

#endif