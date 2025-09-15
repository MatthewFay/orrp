#include "engine/bitmap_cache/cache_entry.h"

typedef enum { BITMAP_DIRTY_LIST } flush_msg_data_type;

typedef struct flush_msg_s {
  flush_msg_data_type data_type;
  union {
    bm_cache_entry_t *bm_cache_dirty_head;
  } data;
} flush_msg_t;

flush_msg_t *flush_msg_create(flush_msg_data_type data_type, void *data);
void flush_msg_free(flush_msg_t *flush_msg);
