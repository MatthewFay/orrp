#include "engine/bitmap_cache/cache_shard.h"

typedef enum { BITMAP_DIRTY_SNAPSHOT } flush_msg_data_type;

typedef struct flush_msg_s {
  flush_msg_data_type data_type;
  union {
    bm_cache_dirty_snapshot_t *bm_cache_dirty_snapshot;
  } data;
} flush_msg_t;

flush_msg_t *flush_msg_create(flush_msg_data_type data_type, void *data);
void flush_msg_free(flush_msg_t *flush_msg);
