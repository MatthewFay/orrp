#include "cache_shard.h"
#include "ck_ring.h"

#define HT_SEED 0

static void *_hs_malloc(size_t r) { return malloc(r); }
static void _hs_free(void *p, size_t b, bool r) {
  (void)b;
  (void)r;
  free(p);
}
static struct ck_malloc _my_allocator = {.malloc = _hs_malloc,
                                         .free = _hs_free};

bool bm_init_shard(bm_cache_shard_t *shard) {
  uv_mutex_init(&shard->dirty_list_lock);

  shard->lru_head = shard->lru_tail = NULL;
  shard->dirty_head = NULL;
  shard->n_entries = 0;
  ck_ring_init(&shard->ring, CAPACITY_PER_SHARD);
  if (!ck_ht_init(&shard->table, CK_HT_MODE_BYTESTRING,
                  NULL, // Custom hash function
                  &_my_allocator, CAPACITY_PER_SHARD, HT_SEED)) {
    return false;
  }
  return true;
}

bool shard_enqueue_msg(bm_cache_shard_t *shard, bm_cache_queue_msg_t *msg) {
  if (!shard || !msg)
    return false;
  // MPSC: Multiple producer, single consumer
  return ck_ring_enqueue_mpsc(&shard->ring, shard->ring_buffer, msg);
}