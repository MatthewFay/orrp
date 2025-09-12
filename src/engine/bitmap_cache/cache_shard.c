#include "cache_shard.h"
#include "ck_ht.h"
#include "ck_ring.h"
#include "engine/bitmap_cache/cache_entry.h"
#include <stdbool.h>

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
                  NULL, // Initialize with Murmur64 (default)
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

bool shard_get_entry(bm_cache_shard_t *shard, const char *cache_key,
                     bm_cache_value_entry_t **entry_out) {
  ck_ht_hash_t hash;
  ck_ht_entry_t entry;

  if (!cache_key)
    return false;
  unsigned long key_len = strlen(cache_key);
  ck_ht_hash(&hash, &shard->table, cache_key, key_len);
  ck_ht_entry_set(&entry, hash, cache_key, key_len, NULL);
  if (!ck_ht_get_spmc(&shard->table, hash, &entry)) {
    return false;
  }
  *entry_out = (bm_cache_value_entry_t *)entry.value;
  return true;
}