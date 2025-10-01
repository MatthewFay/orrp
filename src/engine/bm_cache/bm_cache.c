#include "bitmap_cache.h"
#include "cache_ebr.h"
#include "cache_queue_consumer.h"
#include "cache_queue_msg.h"
#include "cache_shard.h"
#include "ck_epoch.h"
#include "ck_pr.h"
#include "core/db.h"
#include "core/hash.h"
#include "engine/engine_writer/engine_writer.h"
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NUM_SHARDS 16 // Power of 2 for fast modulo
#define SHARD_MASK (NUM_SHARDS - 1)
#define NUM_CONSUMER_THREADS 4
#define SHARDS_PER_CONSUMER (NUM_SHARDS / NUM_CONSUMER_THREADS)

#define MAX_CACHE_KEY_SIZE 256
#define MAX_ENQUEUE_ATTEMPTS 3

typedef struct bitmap_cache_handle_s {
  ck_epoch_section_t epoch_section;
} bitmap_cache_handle_t;

typedef struct bm_cache_s {
  bm_cache_shard_t shards[NUM_SHARDS];
  bm_cache_consumer_t consumers[NUM_CONSUMER_THREADS];
  bool is_initialized;
  eng_writer_t *writer;
} bm_cache_t;

// Global instance of our cache
static bm_cache_t g_bm_cache;

// Get shard index from key
static int _get_shard_index(const char *key) {
  return xxhash64(key, strlen(key), 0) & SHARD_MASK;
}

// Get unique cache key for entry
static bool _get_cache_key(char *buffer, size_t buffer_size,
                           const char *container_name,
                           eng_user_dc_db_type_t db_type,
                           const db_key_t db_key) {
  int r = -1;
  if (db_key.type == DB_KEY_INTEGER) {
    r = snprintf(buffer, buffer_size, "%s:%d:%u", container_name, (int)db_type,
                 db_key.key.i);
  } else if (db_key.type == DB_KEY_STRING) {
    r = snprintf(buffer, buffer_size, "%s:%d:%s", container_name, (int)db_type,
                 db_key.key.s);
  } else {
    return false;
  }
  if (r < 0 || (size_t)r >= buffer_size) {
    return false;
  }
  return true;
}

static bool _enqueue_msg(const char *cache_key, bm_cache_queue_msg_t *msg) {
  int s_idx = _get_shard_index(cache_key);
  bool enqueued = false;
  for (int i = 0; i < MAX_ENQUEUE_ATTEMPTS; i++) {
    if (shard_enqueue_msg(&g_bm_cache.shards[s_idx], msg)) {
      enqueued = true;
      break;
    }
    // Ring buffer is full
    ck_pr_stall();
    // might add a short sleep here
  }
  return enqueued;
}

// --- Public API Implementations ---

bool bitmap_cache_init(eng_writer_t *writer) {
  if (!writer)
    return false;

  for (int i = 0; i < NUM_SHARDS; ++i) {
    if (!bm_init_shard(&g_bm_cache.shards[i])) {
      return false;
    }
  }

  g_bm_cache.writer = writer;

  for (int i = 0; i < NUM_CONSUMER_THREADS; i++) {
    bm_cache_consumer_config_t config = {.shards = g_bm_cache.shards,
                                         .writer = g_bm_cache.writer,
                                         .flush_every_n = 100,
                                         .shard_start = i * SHARDS_PER_CONSUMER,
                                         .shard_count = SHARDS_PER_CONSUMER,
                                         .consumer_id = i};

    if (!bm_cache_consumer_start(&g_bm_cache.consumers[i], &config)) {
      return false;
    }
  }

  g_bm_cache.is_initialized = true;
  return true;
}

bool bitmap_cache_ingest(const bitmap_cache_key_t *key, uint32_t value,
                         const char *idempotency_key) {
  char cache_key[MAX_CACHE_KEY_SIZE];
  (void)idempotency_key;
  if (!key || !key->container_name)
    return false;
  if (!_get_cache_key(cache_key, sizeof(cache_key), key->container_name,
                      key->db_type, key->db_key)) {
    return false;
  }

  bm_cache_queue_msg_t *msg =
      bm_cache_create_msg(BM_CACHE_ADD_VALUE, key, value, cache_key);
  if (!msg) {
    return false;
  }

  if (!_enqueue_msg(cache_key, msg)) {
    bm_cache_free_msg(msg);
    return false;
  }
  // notify queue consumer using libuv?
  return true;
}

bitmap_cache_handle_t *bitmap_cache_query_begin(void) {
  bitmap_cache_handle_t *h = malloc(sizeof(bitmap_cache_handle_t));
  if (!h)
    return NULL;
  bm_cache_ebr_reg();
  ck_epoch_begin(&bitmap_cache_thread_epoch_record, &h->epoch_section);
  return h;
}

void bitmap_cache_query_end(bitmap_cache_handle_t *handle) {
  if (!handle)
    return;
  ck_epoch_end(&bitmap_cache_thread_epoch_record, &handle->epoch_section);
  free(handle);
}

// TODO: This is tricky - need to flush in-flight and in-memory data to disk
bool bitmap_cache_shutdown(void) {
  bool success = true;
  for (int i = 0; i < NUM_CONSUMER_THREADS; i++) {
    if (!bm_cache_consumer_stop(&g_bm_cache.consumers[i])) {
      success = false;
    }
  }

  return success;
}
