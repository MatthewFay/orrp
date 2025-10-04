#ifndef ENGINE_WRITER_H
#define ENGINE_WRITER_H

/**
 * Engine Writer -
 * Flushes dirty data to disk
 */
#include "ck_ring.h"
#include "core/bitmaps.h"
#include "engine/container/container.h"
#include "uv.h"
#include <stdint.h>

#define FLUSH_QUEUE_CAPACITY 32768

// Copy of data to write
typedef struct {
  bitmap_t *bitmap; // TODO: support diff data types
  _Atomic(uint64_t) *flush_version_ptr;
  eng_container_db_key_t *db_key;
} eng_writer_data_t;

typedef struct eng_writer_config_s {
  // Engine writer config
  uint32_t flush_interval_ms;
} eng_writer_config_t;

typedef struct eng_writer_s {
  eng_writer_config_t config;
  uv_thread_t thread;
  uv_timer_t timer;

  volatile bool should_stop;

  // Stats
  uint64_t entries_written;

  ck_ring_t ring;
  ck_ring_buffer_t ring_buffer[FLUSH_QUEUE_CAPACITY];
} eng_writer_t;

bool eng_writer_start(eng_writer_t *writer, const eng_writer_config_t *config);
bool eng_writer_stop(eng_writer_t *writer);
bool eng_writer_force_flush(eng_writer_t *writer);

bool eng_writer_queue_up_bm_dirty_snapshot(
    eng_writer_t *writer, bm_cache_dirty_snapshot_t *dirty_snapshot);
#endif