#ifndef ENGINE_WRITER_H
#define ENGINE_WRITER_H

/**
 * Engine Writer -
 * Flushes dirty data to disk and performs memory reclamation.
 */
#include "ck_ring.h"
#include "engine/bitmap_cache/cache_entry.h"
#include "uv.h"
#include <stdint.h>

#define FLUSH_QUEUE_CAPACITY 32768

typedef struct eng_writer_config_s {
  // Engine writer config
  uint32_t flush_interval_ms;

  // Epoch reclamation config
  uint32_t reclaim_every; // Run reclamation after every N flush cycles
} eng_writer_config_t;

typedef struct eng_writer_s {
  eng_writer_config_t config;
  uv_thread_t thread;
  uv_timer_t timer;

  volatile bool should_stop;

  // Stats
  uint64_t entries_written;
  uint64_t reclaim_cycles;
  uint64_t objects_reclaimed;

  ck_ring_t ring;
  ck_ring_buffer_t ring_buffer[FLUSH_QUEUE_CAPACITY];
} eng_writer_t;

bool eng_writer_start(eng_writer_t *writer, const eng_writer_config_t *config);
bool eng_writer_stop(eng_writer_t *writer);
bool eng_writer_force_flush(eng_writer_t *writer);

bool eng_writer_queue_up_bm_dirty_list(eng_writer_t *writer,
                                       bm_cache_entry_t *dirty_head);
#endif