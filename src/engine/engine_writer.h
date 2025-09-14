#ifndef ENGINE_WRITER_H
#define ENGINE_WRITER_H

#include "uv.h"
#include <stdint.h>

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
} eng_writer_t;

bool eng_writer_start(eng_writer_t *worker, const eng_writer_config_t *config);
bool eng_writer_stop(eng_writer_t *worker);
bool eng_writer_force_flush(eng_writer_t *worker);
#endif