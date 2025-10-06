#ifndef ENGINE_WRITER_H
#define ENGINE_WRITER_H

/**
 * Engine Writer -
 * Flushes dirty data to disk
 */
#include "engine/engine_writer/engine_writer_queue.h"
#include "uv.h"
#include <stdint.h>

#define FLUSH_QUEUE_CAPACITY 32768

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

  eng_writer_queue_t queue;
} eng_writer_t;

bool eng_writer_start(eng_writer_t *writer, const eng_writer_config_t *config);
bool eng_writer_stop(eng_writer_t *writer);
bool eng_writer_force_flush(eng_writer_t *writer);

#endif