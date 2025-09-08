#ifndef MAINTENANCE_WORKER_H
#define MAINTENANCE_WORKER_H

#include "uv.h"
#include <stdint.h>

typedef struct maintenance_config_s {
  // Engine writer config
  uint32_t flush_interval_ms;
  uint32_t flush_batch_size;
  uint64_t flush_cycles;

  // Epoch reclamation config
  uint32_t reclaim_every; // Run reclamation after every N flush cycles
} maintenance_config_t;

typedef struct maintenance_worker_s {
  maintenance_config_t config;
  uv_thread_t thread;
  uv_timer_t timer;

  volatile bool should_stop;

  // Stats
  uint64_t entries_written;
  uint64_t reclaim_cycles;
  uint64_t objects_reclaimed;
} maintenance_worker_t;

int maintenance_worker_start(maintenance_worker_t *worker,
                             const maintenance_config_t *config);
int maintenance_worker_stop(maintenance_worker_t *worker);
int maintenance_worker_force_flush(maintenance_worker_t *worker);

// engine_writer_config_t writer_config = {
//         .cache = cache,
//         .db_path = "data.lmdb",
//         .flush_interval_ms = 1000,
//         .batch_size = 1000
//     };

//     return engine_writer_start(&cache->writer, &writer_config);

#endif