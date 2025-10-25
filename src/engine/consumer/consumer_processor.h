#ifndef consumer_processor_h
#define consumer_processor_h

#include "engine/consumer/consumer_batch.h"
#include "engine/consumer/consumer_cache_internal.h"
#include <stdbool.h>
#include <stdlib.h>

typedef enum {
  CONSUMER_PROCESS_SUCCESS,         // All succeeded
  CONSUMER_PROCESS_PARTIAL_FAILURE, // Some failed, some succeeded
  CONSUMER_PROCESS_FAILURE          // All failed (or critical error)
} consumer_process_status_t;

typedef struct {
  consumer_process_status_t status;
  const char *err_msg;
  uint32_t msgs_processed;
  uint32_t msgs_failed;
} consumer_process_result_t;

// Main entry point - processes a single container batch
consumer_process_result_t
consumer_process_container_batch(consumer_cache_t *cache, eng_container_t *dc,
                                 MDB_txn *txn,
                                 ck_epoch_record_t *consumer_epoch_record,
                                 consumer_batch_container_t *batch);

#endif