#include "consumer_cache.h"
#include "ck_epoch.h"
#include "consumer_cache_ebr.h"
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// --- Public API Implementations ---

void consumer_cache_reg(consumer_cache_t *consumer_cache,
                        ck_epoch_record_t *thread_record) {
  consumer_cache_ebr_reg(consumer_cache, thread_record);
}

void consumer_cache_query_begin(ck_epoch_record_t *thread_record,
                                consumer_cache_handle_t *handle) {
  ck_epoch_begin(thread_record, &handle->epoch_section);
}

void consumer_cache_query_end(ck_epoch_record_t *thread_record,
                              consumer_cache_handle_t *handle) {
  ck_epoch_end(thread_record, &handle->epoch_section);
}
