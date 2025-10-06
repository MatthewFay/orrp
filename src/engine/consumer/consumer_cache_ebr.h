#ifndef CONSUMER_CACHE_EBR_H
#define CONSUMER_CACHE_EBR_H

#include "ck_epoch.h"
#include "consumer_cache_internal.h"

// Register thread for EBR
void consumer_cache_ebr_reg(consumer_cache_t *consumer_cache,
                            ck_epoch_record_t *record);

// Mark object as retired
void consumer_cache_ebr_retire(ck_epoch_record_t *record,
                               ck_epoch_entry_t *epoch_entry);

// Free memory
void consumer_cache_reclamation(ck_epoch_record_t *record);

#endif