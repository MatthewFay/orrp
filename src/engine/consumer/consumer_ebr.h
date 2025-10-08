#ifndef CONSUMER_EBR_H
#define CONSUMER_EBR_H

#include "ck_epoch.h"

// Initialize epoch for consumer
void consumer_ebr_init(ck_epoch_t *epoch);

// Register thread for EBR
void consumer_ebr_register(ck_epoch_t *epoch, ck_epoch_record_t *record);

void consumer_ebr_unregister(ck_epoch_record_t *record);

// Mark bitmap for retirement (consumer thread calls this)
void consumer_ebr_retire(ck_epoch_record_t *record, ck_epoch_entry_t *entry);

// Reclaim memory (consumer thread calls this)
void consumer_ebr_reclaim(ck_epoch_record_t *record);

#endif