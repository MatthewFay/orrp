#ifndef CONSUMER_EBR_H
#define CONSUMER_EBR_H

#include "ck_epoch.h"

// Mark cached bitmap for retirement (consumer thread calls this)
void consumer_ebr_retire_bitmap(ck_epoch_entry_t *entry);

#endif