#ifndef BITMAP_CACHE_EBR_H
#define BITMAP_CACHE_EBR_H

#include "ck_epoch.h"

// The THREAD-LOCAL epoch record pointer.
// Each thread gets its own "ID badge".
extern _Thread_local ck_epoch_record_t *bitmap_cache_thread_epoch_record;

// Global epoch for entire cache.
extern ck_epoch_t bitmap_cache_g_epoch;

void bm_cache_dispose(ck_epoch_entry_t *entry);

#endif