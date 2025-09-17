#include "cache_ebr.h"
#include "core/bitmaps.h"

_Thread_local ck_epoch_record_t *bitmap_cache_thread_epoch_record = NULL;

// Global epoch for entire cache.
ck_epoch_t bitmap_cache_g_epoch;

// =============================================================================
// --- Epoch Reclamation Callback ---
// =============================================================================

CK_EPOCH_CONTAINER(bitmap_t, epoch_entry, get_bitmap_from_epoch)

void bm_cache_dispose(ck_epoch_entry_t *entry) {
  bitmap_t *bm = get_bitmap_from_epoch(entry);
  bitmap_free(bm);
}
