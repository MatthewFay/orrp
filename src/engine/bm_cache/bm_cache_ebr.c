#include "cache_ebr.h"
#include "core/bitmaps.h"

_Thread_local ck_epoch_record_t bitmap_cache_thread_epoch_record;
_Thread_local bool bitmap_cache_thread_registered = false;
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

void bm_cache_ebr_reg() {
  if (!bitmap_cache_thread_registered) {
    ck_epoch_register(&bitmap_cache_g_epoch, &bitmap_cache_thread_epoch_record,
                      NULL);
  }
}

void bm_cache_ebr_retire(ck_epoch_entry_t *epoch_entry) {
  ck_epoch_call(&bitmap_cache_thread_epoch_record, epoch_entry,
                bm_cache_dispose);
}

void bm_cache_reclamation() {
  ck_epoch_synchronize(&bitmap_cache_thread_epoch_record);
  ck_epoch_reclaim(&bitmap_cache_thread_epoch_record);
}