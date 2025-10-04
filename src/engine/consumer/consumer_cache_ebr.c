#include "consumer_cache_ebr.h"
#include "consumer_cache/consumer_cache.h"
#include "core/bitmaps.h"

CK_EPOCH_CONTAINER(bitmap_t, epoch_entry, get_bitmap_from_epoch)

// =============================================================================
// --- Epoch Reclamation Callback ---
// =============================================================================
static void _consumer_cache_dispose(ck_epoch_entry_t *entry) {
  bitmap_t *bm = get_bitmap_from_epoch(entry);
  bitmap_free(bm);
}

void consumer_cache_ebr_reg(consumer_cache_t *consumer_cache,
                            ck_epoch_record_t *record) {
  ck_epoch_register(&consumer_cache->epoch, record, NULL);
}

void consumer_cache_ebr_retire(ck_epoch_record_t *record,
                               ck_epoch_entry_t *epoch_entry) {
  ck_epoch_call(record, epoch_entry, _consumer_cache_dispose);
}

void consumer_cache_reclamation(ck_epoch_record_t *record) {
  ck_epoch_synchronize(record);
  ck_epoch_reclaim(record);
}