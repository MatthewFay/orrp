#include "core/ebr.h"
#include "ck_epoch.h"

// The One True Epoch
ck_epoch_t g_data_plane_epoch;

static _Thread_local struct {
  ck_epoch_record_t record CK_CC_CACHELINE;
  bool is_registered;
} t_local_ebr = {0};

ck_epoch_record_t *ebr_get_trecord(void) { return &t_local_ebr.record; }

void ebr_epoch_global_init(void) { ck_epoch_init(&g_data_plane_epoch); }

void ebr_register(void) {
  ck_epoch_register(&g_data_plane_epoch, &t_local_ebr.record, NULL);
}

void ebr_unregister(void) { ck_epoch_unregister(&t_local_ebr.record); }

// Non-blocking cleanup
// Returns true if memory was reclaimed, false if busy
bool ebr_poll_nonblocking(void) {
  // This attempts to advance the epoch AND execute callbacks (free memory)
  return ck_epoch_poll(&t_local_ebr.record);
}

void ebr_full_reclaim_blocking(void) {
  // Wait until all active readers have finished with old data
  ck_epoch_synchronize(&t_local_ebr.record);
  // Now that we know it's safe, actually execute the free() callbacks
  ck_epoch_reclaim(&t_local_ebr.record);
}

void ebr_begin(ck_epoch_section_t *section) {
  if (__builtin_expect(!t_local_ebr.is_registered, 0)) {
    ebr_register();
    t_local_ebr.is_registered = true;
  }
  ck_epoch_begin(&t_local_ebr.record, section);
}

void ebr_end(ck_epoch_section_t *section) {
  ck_epoch_end(&t_local_ebr.record, section);
}

void ebr_call(ck_epoch_entry_t *entry, ck_epoch_cb_t *cb) {
  ck_epoch_call(&t_local_ebr.record, entry, cb);
}