#ifndef EBR_H
#define EBR_H
#include <ck_epoch.h>

void ebr_epoch_global_init(void);

void ebr_register(void);

void ebr_unregister(void);

// Non-blocking cleanup
// Returns true if memory was reclaimed, false if busy
bool ebr_poll_nonblocking(void);

void ebr_full_reclaim_blocking(void);

void ebr_begin(ck_epoch_section_t *section);

void ebr_end(ck_epoch_section_t *section);

void ebr_call(ck_epoch_entry_t *entry, ck_epoch_cb_t *cb);

ck_epoch_record_t *ebr_get_trecord(void);
#endif