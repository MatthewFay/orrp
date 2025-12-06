#ifndef eng_writer_queue_MSG_H
#define eng_writer_queue_MSG_H

#include "engine/container/container_types.h"
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

typedef enum {
  WRITE_COND_ALWAYS = 0,        // Standard 'put'. Overwrites whatever is there.
  WRITE_COND_NO_OVERWRITE,      // 'put' only if key does not exist
  WRITE_COND_INT32_GREATER_THAN // 'put' only if new_val > existing_val
                                // (Monotonic)
} write_condition_t;

typedef struct eng_writer_entry_s {
  // Writer takes ownership
  void *value;
  size_t value_size;

  // Owned by caller
  _Atomic(uint64_t) *flush_version_ptr;
  uint64_t version;
  bool bump_flush_version; // If true, bump flush version to `version`

  eng_container_db_key_t db_key;
  write_condition_t write_condition;
} eng_writer_entry_t;

typedef struct eng_writer_msg_s {
  eng_writer_entry_t *entries; // pointer to array
  uint32_t count;
} eng_writer_msg_t;

void eng_writer_queue_free_msg_entry(eng_writer_entry_t *e);
void eng_writer_queue_free_msg(eng_writer_msg_t *msg);

#endif