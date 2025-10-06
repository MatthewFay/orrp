#ifndef eng_writer_queue_MSG_H
#define eng_writer_queue_MSG_H

#include "core/bitmaps.h"
#include "engine/container/container.h"
#include <stdint.h>

typedef struct eng_writer_entry_s {
  bitmap_t *bitmap_copy; // TODO: support diff data types
  _Atomic(uint64_t) *flush_version_ptr;
  eng_container_db_key_t db_key;
} eng_writer_entry_t;

typedef struct eng_writer_msg_s {
  eng_writer_entry_t *entries; // pointer to array
  uint32_t count;
} eng_writer_msg_t;

void eng_writer_queue_free_msg(eng_writer_msg_t *msg);

#endif