#ifndef worker_types_h
#define worker_types_h

// shared worker module types

#include "uthash.h"

typedef struct worker_entity_tag_counter_s {
  UT_hash_handle hh;
  char *tag_entity_id_key; // tag|entity_id
  uint32_t count;
} worker_entity_tag_counter_t;

#endif