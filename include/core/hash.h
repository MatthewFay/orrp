#ifndef HASH_H
#define HASH_H

#include <stddef.h>
#include <stdint.h>

uint64_t xxhash64(const void *input, size_t len, uint64_t seed);

#endif