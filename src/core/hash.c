#include <stddef.h>
#include <stdint.h>

/*
================================================================
 xxH64 - High-performance 64-bit hash function
 By Yann Collet
 Public Domain
================================================================
*/

// --- Constants ---
static const uint64_t PRIME64_1 = 0x9E3779B185EBCA87ULL;
static const uint64_t PRIME64_2 = 0xC2B2AE3D27D4EB4FULL;
static const uint64_t PRIME64_3 = 0x165667B19E3779F9ULL;
static const uint64_t PRIME64_4 = 0x85EBCA77C2b2AE63ULL;
static const uint64_t PRIME64_5 = 0x27D4EB2F165667C5ULL;

// --- Helper Functions ---
static uint64_t xxhash_rotl64(uint64_t x, int r) {
  return (x << r) | (x >> (64 - r));
}

// --- Core Hashing Logic ---
static uint64_t xxhash64_finalize(uint64_t h) {
  h ^= h >> 33;
  h *= PRIME64_2;
  h ^= h >> 29;
  h *= PRIME64_3;
  h ^= h >> 32;
  return h;
}

/**
 * @brief Calculates the 64-bit xxHash for a given block of data.
 *
 * @param input Pointer to the data to be hashed.
 * @param len The length of the data in bytes.
 * @param seed A seed value to initialize the hash.
 * @return The 64-bit hash value.
 */
uint64_t xxhash64(const void *input, size_t len, uint64_t seed) {
  const uint8_t *p = (const uint8_t *)input;
  const uint8_t *const end = p + len;
  uint64_t h64;

  if (len >= 32) {
    const uint8_t *const limit = end - 32;
    uint64_t v1 = seed + PRIME64_1 + PRIME64_2;
    uint64_t v2 = seed + PRIME64_2;
    uint64_t v3 = seed + 0;
    uint64_t v4 = seed - PRIME64_1;

    do {
      v1 += (*(const uint64_t *)p) * PRIME64_2;
      p += 8;
      v1 = xxhash_rotl64(v1, 31);
      v1 *= PRIME64_1;

      v2 += (*(const uint64_t *)p) * PRIME64_2;
      p += 8;
      v2 = xxhash_rotl64(v2, 31);
      v2 *= PRIME64_1;

      v3 += (*(const uint64_t *)p) * PRIME64_2;
      p += 8;
      v3 = xxhash_rotl64(v3, 31);
      v3 *= PRIME64_1;

      v4 += (*(const uint64_t *)p) * PRIME64_2;
      p += 8;
      v4 = xxhash_rotl64(v4, 31);
      v4 *= PRIME64_1;
    } while (p <= limit);

    h64 = xxhash_rotl64(v1, 1) + xxhash_rotl64(v2, 7) + xxhash_rotl64(v3, 12) +
          xxhash_rotl64(v4, 18);

    // Merge round
    h64 ^= xxhash_rotl64(v1 * PRIME64_2, 31) * PRIME64_1;
    h64 = (h64 ^ (h64 >> 27)) * PRIME64_1 + PRIME64_4;

    h64 ^= xxhash_rotl64(v2 * PRIME64_2, 31) * PRIME64_1;
    h64 = (h64 ^ (h64 >> 27)) * PRIME64_1 + PRIME64_4;

    h64 ^= xxhash_rotl64(v3 * PRIME64_2, 31) * PRIME64_1;
    h64 = (h64 ^ (h64 >> 27)) * PRIME64_1 + PRIME64_4;

    h64 ^= xxhash_rotl64(v4 * PRIME64_2, 31) * PRIME64_1;
    h64 = (h64 ^ (h64 >> 27)) * PRIME64_1 + PRIME64_4;

  } else {
    h64 = seed + PRIME64_5;
  }

  h64 += len;

  // Process remaining bytes
  while (p + 8 <= end) {
    uint64_t k1 = *(const uint64_t *)p;
    k1 *= PRIME64_2;
    k1 = xxhash_rotl64(k1, 31);
    k1 *= PRIME64_1;
    h64 ^= k1;
    h64 = xxhash_rotl64(h64, 27) * PRIME64_1 + PRIME64_4;
    p += 8;
  }
  if (p + 4 <= end) {
    h64 ^= (*(const uint32_t *)p) * PRIME64_1;
    h64 = xxhash_rotl64(h64, 23) * PRIME64_2 + PRIME64_3;
    p += 4;
  }
  while (p < end) {
    h64 ^= (*p) * PRIME64_5;
    h64 = xxhash_rotl64(h64, 11) * PRIME64_1;
    p++;
  }

  return xxhash64_finalize(h64);
}
