#ifndef CORE_BIN_LOG_H
#define CORE_BIN_LOG_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <uv.h>

/**
 * Binary Log Handle
 * A generic, append-only, thread-safe, durable file storage.
 * Format per record: [Length (4B)] [CRC32 (4B)] [Data (N bytes)]
 */
typedef struct {
  // --- HOT FIELDS (Accessed frequently) ---
  uv_rwlock_t lock;
  FILE *handle; // Kept open for performance
  bool initialized;

  // --- COLD FIELDS (Accessed rarely) ---
  char path[1024];
} bin_log_t;

typedef enum { BL_CB_CONTINUE, BL_CB_STOP, BL_CB_ERR } bin_log_cb_result_t;

typedef enum {
  BL_SCAN_OK = 0,           // Success: Reached end of valid log
  BL_SCAN_STOPPED = 1,      // Stopped: Callback stopped
  BL_SCAN_NO_LOG = 2,       // No log file to open
  BL_SCAN_TORN = 3,         // Detected torn write
  BL_SCAN_ERR_INVALID = -1, // Error: Invalid arguments
  BL_SCAN_ERR_OOM = -2,     // Error: Out of memory
  BL_SCAN_ERR_CRC = -3,     // Error: Data corruption detected
  BL_SCAN_CB_ERR = -4       // Error: callback error
} bin_log_scan_result_t;

/**
 * Callback for iteration.
 * @param data Pointer to the record data
 * @param len Size of the data
 * @param arg User-provided argument
 * @return bin_log_cb_result_t (Continue/Stop/Err)
 */
typedef bin_log_cb_result_t (*bin_log_cb)(void *data, uint32_t len, void *arg);

/**
 * Initialize the log. Opens the file immediately.
 * @return true on success.
 */
bool bin_log_init(bin_log_t *log, const char *path);

/**
 * Close file handle and destroy locks.
 */
void bin_log_close(bin_log_t *log);

/**
 * Append a record safely.
 * - Thread-safe (Writers block Writers/Readers)
 * - Calculates CRC32
 * - Performs fsync() for durability
 */
bool bin_log_append(bin_log_t *log, const void *data, uint32_t len);

/**
 * Iterate through all valid records.
 * - Thread-safe (Readers block Writers)
 * - Verifies CRC32
 * - Automatically stops at end-of-file or first corrupted record (torn write)
 */
bin_log_scan_result_t bin_log_scan(bin_log_t *log, bin_log_cb cb, void *arg);

#endif // CORE_BIN_LOG_H