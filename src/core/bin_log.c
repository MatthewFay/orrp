#include "core/bin_log.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h> // for fsync

static uint32_t _crc32(const void *data, size_t n_bytes) {
  uint32_t crc = 0xFFFFFFFF;
  const uint8_t *p = data;
  for (size_t i = 0; i < n_bytes; i++) {
    crc ^= p[i];
    for (int j = 0; j < 8; j++) {
      crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
    }
  }
  return ~crc;
}

bool bin_log_init(bin_log_t *log, const char *path) {
  if (!log || !path)
    return false;

  // Safety check for path length
  if (strlen(path) >= sizeof(log->path))
    return false;
  strcpy(log->path, path);

  if (uv_rwlock_init(&log->lock) != 0)
    return false;

  // OPEN ONCE: Append Binary mode.
  // Creates file if missing. Positions stream at end.
  log->handle = fopen(path, "ab");
  if (!log->handle) {
    uv_rwlock_destroy(&log->lock);
    return false;
  }

  log->initialized = true;
  return true;
}

void bin_log_close(bin_log_t *log) {
  if (log && log->initialized) {
    // Acquire write lock to ensure no one is writing/reading while we close
    uv_rwlock_wrlock(&log->lock);

    if (log->handle) {
      fclose(log->handle);
      log->handle = NULL;
    }

    uv_rwlock_wrunlock(&log->lock);
    uv_rwlock_destroy(&log->lock);
    log->initialized = false;
  }
}

bool bin_log_append(bin_log_t *log, const void *data, uint32_t len) {
  if (!log || !log->initialized || !data)
    return false;

  // Compute checksum outside lock (CPU work)
  uint32_t crc = _crc32(data, len);

  uv_rwlock_wrlock(&log->lock);

  bool success = false;

  uint32_t header[2] = {len, crc};
  if (fwrite(header, sizeof(uint32_t), 2, log->handle) != 2)
    goto cleanup;

  if (fwrite(data, 1, len, log->handle) != len)
    goto cleanup;

  // Durability
  // fflush flushes C stdlib buffer -> Kernel buffer
  if (fflush(log->handle) != 0)
    goto cleanup;

  // fsync flushes Kernel buffer -> Physical Disk
  if (fsync(fileno(log->handle)) != 0)
    goto cleanup;

  success = true;

cleanup:
  uv_rwlock_wrunlock(&log->lock);
  return success;
}

bin_log_scan_result_t bin_log_scan(bin_log_t *log, bin_log_cb cb, void *arg) {
  if (!log || !log->initialized || !cb)
    return BL_SCAN_ERR_INVALID;

  uv_rwlock_rdlock(&log->lock);

  // We open a separate read handle so we don't mess up the append position
  // of the main write handle.
  FILE *f = fopen(log->path, "rb");
  if (!f) {
    uv_rwlock_rdunlock(&log->lock);
    // If file is missing, it just means empty log. Not an error.
    return BL_SCAN_NO_LOG;
  }

  uint32_t header[2]; // [len, crc]
  void *buffer = NULL;
  size_t buf_cap = 0;
  bool keep_going = true;
  bin_log_scan_result_t result = BL_SCAN_OK;

  while (keep_going && fread(header, sizeof(uint32_t), 2, f) == 2) {
    uint32_t len = header[0];
    uint32_t stored_crc = header[1];

    if (len > buf_cap) {
      void *new_buf = realloc(buffer, len);
      if (!new_buf) {
        // OOM: Stop scanning
        keep_going = false;
        result = BL_SCAN_ERR_OOM;
        break;
      }
      buffer = new_buf;
      buf_cap = len;
    }

    if (fread(buffer, 1, len, f) != len) {
      // EOF mid-record. This is a "Torn Write" (crash during write).
      // We safely ignore this partial record and stop.
      result = BL_SCAN_TORN;
      break;
    }

    if (_crc32(buffer, len) != stored_crc) {
      // Data Corruption. Stop immediately.
      result = BL_SCAN_ERR_CRC;
      break;
    }

    bin_log_cb_result_t cb_r = cb(buffer, len, arg);

    switch (cb_r) {
    case BL_CB_CONTINUE:
      keep_going = true;
      break;
    case BL_CB_STOP:
      keep_going = false;
      result = BL_SCAN_STOPPED;
      break;
    case BL_CB_ERR:
      keep_going = false;
      result = BL_SCAN_CB_ERR;
      break;
    default:
      break;
    }
  }

  if (buffer)
    free(buffer);
  fclose(f);
  uv_rwlock_rdunlock(&log->lock);
  return result;
}