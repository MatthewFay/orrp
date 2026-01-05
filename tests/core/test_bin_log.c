#include "core/bin_log.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h> // for unlink and access

#define TEST_LOG_PATH "./test_wal.bin"

static bin_log_t log_handle;

// --- Helper Context for Scan Callbacks ---
typedef struct {
  int count;
  char items[10][256]; // Simple storage for up to 10 strings
  int stop_at_index;   // Return BL_CB_STOP at this count (-1 to disable)
  int error_at_index;  // Return BL_CB_ERR at this count (-1 to disable)
} scan_ctx_t;

// --- Setup & Teardown ---

void setUp(void) {
  // Ensure we start with a clean slate
  unlink(TEST_LOG_PATH);
  memset(&log_handle, 0, sizeof(bin_log_t));
}

void tearDown(void) {
  // Ensure handle is closed and file removed
  bin_log_close(&log_handle);
  unlink(TEST_LOG_PATH);
}

// --- Helper Callback ---
bin_log_cb_result_t scan_test_cb(void *data, uint32_t len, void *arg) {
  scan_ctx_t *ctx = (scan_ctx_t *)arg;

  // Check for forced error injection
  if (ctx->error_at_index != -1 && ctx->count == ctx->error_at_index) {
    return BL_CB_ERR;
  }

  // Check for forced stop
  if (ctx->stop_at_index != -1 && ctx->count == ctx->stop_at_index) {
    return BL_CB_STOP;
  }

  if (ctx->count >= 10)
    return BL_CB_STOP; // Safety cap

  // Copy data for verification
  if (len < 256) {
    memcpy(ctx->items[ctx->count], data, len);
    ctx->items[ctx->count][len] = '\0';
  }

  ctx->count++;
  return BL_CB_CONTINUE;
}

// --- Tests ---

void test_Init_ShouldCreateFile_WhenPathValid(void) {
  bool result = bin_log_init(&log_handle, TEST_LOG_PATH);
  TEST_ASSERT_TRUE(result);

  // Verify file exists on disk
  TEST_ASSERT_EQUAL(0, access(TEST_LOG_PATH, F_OK));
}

void test_Init_ShouldFail_WhenPathIsNull(void) {
  bool result = bin_log_init(&log_handle, NULL);
  TEST_ASSERT_FALSE(result);
}

void test_AppendAndScan_HappyPath(void) {
  bin_log_init(&log_handle, TEST_LOG_PATH);

  const char *msg1 = "EntryOne";
  const char *msg2 = "EntryTwo";

  TEST_ASSERT_TRUE(bin_log_append(&log_handle, msg1, strlen(msg1)));
  TEST_ASSERT_TRUE(bin_log_append(&log_handle, msg2, strlen(msg2)));

  // Verify via Scan
  scan_ctx_t ctx = {0};
  ctx.stop_at_index = -1;
  ctx.error_at_index = -1;

  bin_log_scan_result_t scan_res =
      bin_log_scan(&log_handle, scan_test_cb, &ctx);

  TEST_ASSERT_EQUAL_INT(BL_SCAN_OK, scan_res);
  TEST_ASSERT_EQUAL_INT(2, ctx.count);
  TEST_ASSERT_EQUAL_STRING("EntryOne", ctx.items[0]);
  TEST_ASSERT_EQUAL_STRING("EntryTwo", ctx.items[1]);
}

void test_Scan_ShouldReturnNoLog_IfFileDeleted(void) {
  // 1. Initialize (creates file)
  bin_log_init(&log_handle, TEST_LOG_PATH);

  // 2. Delete file externally
  unlink(TEST_LOG_PATH);

  // 3. Scan
  scan_ctx_t ctx = {0};
  bin_log_scan_result_t res = bin_log_scan(&log_handle, scan_test_cb, &ctx);

  TEST_ASSERT_EQUAL_INT(BL_SCAN_NO_LOG, res);
}

void test_Scan_ShouldStop_WhenCallbackRequestsStop(void) {
  bin_log_init(&log_handle, TEST_LOG_PATH);
  bin_log_append(&log_handle, "1", 1);
  bin_log_append(&log_handle, "2", 1);
  bin_log_append(&log_handle, "3", 1);

  scan_ctx_t ctx = {0};
  ctx.stop_at_index =
      1; // Stop after seeing 1 item (index 0 is processed, then check index 1)
         // Actually, logic is: cb is called for Item 0.
         // If stop_at_index == 0, it returns STOP immediately.
         // Let's test stopping at the 2nd item (index 1).
  ctx.stop_at_index = 1;
  ctx.error_at_index = -1;

  // Item 0: "1" processed, count->1. Returns CONTINUE.
  // Item 1: "2" processed check: count is 1. Matches stop_at_index. Returns
  // STOP.

  bin_log_scan_result_t res = bin_log_scan(&log_handle, scan_test_cb, &ctx);

  TEST_ASSERT_EQUAL_INT(BL_SCAN_STOPPED, res);
  // Should have processed item 0 ("1"), then encountered item 1 ("2") and
  // stopped. Note: My callback increments count BEFORE returning STOP? Let's
  // check callback logic: if (ctx->count == ctx->stop_at_index) return
  // BL_CB_STOP;
  // ... memcpy ... count++ ... return CONTINUE.

  // If stop_at_index is 1:
  // Call 1 (Item 0): count is 0. != 1. Copied. count becomes 1. Returns
  // CONTINUE. Call 2 (Item 1): count is 1. == 1. Returns STOP. Not copied.

  // So we expect 1 item successfully processed.
  TEST_ASSERT_EQUAL_INT(1, ctx.count);
  TEST_ASSERT_EQUAL_STRING("1", ctx.items[0]);
}

void test_Scan_ShouldError_WhenCallbackReturnsError(void) {
  bin_log_init(&log_handle, TEST_LOG_PATH);
  bin_log_append(&log_handle, "A", 1);

  scan_ctx_t ctx = {0};
  ctx.stop_at_index = -1;
  ctx.error_at_index = 0; // Error immediately on first item

  bin_log_scan_result_t res = bin_log_scan(&log_handle, scan_test_cb, &ctx);

  TEST_ASSERT_EQUAL_INT(BL_SCAN_CB_ERR, res);
  TEST_ASSERT_EQUAL_INT(0, ctx.count);
}

void test_TornWrite_ShouldReturnTornStatus(void) {
  // 1. Create a valid log with 1 entry
  bin_log_init(&log_handle, TEST_LOG_PATH);
  bin_log_append(&log_handle, "Valid", 5);
  bin_log_close(&log_handle);

  // 2. Append a partial record manually
  // Structure: [Len 4B] [CRC 4B] [Data...]
  FILE *f = fopen(TEST_LOG_PATH, "ab");
  uint32_t len = 10;
  uint32_t crc = 0xDEADBEEF;
  fwrite(&len, 4, 1, f);
  fwrite(&crc, 4, 1, f);
  fwrite("123", 1, 3, f); // Write only 3 of 10 bytes
  fclose(f);

  // 3. Re-open and Scan
  bin_log_t new_handle = {0};
  bin_log_init(&new_handle, TEST_LOG_PATH);

  scan_ctx_t ctx = {0};
  ctx.stop_at_index = -1;
  ctx.error_at_index = -1;

  bin_log_scan_result_t res = bin_log_scan(&new_handle, scan_test_cb, &ctx);

  // Expecting TORN status
  TEST_ASSERT_EQUAL_INT(BL_SCAN_TORN, res);

  // Verify the first valid record was still read
  TEST_ASSERT_EQUAL_INT(1, ctx.count);
  TEST_ASSERT_EQUAL_STRING("Valid", ctx.items[0]);

  bin_log_close(&new_handle);
}

void test_DataCorruption_ShouldReturnCrcError(void) {
  // 1. Create valid log
  bin_log_init(&log_handle, TEST_LOG_PATH);
  bin_log_append(&log_handle, "CleanData", 9);
  bin_log_close(&log_handle);

  // 2. Corrupt a byte in the data section
  // Record start: 0.
  // Data starts at: 4 (Len) + 4 (CRC) = 8.
  FILE *f = fopen(TEST_LOG_PATH, "r+b");
  fseek(f, 8 + 2, SEEK_SET); // 3rd byte of "CleanData" ('e')
  fputc('X', f);             // Change to 'X'
  fclose(f);

  // 3. Scan
  bin_log_t new_handle = {0};
  bin_log_init(&new_handle, TEST_LOG_PATH);

  scan_ctx_t ctx = {0};
  ctx.stop_at_index = -1;
  ctx.error_at_index = -1;

  bin_log_scan_result_t res = bin_log_scan(&new_handle, scan_test_cb, &ctx);

  TEST_ASSERT_EQUAL_INT(BL_SCAN_ERR_CRC, res);
  TEST_ASSERT_EQUAL_INT(0, ctx.count); // No valid records read

  bin_log_close(&new_handle);
}

void test_Inputs_ShouldFailGracefully(void) {
  // Test Null Log
  TEST_ASSERT_EQUAL_INT(BL_SCAN_ERR_INVALID,
                        bin_log_scan(NULL, scan_test_cb, NULL));

  // Test Null Callback
  bin_log_init(&log_handle, TEST_LOG_PATH);
  TEST_ASSERT_EQUAL_INT(BL_SCAN_ERR_INVALID,
                        bin_log_scan(&log_handle, NULL, NULL));
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_Init_ShouldCreateFile_WhenPathValid);
  RUN_TEST(test_Init_ShouldFail_WhenPathIsNull);
  RUN_TEST(test_AppendAndScan_HappyPath);
  RUN_TEST(test_Scan_ShouldReturnNoLog_IfFileDeleted);
  RUN_TEST(test_Scan_ShouldStop_WhenCallbackRequestsStop);
  RUN_TEST(test_Scan_ShouldError_WhenCallbackReturnsError);
  RUN_TEST(test_TornWrite_ShouldReturnTornStatus);
  RUN_TEST(test_DataCorruption_ShouldReturnCrcError);
  RUN_TEST(test_Inputs_ShouldFailGracefully);
  return UNITY_END();
}