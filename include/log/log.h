#pragma once
#include "uv.h"
#include "zlog.h"

// Define log levels numerically so we can strip by level
#define LOG_LEVEL_DEBUG 0
#define LOG_LEVEL_INFO 1
#define LOG_LEVEL_WARN 2
#define LOG_LEVEL_ERROR 3
#define LOG_LEVEL_FATAL 4

#ifndef LOG_LEVEL
#define LOG_LEVEL LOG_LEVEL_INFO
#endif

// =============================================================================
// == GLOBAL INITIALIZATION & SHUTDOWN                                        ==
// =============================================================================

/**
 * Initializes the global zlog system from a configuration file.
 * This MUST be called once at the beginning of main().
 *
 * @param conf_path Path to the zlog.conf file.
 * @return 0 on success, -1 on failure.
 */
static inline int log_global_init(const char *conf_path) {
  int rc = zlog_init(conf_path);
  if (rc != 0) {
    fprintf(stderr, "FATAL: zlog_init() failed from config: %s\n", conf_path);
    return -1;
  }
  return 0;
}

/**
 * Shuts down the global zlog system, flushing any buffered logs.
 * This should be called once at the very end of main() before exiting.
 */
static inline void log_global_shutdown(void) { zlog_fini(); }

// Each module will call `LOG_INIT(module_name);` once
// This is Thread-safe
#define LOG_INIT(category)                                                     \
  static zlog_category_t *LOG_CATEGORY = NULL;                                 \
  static uv_once_t LOG_INIT_ONCE = UV_ONCE_INIT;                               \
  static void _log_init_task_##category(void) {                                \
    LOG_CATEGORY = zlog_get_category(#category);                               \
    if (!LOG_CATEGORY) {                                                       \
      fprintf(stderr, "FATAL: zlog_get_category failed for '%s'\n",            \
              #category);                                                      \
    }                                                                          \
  }                                                                            \
  static inline void log_init_##category(void) {                               \
    uv_once(&LOG_INIT_ONCE, _log_init_task_##category);                        \
  }

// =============================================================================
// == STANDARD LOGGING MACROS                                                 ==
// =============================================================================

// Macros for logging that compile out if below build-level
#if LOG_LEVEL <= LOG_LEVEL_DEBUG
#define LOG_DEBUG(fmt, ...) zlog_debug(LOG_CATEGORY, fmt, ##__VA_ARGS__)
#else
#define LOG_DEBUG(fmt, ...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_INFO
#define LOG_INFO(fmt, ...) zlog_info(LOG_CATEGORY, fmt, ##__VA_ARGS__)
#else
#define LOG_INFO(fmt, ...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_WARN
#define LOG_WARN(fmt, ...) zlog_warn(LOG_CATEGORY, fmt, ##__VA_ARGS__)
#else
#define LOG_WARN(fmt, ...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_ERROR
#define LOG_ERROR(fmt, ...) zlog_error(LOG_CATEGORY, fmt, ##__VA_ARGS__)
#else
#define LOG_ERROR(fmt, ...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_FATAL
#define LOG_FATAL(fmt, ...) zlog_fatal(LOG_CATEGORY, fmt, ##__VA_ARGS__)
#else
#define LOG_FATAL(fmt, ...)
#endif

// =============================================================================
// == STRUCTURED LOGGING (logfmt) - Machine-Readable                          ==
// =============================================================================

/**
 * Structured logging helpers using logfmt format: action=name key=value ...
 *
 * Usage:
 *   LOG_ACTION_DEBUG("cache_hit", "key=\"%s\" latency_us=%lu", key, latency);
 *   LOG_ACTION_ERROR("db_write_failed", "key=\"%s\" err=\"%s\"", key, err);
 */

#if LOG_LEVEL <= LOG_LEVEL_DEBUG
#define LOG_ACTION_DEBUG(action, fmt, ...)                                     \
  zlog_debug(LOG_CATEGORY, "action=" action " " fmt, ##__VA_ARGS__)
#else
#define LOG_ACTION_DEBUG(action, fmt, ...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_INFO
#define LOG_ACTION_INFO(action, fmt, ...)                                      \
  zlog_info(LOG_CATEGORY, "action=" action " " fmt, ##__VA_ARGS__)
#else
#define LOG_ACTION_INFO(action, fmt, ...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_WARN
#define LOG_ACTION_WARN(action, fmt, ...)                                      \
  zlog_warn(LOG_CATEGORY, "action=" action " " fmt, ##__VA_ARGS__)
#else
#define LOG_ACTION_WARN(action, fmt, ...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_ERROR
#define LOG_ACTION_ERROR(action, fmt, ...)                                     \
  zlog_error(LOG_CATEGORY, "action=" action " " fmt, ##__VA_ARGS__)
#else
#define LOG_ACTION_ERROR(action, fmt, ...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_FATAL
#define LOG_ACTION_FATAL(action, fmt, ...)                                     \
  zlog_fatal(LOG_CATEGORY, "action=" action " " fmt, ##__VA_ARGS__)
#else
#define LOG_ACTION_FATAL(action, fmt, ...)
#endif

// =============================================================================
// == STANDARD ACTION NAMES                                                   ==
// =============================================================================

// Client operations
#define ACT_CLIENT_TIMEOUT "client_timeout"
#define ACT_CLIENT_FREED "client_freed"
#define ACT_CLIENT_CONNECTED "client_connected"
#define ACT_CLIENT_DISCONNECTED "client_disconnected"

// Command processing
#define ACT_CMD_RECEIVED "cmd_received"
#define ACT_CMD_PROCESSING "cmd_processing"
#define ACT_CMD_SUCCEEDED "cmd_succeeded"
#define ACT_CMD_TOO_LONG "cmd_too_long"
#define ACT_CMD_CTX_BUILD_FAILED "cmd_ctx_build_failed"
#define ACT_CMD_ENQUEUE_FAILED "cmd_enqueue_failed"

// Key formatting operations
#define ACT_KEY_FORMAT_FAILED "key_format_failed"

// Tag counter operations
#define ACT_TAG_COUNTER_NOT_FOUND "tag_counter_not_found"

// Parsing operations
#define ACT_TOKENIZATION_FAILED "tokenization_failed"
#define ACT_PARSE_FAILED "parse_failed"

// API execution
#define ACT_API_EXEC_FAILED "api_exec_failed"
#define ACT_CMD_EXEC_FAILED "cmd_exec_failed"

// Translator
#define ACT_TRANSLATION_ERROR "translation_error"

// Work queue operations
#define ACT_WORK_QUEUE_FAILED "work_queue_failed"

// Connection operations
#define ACT_CONNECTION_FAILED "connection_failed"
#define ACT_CONNECTION_REJECTED "connection_rejected"

// Data operations
#define ACT_DATA_RECEIVED "data_received"
#define ACT_READ_FAILED "read_failed"
#define ACT_BUFFER_OVERFLOW "buffer_overflow"

// Server lifecycle
#define ACT_SERVER_STARTED "server_started"
#define ACT_SERVER_CONFIG "server_config"
#define ACT_SERVER_START_FAILED "server_start_failed"
#define ACT_SERVER_SHUTDOWN_INITIATED "server_shutdown_initiated"
#define ACT_SERVER_FINALIZING "server_finalizing"

// Signal handling
#define ACT_SIGNAL_RECEIVED "signal_received"

// Handle management
#define ACT_HANDLE_CLOSING "handle_closing"
#define ACT_LOOP_CLOSE_FAILED "loop_close_failed"

// Operation Lifecycle
#define ACT_OP_RECEIVED "op_received"
#define ACT_OP_VALIDATED "op_validated"
#define ACT_OP_VALIDATION_FAILED "op_validation_failed"
#define ACT_OP_APPLIED "op_applied"
#define ACT_OP_REJECTED "op_rejected"
// Operation creation
#define ACT_OP_CREATE_FAILED "op_create_failed"
#define ACT_OP_CREATED "op_created"

// Cache Operations
#define ACT_CACHE_HIT "cache_hit"
#define ACT_CACHE_MISS "cache_miss"
#define ACT_CACHE_ENTRY_CREATED "cache_entry_created"
#define ACT_CACHE_ENTRY_UPDATED "cache_entry_updated"
#define ACT_CACHE_ENTRY_EVICTED "cache_entry_evicted"
#define ACT_CACHE_ENTRY_EVICT_FAILED "cache_entry_evict_failed"
#define ACT_CACHE_ENTRY_FREED "cache_entry_freed"
#define ACT_CACHE_ENTRY_CREATE_FAILED "cache_entry_create_failed"
#define ACT_CACHE_ENTRY_ADD_FAILED "cache_entry_add_failed"

// Database Operations
#define ACT_DB_READ "db_read"
#define ACT_DB_READ_NOT_FOUND "db_read_not_found"
#define ACT_DB_WRITE "db_write"
#define ACT_DB_DELETE "db_delete"
#define ACT_DB_READ_FAILED "db_read_failed"
#define ACT_DB_WRITE_FAILED "db_write_failed"
#define ACT_DB_DELETE_FAILED "db_delete_failed"
#define ACT_DB_HANDLE_FAILED "db_handle_failed"

// Container Operations
#define ACT_CONTAINER_OPENED "container_opened"
#define ACT_CONTAINER_CLOSED "container_closed"
#define ACT_CONTAINER_OPEN_FAILED "container_open_failed"
#define ACT_CONTAINER_CACHED "container_cached"
#define ACT_CONTAINER_EVICTED "container_evicted"

// Transaction Operations
#define ACT_TXN_BEGIN "txn_begin"
#define ACT_TXN_COMMIT "txn_commit"
#define ACT_TXN_ABORT "txn_abort"
#define ACT_TXN_FAILED "txn_failed"

// Queue Operations
#define ACT_MSG_ENQUEUED "msg_enqueued"
#define ACT_MSG_DEQUEUED "msg_dequeued"
#define ACT_MSG_PROCESSED "msg_processed"
#define ACT_MSG_CREATE_FAILED "msg_create_failed"
#define ACT_MSG_INVALID "msg_invalid"
#define ACT_MSG_PROCESS_FAILED "msg_process_failed"
#define ACT_MSG_ENQUEUE_FAILED "msg_enqueue_failed"
#define ACT_QUEUE_FULL "queue_full"
#define ACT_QUEUE_EMPTY "queue_empty"
#define ACT_QUEUE_INIT "queue_init"
#define ACT_QUEUE_INIT_FAILED "queue_init_failed"
#define ACT_QUEUE_DESTROY "queue_destroy"
#define ACT_QUEUE_INVALID "queue_invalid"

// Thread Operations
#define ACT_THREAD_STARTED "thread_started"
#define ACT_THREAD_STOPPED "thread_stopped"

// Worker
#define ACT_WORKER_IDLE "worker_idle"
#define ACT_WORKER_BUSY "worker_busy"
#define ACT_WORKER_WRITER_MSG_FAILED "worker_writer_msg_failed"

// System Events
#define ACT_SYSTEM_INIT "system_init"
#define ACT_SYSTEM_SHUTDOWN "system_shutdown"
#define ACT_MEMORY_ALLOC_FAILED "memory_alloc_failed"
#define ACT_RESOURCE_EXHAUSTED "resource_exhausted"

// Performance/Metrics
#define ACT_PERF_SLOW_OP "perf_slow_op"
#define ACT_PERF_BATCH_COMPLETE "perf_batch_complete"
#define ACT_PERF_FLUSH_COMPLETE "perf_flush_complete"

#define ACT_DESERIALIZATION_FAILED "deserialization_failed"
#define ACT_SERIALIZATION_FAILED "serialization_failed"
#define ACT_SERIALIZATION_SUCCESS "serialization_success"

// Bitmap operations
#define ACT_BITMAP_COPY_FAILED "bitmap_copy_failed"

// Subsystem Operations
#define ACT_SUBSYSTEM_INIT "subsystem_init"
#define ACT_SUBSYSTEM_INIT_FAILED "subsystem_init_failed"
#define ACT_SUBSYSTEM_SHUTDOWN "subsystem_shutdown"

// Thread Pool Operations
#define ACT_THREAD_POOL_STARTING "thread_pool_starting"
#define ACT_THREAD_POOL_STOPPING "thread_pool_stopping"
#define ACT_THREAD_START_FAILED "thread_start_failed"
#define ACT_THREAD_STOP_FAILED "thread_stop_failed"
#define ACT_THREAD_STOPPING "thread_stopping"

// Batch operations
#define ACT_BATCH_INVALID "batch_invalid"
#define ACT_BATCH_CREATED "batch_created"
#define ACT_BATCH_ADD_FAILED "batch_add_failed"
#define ACT_BATCH_PROCESSED "batch_processed"
#define ACT_BATCH_PROCESS_FAILED "batch_process_failed"
#define ACT_BATCH_GROUPING "batch_grouping"
#define ACT_BATCH_GROUPING_FAILED "batch_grouping_failed"

// Write operations
#define ACT_WRITE_FAILED "write_failed"

// Writer statistics
#define ACT_WRITER_STATS "writer_stats"

// Flush operations
#define ACT_FLUSH_STARTING "flush_starting"
#define ACT_FLUSH_FAILED "flush_failed"
#define ACT_FLUSH_ENTRIES_SKIPPED "flush_entries_skipped"
// Flush version operations
#define ACT_FLUSH_VERSION_UPDATED "flush_version_updated"
#define ACT_FLUSH_PARTIAL_FAILURE "flush_partial_failure"

// EBR operations
#define ACT_EBR_RECLAIM "ebr_reclaim"

// Consumer statistics
#define ACT_CONSUMER_STATS "consumer_stats"

// General operations
#define ACT_INVALID_ARGS "invalid_args"

// Counter operations
#define ACT_COUNTER_INIT "counter_init"

// Race condition handling
#define ACT_RACE_CONDITION "race_condition"

// Entity operations
#define ACT_ENTITY_MAPPING_FAILED "entity_mapping_failed"
#define ACT_EVENT_ID_FAILED "event_id_failed"
#define ACT_TAG_COUNTER_FAILED "tag_counter_failed"
#define ACT_COUNTER_ENTITY_FAILED "counter_entity_failed"

// Cleanup
#define ACT_CLEANUP_COMPLETE "cleanup_complete"

// Worker statistics
#define ACT_WORKER_STATS "worker_stats"

// Query
#define ACT_QUERY_ERROR "query_error"

// =============================================================================
// == COMMON FIELD NAMES (for consistency)                                    ==
// =============================================================================

/**
 * Standard field naming conventions for logfmt:
 *
 * Identifiers:
 *   key="..."           - Database key
 *   entity_id="..."     - Entity identifier
 *   event_id=123        - Event identifier
 *   container="..."     - Container name
 *   thread_id=123       - Thread identifier
 *
 * Types/Categories:
 *   op_type=put         - Operation type
 *   db_type=sys         - Database type
 *   val_type=bitmap     - Value type
 *
 * Errors:
 *   err="not found"     - Error message
 *   err_code=404        - Error code
 *
 * Performance:
 *   duration_ms=123     - Duration in milliseconds
 *   duration_us=456     - Duration in microseconds
 *   latency_us=789      - Latency in microseconds
 *   count=100           - Count/size
 *   size_bytes=1024     - Size in bytes
 *
 * Status:
 *   status=success      - Operation status
 *   cached=true         - Cache hit/miss
 *   dirty=true          - Dirty flag
 */

// =============================================================================
// == USAGE EXAMPLES                                                          ==
// =============================================================================

/*
// Basic usage:
LOG_ACTION_INFO(ACT_CACHE_HIT, "key=\"%s\" latency_us=%lu", key, latency);

// Output:
// 10-26 14:23:45.123 INFO  [thread:consumer:236] action=cache_hit
key="user_123" latency_us=42

// With multiple fields:
LOG_ACTION_ERROR(ACT_DB_WRITE_FAILED,
                 "key=\"%s\" container=\"%s\" err=\"%s\" err_code=%d",
                 key, container, strerror(errno), errno);

// Output:
// 10-26 14:23:45.124 ERROR [thread:writer:89] action=db_write_failed key="xyz"
container="user_456" err="Permission denied" err_code=13

// Performance logging:
LOG_ACTION_DEBUG(ACT_PERF_BATCH_COMPLETE,
                 "count=%zu duration_ms=%lu avg_latency_us=%lu",
                 batch_size, duration_ms, avg_latency);

// Output:
// 10-26 14:23:45.125 DEBUG [thread:consumer:156] action=perf_batch_complete
count=1000 duration_ms=234 avg_latency_us=234
*/