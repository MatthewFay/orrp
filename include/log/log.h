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

// Each module will call LOG_INIT("module_name") once
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
