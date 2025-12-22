/**
 * Worker helpers for logging
 */

#define _LOG_ENT_IMPL(level_func, action, ent_node, fmt, ...)                  \
  do {                                                                         \
    if ((ent_node)->type == AST_LITERAL_STRING) {                              \
      level_func(action, "entity=\"%s\" " fmt, (ent_node)->string_value,       \
                 ##__VA_ARGS__);                                               \
    } else {                                                                   \
      level_func(action, "entity=%lld " fmt,                                   \
                 (long long)(ent_node)->number_value, ##__VA_ARGS__);          \
    }                                                                          \
  } while (0)

#if LOG_LEVEL <= LOG_LEVEL_DEBUG
#define LOG_ENT_DEBUG(action, node, fmt, ...)                                  \
  _LOG_ENT_IMPL(LOG_ACTION_DEBUG, action, node, fmt, ##__VA_ARGS__)
#else
#define LOG_ENT_DEBUG(...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_INFO
#define LOG_ENT_INFO(action, node, fmt, ...)                                   \
  _LOG_ENT_IMPL(LOG_ACTION_INFO, action, node, fmt, ##__VA_ARGS__)
#else
#define LOG_ENT_INFO(...)
#endif

#if LOG_LEVEL <= LOG_LEVEL_ERROR
#define LOG_ENT_ERROR(action, node, fmt, ...)                                  \
  _LOG_ENT_IMPL(LOG_ACTION_ERROR, action, node, fmt, ##__VA_ARGS__)
#else
#define LOG_ENT_ERROR(...)
#endif