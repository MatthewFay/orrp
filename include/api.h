#ifndef API_H
#define API_H

#include "engine.h"
#include <stdbool.h>

enum api_response_type { ADD };

typedef struct api_response_s {
  enum api_response_type type;
  bool is_ok;
  void *data;
  const char *err_msg;
} api_response_t;

void free_api_response(api_response_t *r);

// ADD <namespace> <key> <id>
api_response_t *api_add(char *ns, char *key, char *id, eng_db_t *db);

#endif // API_H