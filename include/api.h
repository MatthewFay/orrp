#include <stdbool.h>
#include "core/db.h"

enum api_response_type
{
  ADD
};

typedef struct api_response_s
{
  enum api_response_type type;
  bool is_ok;
  void *data;
  char *err_msg;
} api_response_t;

void free_api_response(api_response_t *r);

// ADD <namespace> <key> <id>
api_response_t *api_add(char *ns, char *key, char *id, eng_db_t *db);
