#include "api.h"
#include "engine.h"
#include <stdlib.h>
#include <string.h>

const int MAX_NS_LEN = 128;
const int MAX_KEY_LEN = 128;
const int MAX_ID_LEN = 128;

void free_api_response(api_response_t *r) {
  free(r->data);
  free(r);
}

static api_response_t *_create_api_resp(enum api_response_type t) {
  api_response_t *r = malloc(sizeof(api_response_t));
  r->is_ok = false; // Default to false
  r->data = NULL;
  r->err_msg = NULL;
  r->type = t;
  return r;
}

static bool _validate_ns_key_id(api_response_t *r, char *ns, char *key,
                                char *id) {
  if (!ns) {
    r->err_msg = "Missing namespace";
    return false;
  }
  if (!key) {
    r->err_msg = "Missing key";
    return false;
  }
  if (!id) {
    r->err_msg = "Missing id";
    return false;
  }
  if (strlen(ns) > MAX_NS_LEN) {
    r->err_msg = "Namespace too long";
    return false;
  }
  if (strlen(key) > MAX_KEY_LEN) {
    r->err_msg = "Key too long";
    return false;
  }
  if (strlen(id) > MAX_ID_LEN) {
    r->err_msg = "Id too long";
    return false;
  }
  return true;
}

api_response_t *api_add(char *ns, char *key, char *id, eng_db_t *db) {
  api_response_t *r = _create_api_resp(ADD);
  bool valid = _validate_ns_key_id(r, ns, key, id);
  if (!valid)
    return r;
  eng_add(r, db, ns, key, id);
  return r;
}
