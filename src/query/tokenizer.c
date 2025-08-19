#include "query/tokenizer.h"
#include "core/queue.h"
#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const int MAX_TOKENS = 256;
const int MAX_TEXT_VAL_LEN = 256;
const int MAX_NUMBERS_SEQ = 9;
const int MAX_TOTAL_CHARS = 2048;

void tok_clear_all(Queue *tokens) {
  if (!tokens)
    return;
  token_t *t;
  while (!q_empty(tokens)) {
    t = q_dequeue(tokens);
    tok_free(t);
  }
}

void tok_free(token_t *token) {
  if (!token)
    return;
  free(token->text_value);
  free(token);
}

static void *_cleanup_on_err(Queue *q) {
  tok_clear_all(q);
  q_destroy(q);
  return NULL;
}

static token_t *_create_token(token_type type, char *text_value,
                              uint32_t number_value) {
  token_t *t = malloc(sizeof(token_t));
  if (!t)
    return NULL;
  t->type = type;
  t->text_value = text_value ? strdup(text_value) : NULL;
  t->number_value = number_value;
  return t;
}

static bool _max_toks(int num_tokens) { return num_tokens >= MAX_TOKENS; }

// Any char other than these must be enclosed in quotes.
static bool _valid_unenclosed_char(char c) {
  return c == '_' || c == '-' || isalnum((unsigned char)c);
}

static bool _enqueue(Queue *q, int *num_tokens, token_type type,
                     token_t **out_token, size_t *i, int incr_i,
                     char *text_value, uint32_t number_value) {
  if (_max_toks(*num_tokens)) {
    _cleanup_on_err(q);
    return false;
  }
  *out_token = _create_token(type, text_value, number_value);
  if (!*out_token) {
    _cleanup_on_err(q);

    return false;
  }
  q_enqueue(q, *out_token);
  ++(*num_tokens);

  if (incr_i > 0)
    *i = *i + incr_i;
  return true;
}

// TODO: should not return 0 on error.
// Change the function signature to
// bool _parse_uint32(const char *str, size_t len, uint32_t *out_value)
// to return success/failure and pass the result via a pointer.
// `len`: Length of number
static uint32_t _parse_uint32(const char *str, size_t len) {
  if (len == 0 || len > 10) { // Max 10 digits for uint32_t
    return 0;
  }
  char buf[12] = {0}; // Enough for max uint32_t (10 digits) + null + 1 extra
  memcpy(buf, str, len);
  buf[len] = '\0';
  char *endptr;
  unsigned long value = strtoul(buf, &endptr, 10);
  if (*endptr != '\0' || value > UINT32_MAX) {
    return 0;
  }

  return (uint32_t)value;
}

static void _to_lowercase(char *dest, const char *src, size_t max_len) {
  size_t i;
  for (i = 0; i < max_len - 1 && src[i]; i++) {
    dest[i] = (char)tolower((unsigned char)src[i]);
  }
  dest[i] = '\0';
}

struct {
  const char *kw;
  token_type type;
} kw_map[] = {
    {"and", TOKEN_OP_AND},       {"or", TOKEN_OP_OR},
    {"not", TOKEN_OP_NOT},       {"event", TOKEN_CMD_EVENT},
    {"query", TOKEN_CMD_QUERY},  {"in", TOKEN_KW_IN},
    {"id", TOKEN_KW_ID},         {"entity", TOKEN_KW_ENTITY},
    {"cursor", TOKEN_KW_CURSOR}, {"take", TOKEN_KW_TAKE},
    {"exp", TOKEN_KW_EXP},
};

// Return tokens from input string.
// TODO: Create an iterator/stream, i.e. get_next_token()
Queue *tok_tokenize(char *input) {
  int num_tokens = 0;
  size_t i = 0;
  size_t start = 0;
  size_t end = 0;
  if (!input)
    return NULL;
  size_t input_len = strlen(input);
  if (!input_len || input_len > MAX_TOTAL_CHARS)
    return NULL;

  Queue *q = q_create();

  while (i < input_len) {
    token_t *t;
    char c = input[i];
    bool has_next_char = i + 1 < input_len;

    if (isspace((unsigned char)c)) {
      i++;
    }

    else if (c == '(') {
      if (!_enqueue(q, &num_tokens, TOKEN_SYM_LPAREN, &t, &i, 1, NULL, 0))
        return NULL;
    }

    else if (c == ')') {
      if (!_enqueue(q, &num_tokens, TOKEN_SYM_RPAREN, &t, &i, 1, NULL, 0))
        return NULL;
    }

    else if (c == '>' && has_next_char && input[i + 1] == '=') {
      if (!_enqueue(q, &num_tokens, TOKEN_OP_GTE, &t, &i, 2, NULL, 0))
        return NULL;
    }

    else if (c == '>') {
      if (!_enqueue(q, &num_tokens, TOKEN_OP_GT, &t, &i, 1, NULL, 0))
        return NULL;
    }

    else if (c == '<' && has_next_char && input[i + 1] == '=') {
      if (!_enqueue(q, &num_tokens, TOKEN_OP_LTE, &t, &i, 2, NULL, 0))
        return NULL;
    }

    else if (c == '<') {
      if (!_enqueue(q, &num_tokens, TOKEN_OP_LT, &t, &i, 1, NULL, 0))
        return NULL;
    }

    else if (c == '=') {
      if (!_enqueue(q, &num_tokens, TOKEN_OP_EQ, &t, &i, 1, NULL, 0))
        return NULL;
    }

    else if (c == ':') {
      if (!_enqueue(q, &num_tokens, TOKEN_SYM_COLON, &t, &i, 1, NULL, 0))
        return NULL;
    }

    else if (c == '+') {
      if (!_enqueue(q, &num_tokens, TOKEN_SYM_PLUS, &t, &i, 1, NULL, 0))
        return NULL;
    }

    else if (c == '"' || _valid_unenclosed_char(c)) {
      bool quotes = false;
      char *val = NULL;
      bool all_digits = true;

      if (c == '"') {
        quotes = true;
        i++;
      }

      if (quotes) {
        // Parse quoted string with escape support
        size_t bufcap = MAX_TEXT_VAL_LEN + 1;
        val = malloc(bufcap);
        if (!val)
          return _cleanup_on_err(q);
        size_t vpos = 0;
        bool closed = false;
        while (input[i] && vpos < MAX_TEXT_VAL_LEN) {
          if (input[i] == '\\') {
            i++;
            if (!input[i])
              break;
            if (input[i] == '"' || input[i] == '\\') {
              val[vpos++] = input[i++];
            } else {
              // Accept any escaped char as literal (e.g. \n -> n)
              val[vpos++] = input[i++];
            }
          } else if (input[i] == '"') {
            closed = true;
            i++;
            break;
          } else {
            val[vpos++] = input[i++];
          }
        }
        val[vpos] = '\0';
        if (!closed || vpos == 0) {
          free(val);
          return _cleanup_on_err(q);
        }
        if (!_enqueue(q, &num_tokens, TOKEN_LITERAL_STRING, &t, &i, 0, val,
                      0)) {
          free(val);
          return NULL;
        }
        // i is already at the next char after closing quote
      } else {
        start = i;
        end = start;
        while (input[end] && _valid_unenclosed_char(input[end])) {
          if (!isdigit((unsigned char)input[end]))
            all_digits = false;
          ++end;
        }
        size_t len = end - start;
        if (len == 0 || len > MAX_TEXT_VAL_LEN ||
            (all_digits && len > MAX_NUMBERS_SEQ)) {
          return _cleanup_on_err(q);
        }
        val = malloc(sizeof(char) * (len + 1));
        if (!val) {
          return _cleanup_on_err(q);
        }
        strncpy(val, &input[start], len);
        val[len] = '\0';
        i = end;

        if (all_digits) {
          uint32_t n_val = _parse_uint32(val, len);
          free(val);
          if (!_enqueue(q, &num_tokens, TOKEN_LITERAL_NUMBER, &t, &i, 0, NULL,
                        n_val))
            return NULL;
        } else {
          char *lower_text_value = malloc(len + 1);
          if (!lower_text_value) {
            _cleanup_on_err(q);
            free(val);
            return NULL;
          }
          _to_lowercase(lower_text_value, val, len + 1);
          free(val);

          bool matched = false;
          for (size_t k = 0; k < sizeof(kw_map) / sizeof(kw_map[0]); ++k) {
            if (strcmp(lower_text_value, kw_map[k].kw) == 0) {
              free(lower_text_value);
              if (!_enqueue(q, &num_tokens, kw_map[k].type, &t, &i, 0, NULL, 0))
                return NULL;
              matched = true;
              break;
            }
          }
          if (!matched) {
            if (!_enqueue(q, &num_tokens, TOKEN_IDENTIFER, &t, &i, 0,
                          lower_text_value, 0)) {
              free(lower_text_value);
              return NULL;
            }
          }
        }
      }
    }

    else {
      // invalid character
      return _cleanup_on_err(q);
    }
  }

  if (q_empty(q)) {
    q_destroy(q);
    return NULL;
  }

  return q;
}
