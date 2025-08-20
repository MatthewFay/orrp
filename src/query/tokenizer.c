#include "query/tokenizer.h"
#include "core/queue.h"
#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const int MAX_TOKENS = 256;
const int MAX_TEXT_VAL_LEN = 128;
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

// Characters allowed in an unquoted identifier.
static bool _valid_unenclosed_char(char c) {
  return isalnum((unsigned char)c) || c == '_' || c == '-';
}

// A whitelist for characters allowed within a quoted string.
static bool _valid_enclosed_char(char c) {
  return _valid_unenclosed_char(c) || c == ' ';
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

    // --- UNIFIED PARSING LOGIC FOR STRINGS AND IDENTIFIERS ---
    else if (c == '"' || _valid_unenclosed_char(c)) {
      char *val = NULL;
      size_t start, end;

      bool quotes = (c == '"');
      start = i + (quotes ? 1 : 0); // Content starts after the quote
      end = start;

      // Find the end of the token
      while (end < input_len) {
        char current_char = input[end];
        if (quotes) {
          if (current_char == '"')
            break; // End of quoted string
          if (!_valid_enclosed_char(current_char))
            return _cleanup_on_err(q);
        } else {
          if (!_valid_unenclosed_char(current_char))
            break; // End of identifier
        }
        end++;
      }

      if (quotes && (end >= input_len || input[end] != '"')) {
        return _cleanup_on_err(q); // Unterminated quoted string
      }

      size_t len = end - start;
      if (len == 0 || len > MAX_TEXT_VAL_LEN) {
        return _cleanup_on_err(q);
      }

      // Extract the token value
      val = malloc(len + 1);
      if (!val)
        return _cleanup_on_err(q);
      strncpy(val, &input[start], len);
      val[len] = '\0';

      // Process and enqueue the token
      if (quotes) {
        i = end + 1; // Move past the closing quote
        if (!_enqueue(q, &num_tokens, TOKEN_LITERAL_STRING, &t, &i, 0, val,
                      0)) {
          free(val);
          return NULL;
        }
        free(val);
      } else {
        i = end; // Move to the end of the identifier
        bool all_digits = true;
        for (size_t k = 0; k < len; k++) {
          if (!isdigit((unsigned char)val[k])) {
            all_digits = false;
            break;
          }
        }

        if (all_digits) {
          if (len > MAX_NUMBERS_SEQ)
            return _cleanup_on_err(q);
          uint32_t n_val = _parse_uint32(val, len);
          free(val);
          if (!_enqueue(q, &num_tokens, TOKEN_LITERAL_NUMBER, &t, &i, 0, NULL,
                        n_val))
            return NULL;
        } else {
          char *lower_text_value = malloc(len + 1);
          if (!lower_text_value) {
            free(val);
            return _cleanup_on_err(q);
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
