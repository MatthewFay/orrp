#include "query/tokenizer.h"
#include "core/data_constants.h"
#include "core/queue.h"
#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const int MAX_TOKENS = 256; // Do we need this? Already limit total chars

void tok_clear_all(queue_t *tokens) {
  if (!tokens)
    return;
  token_t *t;
  while (!queue_empty(tokens)) {
    t = queue_dequeue(tokens);
    tok_free(t);
  }
}

void tok_free(token_t *token) {
  if (!token)
    return;
  free(token->text_value);
  free(token);
}

static void *_cleanup_on_err(queue_t *q) {
  tok_clear_all(q);
  queue_destroy(q);
  return NULL;
}

static token_t *_create_token(token_type type, char *text_value,
                              int64_t number_value, size_t text_value_len) {
  token_t *t = malloc(sizeof(token_t));
  if (!t)
    return NULL;
  t->type = type;
  t->text_value = text_value ? strdup(text_value) : NULL;
  t->number_value = number_value;
  t->text_value_len = text_value_len;
  return t;
}

static bool _max_toks(int num_tokens) { return num_tokens >= MAX_TOKENS; }

// Characters allowed in an unquoted identifier.
static bool _valid_unenclosed_char(char c) {
  return isalnum((unsigned char)c) || c == '_' || c == '-' || c == '.';
}

// A whitelist for characters allowed within a quoted string.
static bool _valid_enclosed_char(char c) {
  return _valid_unenclosed_char(c) || c == ' ';
}

static bool _enqueue(queue_t *q, int *num_tokens, token_type type,
                     token_t **out_token, size_t *i, int incr_i,
                     char *text_value, int64_t number_value,
                     size_t text_value_len) {
  if (_max_toks(*num_tokens)) {
    _cleanup_on_err(q);
    return false;
  }
  *out_token = _create_token(type, text_value, number_value, text_value_len);
  if (!*out_token) {
    _cleanup_on_err(q);
    return false;
  }
  queue_enqueue(q, *out_token);
  ++(*num_tokens);

  if (incr_i > 0)
    *i = *i + incr_i;
  return true;
}

// Parses a string into a int64_t.
// Returns true on success, false on failure (invalid chars or overflow).
// Result is stored in *out_value.
static bool _parse_int64(const char *str, size_t len, int64_t *out_value) {
  // Max digits for int64_t is 19
  if (len == 0 || len > 19) {
    return false;
  }

  // Buffer needs to hold 19 digits + null terminator.
  // Using 24 bytes for safe alignment/padding.
  char buf[24];
  memcpy(buf, str, len);
  buf[len] = '\0';

  char *endptr;

  // Reset errno before call to detect overflow reliably
  errno = 0;

  long long value = strtoll(buf, &endptr, 10);

  // Validation Checks:
  // *endptr != '\0': Means the parser hit a non-digit character before the end.
  // errno == ERANGE: Means the value exceeded UINT64_MAX.
  if (*endptr != '\0' || errno == ERANGE) {
    return false;
  }

  *out_value = (int64_t)value;
  return true;
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
} kw_map[] = {{"and", TOKEN_OP_AND},       {"or", TOKEN_OP_OR},
              {"not", TOKEN_OP_NOT},       {"event", TOKEN_CMD_EVENT},
              {"query", TOKEN_CMD_QUERY},  {"index", TOKEN_CMD_INDEX},
              {"in", TOKEN_KW_IN},         {"id", TOKEN_KW_ID},
              {"entity", TOKEN_KW_ENTITY}, {"cursor", TOKEN_KW_CURSOR},
              {"take", TOKEN_KW_TAKE},     {"where", TOKEN_KW_WHERE},
              {"by", TOKEN_KW_BY},         {"having", TOKEN_KW_HAVING},
              {"count", TOKEN_KW_COUNT},   {"key", TOKEN_KW_KEY}};

// Return tokens from input string.
// TODO: Create an iterator/stream, i.e. get_next_token()
queue_t *tok_tokenize(char *input) {
  int num_tokens = 0;
  size_t i = 0;
  if (!input)
    return NULL;
  size_t input_len = strlen(input);
  if (!input_len || input_len > MAX_COMMAND_LEN)
    return NULL;

  queue_t *q = queue_create();

  while (i < input_len) {
    token_t *t;
    char c = input[i];
    bool has_next_char = i + 1 < input_len;

    if (isspace((unsigned char)c)) {
      i++;
    }

    else if (c == '(') {
      if (!_enqueue(q, &num_tokens, TOKEN_SYM_LPAREN, &t, &i, 1, NULL, 0, 0))
        return NULL;
    }

    else if (c == ')') {
      if (!_enqueue(q, &num_tokens, TOKEN_SYM_RPAREN, &t, &i, 1, NULL, 0, 0))
        return NULL;
    }

    else if (c == '>' && has_next_char && input[i + 1] == '=') {
      if (!_enqueue(q, &num_tokens, TOKEN_OP_GTE, &t, &i, 2, NULL, 0, 0))
        return NULL;
    }

    else if (c == '>') {
      if (!_enqueue(q, &num_tokens, TOKEN_OP_GT, &t, &i, 1, NULL, 0, 0))
        return NULL;
    }

    else if (c == '<' && has_next_char && input[i + 1] == '=') {
      if (!_enqueue(q, &num_tokens, TOKEN_OP_LTE, &t, &i, 2, NULL, 0, 0))
        return NULL;
    }

    else if (c == '<') {
      if (!_enqueue(q, &num_tokens, TOKEN_OP_LT, &t, &i, 1, NULL, 0, 0))
        return NULL;
    }

    else if (c == '=') {
      if (!_enqueue(q, &num_tokens, TOKEN_OP_EQ, &t, &i, 1, NULL, 0, 0))
        return NULL;
    }

    else if (c == '!' && has_next_char && input[i + 1] == '=') {
      if (!_enqueue(q, &num_tokens, TOKEN_OP_NEQ, &t, &i, 2, NULL, 0, 0))
        return NULL;
    }

    else if (c == ':') {
      if (!_enqueue(q, &num_tokens, TOKEN_SYM_COLON, &t, &i, 1, NULL, 0, 0))
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
        if (!_enqueue(q, &num_tokens, TOKEN_LITERAL_STRING, &t, &i, 0, val, 0,
                      len)) {
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
          if (len > INT64_MAX_CHARS)
            return _cleanup_on_err(q);
          int64_t n_val = 0;
          bool parse_r = _parse_int64(val, len, &n_val);
          free(val);
          if (!parse_r || !_enqueue(q, &num_tokens, TOKEN_LITERAL_NUMBER, &t,
                                    &i, 0, NULL, n_val, 0))
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
              if (!_enqueue(q, &num_tokens, kw_map[k].type, &t, &i, 0, NULL, 0,
                            0))
                return NULL;
              matched = true;
              break;
            }
          }
          if (!matched) {
            if (!_enqueue(q, &num_tokens, TOKEN_IDENTIFER, &t, &i, 0,
                          lower_text_value, 0, len)) {
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

  if (queue_empty(q)) {
    queue_destroy(q);
    return NULL;
  }

  return q;
}
