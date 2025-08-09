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

static token_t *_create_token(token_type type, char *text_value,
                              uint32_t number_value) {
  token_t *t = malloc(sizeof(token_t));
  if (!t)
    return NULL;
  t->type = type;
  t->text_value = text_value;
  t->number_value = number_value;
  return t;
}

static bool _max_toks(int num_tokens) { return num_tokens >= MAX_TOKENS; }

static bool _valid_char(char c) {
  return c == '_' || c == '-' || isalnum((unsigned char)c);
}

static bool _enqueue(Queue *q, int *num_tokens, token_type type,
                     token_t **out_token, size_t *i, int incr_i,
                     char *text_value, uint32_t number_value) {
  if (_max_toks(*num_tokens)) {
    tok_free_tokens(q);
    return false;
  }
  *out_token = _create_token(type, text_value, number_value);
  if (!*out_token) {
    tok_free_tokens(q);
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

// Return tokens from input string.
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
      if (!_enqueue(q, &num_tokens, LPAREN, &t, &i, 1, NULL, 0))
        return NULL;
    }

    else if (c == ')') {
      if (!_enqueue(q, &num_tokens, RPAREN, &t, &i, 1, NULL, 0))
        return NULL;
    }

    else if (c == '>' && has_next_char && input[i + 1] == '=') {
      if (!_enqueue(q, &num_tokens, GTE_OP, &t, &i, 2, NULL, 0))
        return NULL;
    }

    else if (c == '>') {
      if (!_enqueue(q, &num_tokens, GT_OP, &t, &i, 1, NULL, 0))
        return NULL;
    }

    else if (c == '<' && has_next_char && input[i + 1] == '=') {
      if (!_enqueue(q, &num_tokens, LTE_OP, &t, &i, 2, NULL, 0))
        return NULL;
    }

    else if (c == '<') {
      if (!_enqueue(q, &num_tokens, LT_OP, &t, &i, 1, NULL, 0))
        return NULL;
    }

    else if (c == '=') {
      if (!_enqueue(q, &num_tokens, EQ_OP, &t, &i, 1, NULL, 0))
        return NULL;
    }

    else if (_valid_char(c)) {

      start = i;
      end = start;
      bool all_digits = true;
      while (input[end] && _valid_char(input[end])) {
        if (!isdigit((unsigned char)input[end]))
          all_digits = false;
        ++end;
      }
      size_t len = end - start;
      if (len > MAX_TEXT_VAL_LEN || (all_digits && len > MAX_NUMBERS_SEQ)) {
        tok_free_tokens(q);
        return NULL;
      }
      char *val = malloc(sizeof(char) * (len + 1));
      if (!val) {
        tok_free_tokens(q);
        return NULL;
      }
      strncpy(val, &input[start], len);
      val[len] = '\0';

      if (all_digits) {
        uint32_t n_val = _parse_uint32(val, len);
        free(val);
        if (!_enqueue(q, &num_tokens, NUMBER, &t, &i, 0, NULL, n_val))
          return NULL;
      } else {
        char *lower_text_value = malloc(len + 1);
        if (!lower_text_value) {
          tok_free_tokens(q);
          free(val);
          return NULL;
        }
        _to_lowercase(lower_text_value, val, len + 1);
        free(val);
        if (strcmp(lower_text_value, "and") == 0) {
          free(lower_text_value);
          if (!_enqueue(q, &num_tokens, AND_OP, &t, &i, 0, NULL, 0))
            return NULL;
        } else if (strcmp(lower_text_value, "or") == 0) {
          free(lower_text_value);
          if (!_enqueue(q, &num_tokens, OR_OP, &t, &i, 0, NULL, 0))
            return NULL;
        } else if (strcmp(lower_text_value, "not") == 0) {
          free(lower_text_value);
          if (!_enqueue(q, &num_tokens, NOT_OP, &t, &i, 0, NULL, 0))
            return NULL;
        } else {
          if (!_enqueue(q, &num_tokens, TEXT, &t, &i, 0, lower_text_value, 0)) {
            free(lower_text_value);
            return NULL;
          }
        }
      }

      i = end;

    }

    else {
      // invalid character
      tok_free_tokens(q);
      return NULL;
    }
  }

  if (q_empty(q)) {
    q_destroy(q);
    return NULL;
  }

  return q;
}

void tok_free_tokens(Queue *tokens) {
  if (!tokens)
    return;
  token_t *t;
  while (!q_empty(tokens)) {
    t = q_dequeue(tokens);
    tok_free(t);
  }
  q_destroy(tokens);
}

void tok_free(token_t *token) {
  free(token->text_value);
  free(token);
}