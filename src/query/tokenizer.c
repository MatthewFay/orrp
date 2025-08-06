#include "query/tokenizer.h"
#include <ctype.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const int MAX_TOKENS = 256;
const int MAX_IDENTIFIER_VAL_LEN = 256;
const int MAX_TOTAL_CHARS = 2048;

static token_t *_create_token(token_type type) {
  token_t *t = malloc(sizeof(token_t));
  if (!t)
    return NULL;
  t->type = type;
  t->value = NULL;
  t->next = NULL;
  return t;
}

static void _append(token_t **head, token_t **tail, token_t *t,
                    int *num_tokens) {
  ++(*num_tokens);
  if (!*head) {
    *head = t;
    *tail = t;
  }
  (*tail)->next = t;
  *tail = t;
}

static bool _max_toks(int num_tokens) { return num_tokens >= MAX_TOKENS; }

static bool _id_char(char c) {
  return c == '_' || c == '-' || isalnum((unsigned char)c);
}

// Return tokens from input string.
// TODO: need to improve error handling:
// a) check if token is NULL on create, b) DRY out
token_t *tok_tokenize(char *input) {
  int num_tokens = 0;
  size_t i = 0;
  size_t start = 0;
  size_t end = 0;
  size_t input_len = strlen(input);
  if (!input || !input_len || input_len > MAX_TOTAL_CHARS)
    return NULL;

  token_t *head = NULL;
  token_t *tail = NULL;

  while (i < input_len) {
    char c = input[i];

    if (isspace((unsigned char)c)) {
      i++;
    } else if (c == '(') {
      i++;
      if (_max_toks(num_tokens)) {
        tok_free_tokens(head);
        return NULL;
      }
      token_t *t = _create_token(LPAREN);
      _append(&head, &tail, t, &num_tokens);
    } else if (c == ')') {
      i++;
      if (_max_toks(num_tokens)) {
        tok_free_tokens(head);
        return NULL;
      }
      token_t *t = _create_token(RPAREN);
      _append(&head, &tail, t, &num_tokens);

    } else if (_id_char(c)) {
      if (_max_toks(num_tokens)) {
        tok_free_tokens(head);
        return NULL;
      }
      token_t *t = _create_token(IDENTIFIER);
      _append(&head, &tail, t, &num_tokens);
      start = i;
      end = start + 1;
      while (input[end] && _id_char(input[end])) {
        ++end;
      }
      size_t len = end - start;
      if (len > MAX_IDENTIFIER_VAL_LEN) {
        tok_free_tokens(head);
        return NULL;
      }
      char *id = malloc(sizeof(char) * (len + 1));
      if (!id) {
        tok_free_tokens(head);
        return NULL;
      }
      strncpy(id, &input[start], len);
      id[len] = '\0';
      i = end;

      t->value = id;
    } else {
      // invalid character
      tok_free_tokens(head);
      return NULL;
    }
  }

  return head;
}

void tok_free_tokens(token_t *tokens) {
  if (!tokens)
    return;
  token_t *head = tokens;
  while (head) {
    token_t *tmp = head;
    head = head->next;
    free(tmp->value);
    free(tmp);
  }
}