#ifndef TOKENZ_H
#define TOKENZ_H

#include "core/queue.h"
#include <stddef.h>
#include <stdint.h>

typedef enum {
  // --- Commands ---
  TOKEN_CMD_EVENT,
  TOKEN_CMD_QUERY,
  TOKEN_CMD_INDEX,

  // --- Reserved Keywords ---
  TOKEN_KW_IN,
  TOKEN_KW_ID, // Future: event id, for idempotency
  TOKEN_KW_ENTITY,
  TOKEN_KW_TAKE,
  TOKEN_KW_CURSOR,
  TOKEN_KW_WHERE,
  TOKEN_KW_BY,
  TOKEN_KW_HAVING,
  TOKEN_KW_COUNT,
  TOKEN_KW_KEY,

  TOKEN_IDENTIFER, // unquoted text

  // --- Literals (Values) ---
  TOKEN_LITERAL_STRING,
  TOKEN_LITERAL_NUMBER,

  // --- Operators & Symbols ---
  TOKEN_OP_AND,
  TOKEN_OP_OR,
  TOKEN_OP_NOT,
  TOKEN_OP_GT,
  TOKEN_OP_GTE,
  TOKEN_OP_EQ,
  TOKEN_OP_NEQ,
  TOKEN_OP_LTE,
  TOKEN_OP_LT,
  TOKEN_SYM_COLON,
  TOKEN_SYM_LPAREN,
  TOKEN_SYM_RPAREN
} token_type;

typedef struct token_s {
  token_type type;
  char *text_value;
  size_t text_value_len;
  int64_t number_value;
} token_t;

queue_t *tok_tokenize(char *input);

// Frees all tokens in queue. Does not free queue itself.
void tok_clear_all(queue_t *tokens);

void tok_free(token_t *token);

#endif // TOKENZ_H
