#ifndef TOKENZ_H
#define TOKENZ_H

#include "core/queue.h"
#include <stdint.h>

extern const int MAX_TOKENS;
extern const int MAX_TEXT_VAL_LEN;
extern const int MAX_NUMBERS_SEQ;
extern const int MAX_TOTAL_CHARS;

typedef enum {
  // --- Commands ---
  TOKEN_CMD_TAG,
  TOKEN_CMD_QUERY,

  // --- Reserved Keywords ---
  TOKEN_KW_IN,
  TOKEN_KW_ID,

  TOKEN_IDENTIFER,

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
  TOKEN_OP_LTE,
  TOKEN_OP_LT,
  TOKEN_SYM_COLON,
  TOKEN_SYM_LPAREN,
  TOKEN_SYM_RPAREN,
  TOKEN_SYM_PLUS
} token_type;

typedef struct token_s {
  token_type type;
  char *text_value;
  uint32_t number_value;
} token_t;

Queue *tok_tokenize(char *input);

void tok_clear_all(Queue *tokens);

void tok_free(token_t *token);

#endif // TOKENZ_H
