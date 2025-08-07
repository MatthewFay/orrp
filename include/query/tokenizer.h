#ifndef TOKENZ_H
#define TOKENZ_H

#include <stdint.h>

extern const int MAX_TOKENS;
extern const int MAX_TEXT_VAL_LEN;
extern const int MAX_NUMBERS_SEQ;
extern const int MAX_TOTAL_CHARS;

typedef enum {
  TEXT,
  NUMBER,
  LPAREN,
  RPAREN,
  AND_OP,
  OR_OP,
  NOT_OP,
  GT_OP,
  LT_OP,
  GTE_OP,
  LTE_OP,
  EQ_OP,
  END
} token_type;

typedef struct token_s {
  token_type type;
  char *text_value;
  uint32_t number_value;
  struct token_s *next;
} token_t;

token_t *tok_tokenize(char *input);

void tok_free_tokens(token_t *tokens);

#endif // TOKENZ_H
