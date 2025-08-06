#ifndef TOKENZ_H
#define TOKENZ_H

typedef enum { IDENTIFIER, LPAREN, RPAREN } token_type;

typedef struct token_s {
  token_type type;
  char *value;
  struct token_s *next;
} token_t;

token_t *tok_tokenize(char *input);

void tok_free_tokens(token_t *tokens);

#endif // TOKENZ_H
