#include "core/data_constants.h"
#include "core/queue.h"
#include "query/tokenizer.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// Helper function to dequeue the next token and assert its properties
static void assert_next_token(Queue *tokens, token_type expected_type,
                              const char *expected_text,
                              uint32_t expected_number) {
  token_t *token = q_dequeue(tokens);
  TEST_ASSERT_NOT_NULL(token);

  // Check the token's type
  TEST_ASSERT_EQUAL(expected_type, token->type);

  // If text is expected, compare it
  if (expected_text) {
    TEST_ASSERT_NOT_NULL(token->text_value);
    TEST_ASSERT_EQUAL_STRING(expected_text, token->text_value);
  } else {
    TEST_ASSERT_NULL(token->text_value);
  }

  // If the token is a number, check its value
  if (expected_type == TOKEN_LITERAL_NUMBER) {
    TEST_ASSERT_EQUAL_INT64(expected_number, token->number_value);
  }

  free(token->text_value);
  free(token);
}

// setUp and tearDown are run before and after each test
void setUp(void) {
  // No-op
}

void tearDown(void) {
  // No-op
}

// Test that NULL, empty, or whitespace-only inputs are handled correctly
void test_tokenize_null_or_empty_input(void) {
  TEST_ASSERT_NULL(tok_tokenize(NULL));
  TEST_ASSERT_NULL(tok_tokenize(""));
  TEST_ASSERT_NULL(tok_tokenize("   \t\n   "));
}

// Test simple operators and parentheses
void test_tokenize_simple_operators(void) {
  char input[] = "() >= > <= < = :";
  Queue *tokens = tok_tokenize(input);

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(tokens, TOKEN_SYM_LPAREN, NULL, 0);
  assert_next_token(tokens, TOKEN_SYM_RPAREN, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_GTE, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_GT, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_LTE, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_LT, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_EQ, NULL, 0);
  assert_next_token(tokens, TOKEN_SYM_COLON, NULL, 0);

  // Ensure the queue is properly terminated
  TEST_ASSERT_TRUE(q_empty(tokens));

  tok_clear_all(tokens);
  q_destroy(tokens);
}

// Test simple identifier tokens, ensuring they are converted to lowercase
void test_tokenize_simple_identifier_and_case(void) {
  char input[] = "HeLlO wORLD";
  Queue *tokens = tok_tokenize(input);

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(tokens, TOKEN_IDENTIFER, "hello", 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "world", 0);

  tok_clear_all(tokens);
  q_destroy(tokens);
}

// Test identifier containing valid special characters
void test_tokenize_identifier_with_special_chars(void) {
  char input[] = "first-name last_name user-id_1";
  Queue *tokens = tok_tokenize(input);

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(tokens, TOKEN_IDENTIFER, "first-name", 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "last_name", 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "user-id_1", 0);

  tok_clear_all(tokens);
  q_destroy(tokens);
}

// Test simple number tokens
void test_tokenize_simple_numbers(void) {
  char input[] = "123 45678 0";
  Queue *tokens = tok_tokenize(input);

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(tokens, TOKEN_LITERAL_NUMBER, NULL, 123);
  assert_next_token(tokens, TOKEN_LITERAL_NUMBER, NULL, 45678);
  assert_next_token(tokens, TOKEN_LITERAL_NUMBER, NULL, 0);

  tok_clear_all(tokens);
  q_destroy(tokens);
}

// Test keywords (and, or, not, event, query, in, id) are identified correctly
void test_tokenize_keywords(void) {
  char input[] = "AND or Not event query in id";
  Queue *tokens = tok_tokenize(input);

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(tokens, TOKEN_OP_AND, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_OR, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_NOT, NULL, 0);
  assert_next_token(tokens, TOKEN_CMD_EVENT, NULL, 0);
  assert_next_token(tokens, TOKEN_CMD_QUERY, NULL, 0);
  assert_next_token(tokens, TOKEN_KW_IN, NULL, 0);
  assert_next_token(tokens, TOKEN_KW_ID, NULL, 0);

  tok_clear_all(tokens);
  q_destroy(tokens);
}

// Test that substrings of keywords are treated as identifiers
void test_tokenize_keywords_as_substrings(void) {
  char input[] = "sandwiches northern notorized additional queryable";
  Queue *tokens = tok_tokenize(input);

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(tokens, TOKEN_IDENTIFER, "sandwiches", 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "northern", 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "notorized", 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "additional", 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "queryable", 0);

  tok_clear_all(tokens);
  q_destroy(tokens);
}

// Test a more complex, realistic query
void test_tokenize_complex_query(void) {
  char input[] = "(name=John AND age >= 30) OR status=active";
  Queue *tokens = tok_tokenize(input);

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(tokens, TOKEN_SYM_LPAREN, NULL, 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "name", 0);
  assert_next_token(tokens, TOKEN_OP_EQ, NULL, 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "john", 0);
  assert_next_token(tokens, TOKEN_OP_AND, NULL, 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "age", 0);
  assert_next_token(tokens, TOKEN_OP_GTE, NULL, 0);
  assert_next_token(tokens, TOKEN_LITERAL_NUMBER, NULL, 30);
  assert_next_token(tokens, TOKEN_SYM_RPAREN, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_OR, NULL, 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "status", 0);
  assert_next_token(tokens, TOKEN_OP_EQ, NULL, 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "active", 0);

  tok_clear_all(tokens);
  q_destroy(tokens);
}

// Test that an invalid character causes tokenization to fail
void test_tokenize_invalid_character(void) {
  // '$' is not a valid character
  char input[] = "name$value";
  Queue *tokens = tok_tokenize(input);
  TEST_ASSERT_NULL(tokens);
}

// Test edge case where an operator is at the very end of the string
void test_tokenize_operator_at_end_of_string(void) {
  char input[] = "value >";
  Queue *tokens = tok_tokenize(input);

  TEST_ASSERT_NOT_NULL(tokens);
  assert_next_token(tokens, TOKEN_IDENTIFER, "value", 0);
  assert_next_token(tokens, TOKEN_OP_GT, NULL, 0);

  tok_clear_all(tokens);
  q_destroy(tokens);
}

// Test number length limits
void test_tokenize_number_length_limits(void) {
  // MAX_NUMBERS_SEQ is 9, so a 9-digit number is fine
  char input_ok[] = "999999999";
  Queue *tokens_ok = tok_tokenize(input_ok);
  TEST_ASSERT_NOT_NULL(tokens_ok);
  assert_next_token(tokens_ok, TOKEN_LITERAL_NUMBER, NULL, 999999999);
  tok_clear_all(tokens_ok);
  q_destroy(tokens_ok);

  // A 10-digit number should fail because it exceeds MAX_NUMBERS_SEQ
  char input_fail[] = "1000000000";
  Queue *tokens_fail = tok_tokenize(input_fail);
  TEST_ASSERT_NULL(tokens_fail);
}

// Test identifier length limits
void test_tokenize_identifier_length_limits(void) {
  // This test is a bit slow but necessary for checking boundaries.
  // We create a string exactly at the limit.
  char *long_text_ok = malloc(MAX_TEXT_VAL_LEN + 1);
  TEST_ASSERT_NOT_NULL(long_text_ok);
  memset(long_text_ok, 'a', MAX_TEXT_VAL_LEN);
  long_text_ok[MAX_TEXT_VAL_LEN] = '\0';

  Queue *tokens_ok = tok_tokenize(long_text_ok);
  TEST_ASSERT_NOT_NULL(tokens_ok);
  assert_next_token(tokens_ok, TOKEN_IDENTIFER, long_text_ok, 0);
  tok_clear_all(tokens_ok);
  q_destroy(tokens_ok);

  free(long_text_ok);

  // Now create a string that is one character too long.
  char *long_text_fail = malloc(MAX_TEXT_VAL_LEN + 2);
  TEST_ASSERT_NOT_NULL(long_text_fail);
  memset(long_text_fail, 'b', MAX_TEXT_VAL_LEN + 1);
  long_text_fail[MAX_TEXT_VAL_LEN + 1] = '\0';

  Queue *tokens_fail = tok_tokenize(long_text_fail);
  TEST_ASSERT_NULL(tokens_fail);
  free(long_text_fail);
}

// Test total character limit
void test_tokenize_total_char_limit(void) {
  // Create a string that is one character too long
  char *big_input = malloc(MAX_COMMAND_LEN + 2);
  TEST_ASSERT_NOT_NULL(big_input);
  memset(big_input, 'a', MAX_COMMAND_LEN + 1);
  big_input[MAX_COMMAND_LEN + 1] = '\0';

  TEST_ASSERT_NULL(tok_tokenize(big_input));

  free(big_input);
}

// Test quoted string literals, including edge cases
void test_tokenize_quoted_strings(void) {
  char input[] = "\"Hello World\" \"CaseSensitive\"";
  Queue *tokens = tok_tokenize(input);
  TEST_ASSERT_NOT_NULL(tokens);
  assert_next_token(tokens, TOKEN_LITERAL_STRING, "Hello World", 0);
  assert_next_token(tokens, TOKEN_LITERAL_STRING, "CaseSensitive", 0);
  tok_clear_all(tokens);
  q_destroy(tokens);

  // Unclosed quote should fail
  char input2[] = "\"unterminated";
  Queue *tokens2 = tok_tokenize(input2);
  TEST_ASSERT_NULL(tokens2);

  // Escapes are not allowed: quoted string with a backslash is invalid
  char input3[] = "\"Hello\\World\"";
  Queue *tokens3 = tok_tokenize(input3);
  TEST_ASSERT_NULL(tokens3);

  // Quoted string with a quote inside is invalid
  char input4[] = "\"Hello\"World\"";
  Queue *tokens4 = tok_tokenize(input4);
  TEST_ASSERT_NULL(tokens4);
}

// Test mix of all token types in one input
void test_tokenize_all_token_types(void) {
  char input[] = "event in id ( ) : \"str\" 42 and or not query >= > <= < = "
                 "!= identifier";
  Queue *tokens = tok_tokenize(input);
  TEST_ASSERT_NOT_NULL(tokens);
  assert_next_token(tokens, TOKEN_CMD_EVENT, NULL, 0);
  assert_next_token(tokens, TOKEN_KW_IN, NULL, 0);
  assert_next_token(tokens, TOKEN_KW_ID, NULL, 0);
  assert_next_token(tokens, TOKEN_SYM_LPAREN, NULL, 0);
  assert_next_token(tokens, TOKEN_SYM_RPAREN, NULL, 0);
  assert_next_token(tokens, TOKEN_SYM_COLON, NULL, 0);
  assert_next_token(tokens, TOKEN_LITERAL_STRING, "str", 0);
  assert_next_token(tokens, TOKEN_LITERAL_NUMBER, NULL, 42);
  assert_next_token(tokens, TOKEN_OP_AND, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_OR, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_NOT, NULL, 0);
  assert_next_token(tokens, TOKEN_CMD_QUERY, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_GTE, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_GT, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_LTE, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_LT, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_EQ, NULL, 0);
  assert_next_token(tokens, TOKEN_OP_NEQ, NULL, 0);
  assert_next_token(tokens, TOKEN_IDENTIFER, "identifier", 0);
  tok_clear_all(tokens);
  q_destroy(tokens);
}

// Test that a quoted string with only digits is not treated as a number
void test_tokenize_quoted_digits(void) {
  char input[] = "\"12345\"";
  Queue *tokens = tok_tokenize(input);
  TEST_ASSERT_NOT_NULL(tokens);
  assert_next_token(tokens, TOKEN_LITERAL_STRING, "12345", 0);
  tok_clear_all(tokens);
  q_destroy(tokens);
}

// Test that a string with only whitespace is ignored
void test_tokenize_whitespace_only(void) {
  char input[] = "   \t\n   ";
  Queue *tokens = tok_tokenize(input);
  TEST_ASSERT_NULL(tokens);
}

// Main function to run the tests
int main(void) {
  UNITY_BEGIN();

  RUN_TEST(test_tokenize_null_or_empty_input);
  RUN_TEST(test_tokenize_simple_operators);
  RUN_TEST(test_tokenize_simple_identifier_and_case);
  RUN_TEST(test_tokenize_identifier_with_special_chars);
  RUN_TEST(test_tokenize_simple_numbers);
  RUN_TEST(test_tokenize_keywords);
  RUN_TEST(test_tokenize_keywords_as_substrings);
  RUN_TEST(test_tokenize_complex_query);
  RUN_TEST(test_tokenize_invalid_character);
  RUN_TEST(test_tokenize_operator_at_end_of_string);
  RUN_TEST(test_tokenize_number_length_limits);
  RUN_TEST(test_tokenize_identifier_length_limits);
  RUN_TEST(test_tokenize_total_char_limit);
  RUN_TEST(test_tokenize_quoted_strings);
  RUN_TEST(test_tokenize_all_token_types);
  RUN_TEST(test_tokenize_quoted_digits);
  RUN_TEST(test_tokenize_whitespace_only);

  return UNITY_END();
}
