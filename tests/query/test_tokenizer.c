#include "query/tokenizer.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// Helper function to advance to the next token and assert its properties
static void assert_next_token(token_t **current_token, token_type expected_type,
                              const char *expected_text,
                              uint32_t expected_number) {
  // Ensure we are not reading past the end of the list
  TEST_ASSERT_NOT_NULL(*current_token);

  // Check the token's type
  TEST_ASSERT_EQUAL(expected_type, (*current_token)->type);

  // If text is expected, compare it
  if (expected_text) {
    TEST_ASSERT_NOT_NULL((*current_token)->text_value);
    TEST_ASSERT_EQUAL_STRING(expected_text, (*current_token)->text_value);
  } else {
    TEST_ASSERT_NULL((*current_token)->text_value);
  }

  // If the token is a number, check its value
  if (expected_type == NUMBER) {
    TEST_ASSERT_EQUAL_UINT32(expected_number, (*current_token)->number_value);
  }

  // Move to the next token for the subsequent check
  *current_token = (*current_token)->next;
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
  token_t *tokens = tok_tokenize("   \t\n   ");
  TEST_ASSERT_NOT_NULL(tokens);
  assert_next_token(&tokens, END, NULL, 0);
  tok_free_tokens(tokens);
}

// Test simple operators and parentheses
void test_tokenize_simple_operators(void) {
  char input[] = "() >= > <= < =";
  token_t *tokens = tok_tokenize(input);
  token_t *current = tokens;

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(&current, LPAREN, NULL, 0);
  assert_next_token(&current, RPAREN, NULL, 0);
  assert_next_token(&current, GTE_OP, NULL, 0);
  assert_next_token(&current, GT_OP, NULL, 0);
  assert_next_token(&current, LTE_OP, NULL, 0);
  assert_next_token(&current, LT_OP, NULL, 0);
  assert_next_token(&current, EQ_OP, NULL, 0);
  assert_next_token(&current, END, NULL, 0);

  // Ensure the list is properly terminated
  TEST_ASSERT_NULL(current);

  tok_free_tokens(tokens);
}

// Test simple text tokens, ensuring they are converted to lowercase
void test_tokenize_simple_text_and_case(void) {
  char input[] = "HeLlO wORLD";
  token_t *tokens = tok_tokenize(input);
  token_t *current = tokens;

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(&current, TEXT, "hello", 0);
  assert_next_token(&current, TEXT, "world", 0);
  assert_next_token(&current, END, NULL, 0);

  tok_free_tokens(tokens);
}

// Test text containing valid special characters
void test_tokenize_text_with_special_chars(void) {
  char input[] = "first-name last_name user-id_1";
  token_t *tokens = tok_tokenize(input);
  token_t *current = tokens;

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(&current, TEXT, "first-name", 0);
  assert_next_token(&current, TEXT, "last_name", 0);
  assert_next_token(&current, TEXT, "user-id_1", 0);
  assert_next_token(&current, END, NULL, 0);

  tok_free_tokens(tokens);
}

// Test simple number tokens
void test_tokenize_simple_numbers(void) {
  char input[] = "123 45678 0";
  token_t *tokens = tok_tokenize(input);
  token_t *current = tokens;

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(&current, NUMBER, NULL, 123);
  assert_next_token(&current, NUMBER, NULL, 45678);
  assert_next_token(&current, NUMBER, NULL, 0);
  assert_next_token(&current, END, NULL, 0);

  tok_free_tokens(tokens);
}

// Test keywords (and, or, not) are identified correctly
void test_tokenize_keywords(void) {
  char input[] = "AND or Not";
  token_t *tokens = tok_tokenize(input);
  token_t *current = tokens;

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(&current, AND_OP, NULL, 0);
  assert_next_token(&current, OR_OP, NULL, 0);
  assert_next_token(&current, NOT_OP, NULL, 0);
  assert_next_token(&current, END, NULL, 0);

  tok_free_tokens(tokens);
}

// Test that substrings of keywords are treated as text
void test_tokenize_keywords_as_substrings(void) {
  char input[] = "sandwiches northern notorized";
  token_t *tokens = tok_tokenize(input);
  token_t *current = tokens;

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(&current, TEXT, "sandwiches", 0);
  assert_next_token(&current, TEXT, "northern", 0);
  assert_next_token(&current, TEXT, "notorized", 0);
  assert_next_token(&current, END, NULL, 0);

  tok_free_tokens(tokens);
}

// Test a more complex, realistic query
void test_tokenize_complex_query(void) {
  char input[] = "(name=John AND age >= 30) OR status=active";
  token_t *tokens = tok_tokenize(input);
  token_t *current = tokens;

  TEST_ASSERT_NOT_NULL(tokens);

  assert_next_token(&current, LPAREN, NULL, 0);
  assert_next_token(&current, TEXT, "name", 0);
  assert_next_token(&current, EQ_OP, NULL, 0);
  assert_next_token(&current, TEXT, "john", 0);
  assert_next_token(&current, AND_OP, NULL, 0);
  assert_next_token(&current, TEXT, "age", 0);
  assert_next_token(&current, GTE_OP, NULL, 0);
  assert_next_token(&current, NUMBER, NULL, 30);
  assert_next_token(&current, RPAREN, NULL, 0);
  assert_next_token(&current, OR_OP, NULL, 0);
  assert_next_token(&current, TEXT, "status", 0);
  assert_next_token(&current, EQ_OP, NULL, 0);
  assert_next_token(&current, TEXT, "active", 0);
  assert_next_token(&current, END, NULL, 0);

  tok_free_tokens(tokens);
}

// Test that an invalid character causes tokenization to fail
void test_tokenize_invalid_character(void) {
  // '$' is not a valid character
  char input[] = "name$value";
  token_t *tokens = tok_tokenize(input);
  TEST_ASSERT_NULL(tokens);
}

// Test edge case where an operator is at the very end of the string
void test_tokenize_operator_at_end_of_string(void) {
  char input[] = "value >";
  token_t *tokens = tok_tokenize(input);
  token_t *current = tokens;

  TEST_ASSERT_NOT_NULL(tokens);
  assert_next_token(&current, TEXT, "value", 0);
  assert_next_token(&current, GT_OP, NULL, 0);
  assert_next_token(&current, END, NULL, 0);

  tok_free_tokens(tokens);
}

// Test number length limits
void test_tokenize_number_length_limits(void) {
  // MAX_NUMBERS_SEQ is 9, so a 9-digit number is fine
  char input_ok[] = "999999999";
  token_t *tokens_ok = tok_tokenize(input_ok);
  TEST_ASSERT_NOT_NULL(tokens_ok);
  tok_free_tokens(tokens_ok);

  // A 10-digit number should fail because it exceeds MAX_NUMBERS_SEQ
  char input_fail[] = "1000000000";
  token_t *tokens_fail = tok_tokenize(input_fail);
  TEST_ASSERT_NULL(tokens_fail);
}

// Test text length limits
void test_tokenize_text_length_limits(void) {
  // This test is a bit slow but necessary for checking boundaries.
  // We create a string exactly at the limit.
  char *long_text_ok = malloc(MAX_TEXT_VAL_LEN + 1);
  TEST_ASSERT_NOT_NULL(long_text_ok);
  memset(long_text_ok, 'a', MAX_TEXT_VAL_LEN);
  long_text_ok[MAX_TEXT_VAL_LEN] = '\0';

  token_t *tokens_ok = tok_tokenize(long_text_ok);
  TEST_ASSERT_NOT_NULL(tokens_ok);
  tok_free_tokens(tokens_ok);
  free(long_text_ok);

  // Now create a string that is one character too long.
  char *long_text_fail = malloc(MAX_TEXT_VAL_LEN + 2);
  TEST_ASSERT_NOT_NULL(long_text_fail);
  memset(long_text_fail, 'b', MAX_TEXT_VAL_LEN + 1);
  long_text_fail[MAX_TEXT_VAL_LEN + 1] = '\0';

  token_t *tokens_fail = tok_tokenize(long_text_fail);
  TEST_ASSERT_NULL(tokens_fail);
  free(long_text_fail);
}

// Test total character limit
void test_tokenize_total_char_limit(void) {
  // Create a string that is one character too long
  char *big_input = malloc(MAX_TOTAL_CHARS + 2);
  TEST_ASSERT_NOT_NULL(big_input);
  memset(big_input, 'a', MAX_TOTAL_CHARS + 1);
  big_input[MAX_TOTAL_CHARS + 1] = '\0';

  TEST_ASSERT_NULL(tok_tokenize(big_input));

  free(big_input);
}

// Main function to run the tests
int main(void) {
  UNITY_BEGIN();

  RUN_TEST(test_tokenize_null_or_empty_input);
  RUN_TEST(test_tokenize_simple_operators);
  RUN_TEST(test_tokenize_simple_text_and_case);
  RUN_TEST(test_tokenize_text_with_special_chars);
  RUN_TEST(test_tokenize_simple_numbers);
  RUN_TEST(test_tokenize_keywords);
  RUN_TEST(test_tokenize_keywords_as_substrings);
  RUN_TEST(test_tokenize_complex_query);
  RUN_TEST(test_tokenize_invalid_character);
  RUN_TEST(test_tokenize_operator_at_end_of_string);
  RUN_TEST(test_tokenize_number_length_limits);
  RUN_TEST(test_tokenize_text_length_limits);
  RUN_TEST(test_tokenize_total_char_limit);

  return UNITY_END();
}
