#ifndef DATA_CONSTANTS_H
#define DATA_CONSTANTS_H

// ------------------------------------------------------------------------
// Shared data constants, e.g., sizes, min/max limits shared across modules
// ------------------------------------------------------------------------

#define SLOT_SIZE 64
#define TAG_UNION_SIZE 1
#define NULL_TERM_SIZE 1

// Length for index that supports both string and numeric values
#define GENERIC_IDX_LEN (SLOT_SIZE - TAG_UNION_SIZE - NULL_TERM_SIZE)
#define MAX_EXT_ENTITY_ID_LEN GENERIC_IDX_LEN
// Max length of a text value, e.g., a string for tag key or tag value
#define MAX_TEXT_VAL_LEN 128

#define INT64_MAX_CHARS 19

#define MAX_COMMAND_LEN 2048
#define MAX_CUSTOM_TAGS 32

#endif