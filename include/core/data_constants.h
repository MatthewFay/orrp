#ifndef DATA_CONSTANTS_H
#define DATA_CONSTANTS_H

// ------------------------------------------------------------------------
// Shared data constants, e.g., sizes, min/max limits shared across modules
// ------------------------------------------------------------------------

#define SLOT_SIZE 64
#define TAG_UNION_SIZE 1
#define NULL_TERM_SIZE 1

// Tagged Union Discriminators (First byte of the slot)
#define VAL_TYPE_STR 0x01
#define VAL_TYPE_I64 0x02

// Length for index that supports both string and numeric values
#define GENERIC_IDX_LEN (SLOT_SIZE - TAG_UNION_SIZE - NULL_TERM_SIZE)
#define MAX_ENTITY_STR_LEN GENERIC_IDX_LEN
// Max length of a text value, e.g., a string for tag key or tag value
#define MAX_TEXT_VAL_LEN 128

#define INT64_MAX_CHARS 19

#define MAX_COMMAND_LEN 2048
#define MAX_CUSTOM_TAGS 32

#define MAX_CONTAINER_PATH_LENGTH 128

#define ONE_GIBIBYTE (1024UL * 1024UL * 1024UL)

#define MAX_CONTAINER_SIZE ONE_GIBIBYTE

#define MAX_NUM_INDEXES 32

#endif