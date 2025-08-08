# Compiler and flags
CC = gcc
CFLAGS = -Iinclude -Itests/unity -Isrc/lib/roaring -Isrc/lib/lmdb -Wall -Wextra -g
# LMDB requires the -lrt library for real-time extensions (e.g., mmap) on some systems.
# On some systems (like macOS), this functionality is part of the standard C library
# and -lrt is not needed and can cause "library not found" errors.
LDFLAGS =

# --- SOURCE FILES ---

# Main application sources
APP_SRCS = src/main.c \
			 src/api.c \
			 src/engine.c \
			 src/core/bitmaps.c \
			 src/core/db.c \
			 src/core/stack.c \
			 src/core/queue.c \
			 src/networking/server.c \
			 src/query/ast.c \
			 src/query/tokenizer.c \
			 src/query/parser.c

# Library sources
# Library sources
LIB_SRCS = \
	src/lib/roaring/roaring.c \
	src/lib/lmdb/mdb.c \
	src/lib/lmdb/midl.c

# Unity testing framework source
UNITY_SRC = tests/unity/unity.c

# --- BUILD ARTIFACTS ---

# Directories for build artifacts
BIN_DIR = bin
OBJ_DIR = obj

# Object files for the main application (used for the final binary)
APP_OBJS = $(patsubst src/%.c, $(OBJ_DIR)/src/%.o, $(APP_SRCS))
LIB_OBJS = $(patsubst src/lib/%.c, $(OBJ_DIR)/lib/%.o, $(LIB_SRCS))
OBJS = $(APP_OBJS) $(LIB_OBJS)

# Target executable for the main application
TARGET = $(BIN_DIR)/orrp

# --- BUILD RULES ---

# Default target
all: $(TARGET)

# Rule to create the bin and obj directories
$(BIN_DIR) $(OBJ_DIR):
	@mkdir -p $@

# Build the main application
$(TARGET): $(BIN_DIR) $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $(OBJS)

# HOW TO ADD A NEW TEST:
#
# 1. Let's say you create 'tests/networking/test_server.c'.
#
# 2. First, add its target 'bin/test_server' to the main 'test' rule's dependencies:
#    test: ... bin/test_server
#
# 3. Then, add the command to run it:
#	   ./bin/test_server
#
# 4. Finally, copy the rule and modify it for the new test:
#    bin/test_server: tests/networking/test_server.c src/networking/server.c tests/unity/unity.c | $(BIN_DIR)
#	     $(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

# Main 'test' target: builds and runs all listed test executables
test: bin/test_tokenizer bin/test_stack
	@echo "--- Running tokenizer test ---"
	./bin/test_tokenizer
	@echo "--- Running stack test ---"
	./bin/test_stack
	@echo "--- All tests finished ---"

# --- INDIVIDUAL TEST BUILD RULES ---

# Rule to build the tokenizer test executable
bin/test_tokenizer: tests/query/test_tokenizer.c src/query/tokenizer.c src/core/queue.c tests/unity/unity.c | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

# Rule to build the stack test executable
bin/test_stack: tests/core/test_stack.c src/core/stack.c tests/unity/unity.c | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

# --- OBJECT FILE COMPILATION ---

# Rule to compile a source file from the 'src' directory
$(OBJ_DIR)/src/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# Rule to compile a source file from a library directory
$(OBJ_DIR)/lib/%.o: src/lib/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# --- CLEANUP ---

clean:
	rm -rf $(OBJ_DIR)
	rm -rf $(BIN_DIR)

# Phony targets
.PHONY: all test clean
