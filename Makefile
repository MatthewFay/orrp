# Makefile for orrp

# --- Compiler and Flags ---
CC = gcc
# Compiler flags
CFLAGS = -Iinclude \
		 -Itests/unity \
		 -Ilib/roaring \
		 -Ilib/lmdb \
		 -Ilib/log.c \
		 -Ilib/libuv/include \
		 -Wall -Wextra -g -O0 # TODO: Seperate release (with optimizations) + debug builds

# LDFLAGS: Linker flags
# Note: LMDB requires the -lrt library for real-time extensions (e.g., mmap) on some systems.
# On some systems (like macOS), this functionality is part of the standard C library
# and -lrt is not needed and can cause "library not found" errors.
LDFLAGS = -Llib/libuv/.libs

# LIBS: Libraries to link against
# -luv: The libuv library
# -lm: Math library
# -lpthread: POSIX threads library (required by libuv)
LIBS = -luv -lm -lpthread

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
LIB_SRCS = \
	lib/roaring/roaring.c \
	lib/lmdb/mdb.c \
	lib/lmdb/midl.c \
	lib/log.c/log.c

# Unity testing framework source
UNITY_SRC = tests/unity/unity.c

# --- BUILD ARTIFACTS ---

# Directories for build artifacts
BIN_DIR = bin
OBJ_DIR = obj

# Object files for the main application (used for the final binary)
APP_OBJS = $(patsubst src/%.c, $(OBJ_DIR)/src/%.o, $(APP_SRCS))
LIB_OBJS = $(patsubst lib/%.c, $(OBJ_DIR)/lib/%.o, $(LIB_SRCS))
OBJS = $(APP_OBJS) $(LIB_OBJS)

# Target executable for the main application
TARGET = $(BIN_DIR)/orrp

# Path to the bundled libuv static library
LIBUV_A = lib/libuv/.libs/libuv.a

# --- BUILD RULES ---

# Default target
all: $(TARGET)

# Rule to create the bin and obj directories
$(BIN_DIR) $(OBJ_DIR):
	@mkdir -p $@

# Build the main application
$(TARGET): $(BIN_DIR) $(OBJS) $(LIBUV_A)
	@echo "==> Linking final executable: $(TARGET)"
	$(CC) $(LDFLAGS) -o $@ $(OBJS) $(LIBS)
	@echo "==> ✅ Build complete! Run with: ./$(TARGET)"

# Rule to build the bundled libuv library
# This rule runs 'make' inside the libuv directory to build its static library.
# It only runs if the target libuv.a doesn't exist or its sources are newer.
$(LIBUV_A):
	@echo "==> Building bundled dependency: libuv"
	@if [ ! -f lib/libuv/configure ]; then \
		(cd lib/libuv && ./autogen.sh); \
	fi
	@if [ ! -f lib/libuv/Makefile ]; then \
		(cd lib/libuv && ./configure); \
	fi
	$(MAKE) -C lib/libuv
	@echo "==> Finished building libuv"


# --- TESTING ---

# HOW TO ADD A NEW TEST:
#
# 1. Let's say you create 'tests/networking/test_server.c'.
#
# 2. First, add its target 'bin/test_server' to the main 'test' rule's dependencies:
#    test: ... bin/test_server
#
# 3. Then, add the command to run it:
#    ./bin/test_server
#
# 4. Finally, copy the rule and modify it for the new test:
#    bin/test_server: tests/networking/test_server.c src/networking/server.c tests/unity/unity.c | $(BIN_DIR)
#      $(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Main 'test' target: builds and runs all listed test executables
test: bin/test_tokenizer bin/test_ast bin/test_parser bin/test_stack bin/test_queue
	@echo "--- Running tokenizer test ---"
	./bin/test_tokenizer
	@echo "--- Running ast test ---"
	./bin/test_ast
	@echo "--- Running parser test ---"
	./bin/test_parser
	@echo "--- Running stack test ---"
	./bin/test_stack
	@echo "--- Running queue test ---"
	./bin/test_queue
	@echo "--- All tests finished ---"

# --- INDIVIDUAL TEST BUILD RULES ---

# Rule to build the tokenizer test executable
bin/test_tokenizer: tests/query/test_tokenizer.c src/query/tokenizer.c src/core/queue.c tests/unity/unity.c | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the ast test executable
bin/test_ast: tests/query/test_ast.c src/query/ast.c tests/unity/unity.c | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the parser test executable
bin/test_parser: tests/query/test_parser.c src/query/parser.c src/core/queue.c src/core/stack.c src/query/ast.c src/query/tokenizer.c tests/unity/unity.c | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the stack test executable
bin/test_stack: tests/core/test_stack.c src/core/stack.c tests/unity/unity.c | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the queue test executable
bin/test_queue: tests/core/test_queue.c src/core/queue.c tests/unity/unity.c | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# --- OBJECT FILE COMPILATION ---

# Rule to compile a source file from the 'src' directory
$(OBJ_DIR)/src/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# Rule to compile a source file from a library directory
$(OBJ_DIR)/lib/%.o: lib/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# --- CLEANUP ---

clean:
	rm -rf $(OBJ_DIR)
	rm -rf $(BIN_DIR)
	@if [ -f lib/libuv/Makefile ]; then \
		$(MAKE) -C lib/libuv clean; \
	fi

# Phony targets
.PHONY: all test clean
