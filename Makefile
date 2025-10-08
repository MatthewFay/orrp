# Makefile for orrp

# --- Compiler and Flags ---
CC = gcc
# Compiler flags
# TODO: Seperate release (with optimizations) and dev builds
CFLAGS = -Iinclude \
	 -Isrc \
	 -Itests/unity \
	 -Ilib/roaring \
	 -Ilib/lmdb \
	 -Ilib/log.c \
	 -Ilib/libuv/include \
	 -Ilib/uthash \
	 -Ilib/ck/include \
	 -Wall -Wextra -g -std=c11 \
	 -O0 # No optimizations currently for development purposes

# LDFLAGS: Linker flags
LDFLAGS = -Llib/libuv/.libs

# LIBS: Libraries to link against
# -luv: The libuv library
# -lm: Math library
# -lpthread: POSIX threads library (required by libuv and ck)
# Note: LMDB and ck require the -lrt library for real-time extensions (e.g., mmap) on some systems.
# On some systems (like macOS), this functionality is part of the standard C library
# and -lrt is not needed and can cause "library not found" errors.
LIBS = -luv -lm -lpthread

# --- SOURCE FILES ---

# Main application sources
APP_SRCS = \
		   src/core/bitmaps.c \
			 src/core/conversions.c \
		   src/core/db.c \
			 src/core/hash.c \
			 src/core/lock_striped_ht.c \
			 src/core/queue.c \
		   src/core/stack.c \
			 src/engine/cmd_context/cmd_context.c \
			 src/engine/cmd_queue/cmd_queue_msg.c \
			 src/engine/cmd_queue/cmd_queue.c \
			 src/engine/consumer/consumer_cache_entry.c \
			 src/engine/consumer/consumer_cache_internal.c \
			 src/engine/consumer/consumer_cache.c \
			 src/engine/consumer/consumer_ebr.c \
			 src/engine/consumer/consumer.c \
			 src/engine/container/container.c \
			 src/engine/context/context.c \
			 src/engine/dc_cache/dc_cache.c \
			 src/engine/eng_key_format/eng_key_format.c \
			 src/engine/engine_writer/engine_writer_queue_msg.c \
			 src/engine/engine_writer/engine_writer_queue.c \
			 src/engine/engine_writer/engine_writer.c \
			 src/engine/op/op.c \
			 src/engine/op_queue/op_queue_msg.c \
			 src/engine/op_queue/op_queue.c \
			 src/engine/worker/worker_ops.c \
			 src/engine/worker/worker.c \
			 src/engine/api.c \
			 src/engine/engine.c \
		   src/networking/server.c \
		   src/query/ast.c \
			 src/query/parser.c \
		   src/query/tokenizer.c \
			 src/main.c

# excluding main.c for tests
TEST_APP_SRCS = $(filter-out src/main.c, $(APP_SRCS))

# Library sources
LIB_SRCS = \
  $(wildcard lib/roaring/*.c) \
  $(wildcard lib/lmdb/*.c) \
  $(wildcard lib/log.c/*.c)

# Unity testing framework source
UNITY_SRC = tests/unity/unity.c

# --- BUILD ARTIFACTS ---

# Directories for build artifacts
BIN_DIR = bin
OBJ_DIR = obj

# Combine all application and library source files into one list
ALL_SRCS = $(APP_SRCS) $(LIB_SRCS)

# Automatically generate all object file paths by replacing the extension
# and prepending the object directory. This handles any subdirectory.
OBJS = $(patsubst %.c,$(OBJ_DIR)/%.o,$(ALL_SRCS))

# Target executable for the main application
TARGET = $(BIN_DIR)/orrp

# Path to the bundled libuv static library
LIBUV_A = lib/libuv/.libs/libuv.a

# Path to the bundled Concurrency Kit static library
LIBCK_A = lib/ck/src/libck.a

# --- BUILD RULES ---

# Default target
all: $(TARGET)

# Rule to create the bin and obj directories
$(BIN_DIR) $(OBJ_DIR):
	@mkdir -p $@

# Build the main application
$(TARGET): $(BIN_DIR) $(OBJS) $(LIBUV_A) $(LIBCK_A)
	@echo "==> Linking final executable: $(TARGET)"
	$(CC) $(LDFLAGS) -o $@ $(OBJS) $(LIBCK_A) $(LIBS)
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

# Rule to build Concurrency Kit (ck)
$(LIBCK_A):
	@echo "==> Building bundled dependency: Concurrency Kit"
	@if [ ! -f lib/ck/configure ]; then \
		(echo "Error: Concurrency Kit config not found."; exit 1); \
	fi
	@if [ ! -f lib/ck/Makefile ]; then \
		(cd lib/ck && ./configure); \
	fi
	$(MAKE) -C lib/ck
	@echo "==> Finished building Concurrency Kit"

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
#    bin/test_server: tests/networking/test_server.c $(TEST_APP_SRCS) ${UNITY_SRC} | $(BIN_DIR)
#      $(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Main 'test' target: builds and runs all listed test executables
test: bin/test_bitmaps \
			bin/test_conversions \
			bin/test_db \
			bin/test_hash \
			bin/test_lock_striped_ht \
			bin/test_queue \
			bin/test_stack \
			bin/test_api \
			bin/test_cmd_context \
			bin/test_tokenizer \
 		  bin/test_ast \
			bin/test_parser \
			bin/test_event_api
	@echo "--- Running bitmaps test ---"
	./bin/test_bitmaps
	@echo "--- Running conversions test ---"
	./bin/test_conversions
	@echo "--- Running db test ---"
	./bin/test_db
	@echo "--- Running hash test ---"
	./bin/test_hash
	@echo "--- Running lock_striped_ht test ---"
	./bin/test_lock_striped_ht
	@echo "--- Running queue test ---"
	./bin/test_queue
	@echo "--- Running stack test ---"
	./bin/test_stack

	@echo "--- Running api test ---"
	./bin/test_api
	@echo "--- Running cmd_context test ---"
	./bin/test_cmd_context

	@echo "--- Running ast test ---"
	./bin/test_ast
	@echo "--- Running parser test ---"
	./bin/test_parser
	@echo "--- Running tokenizer test ---"
	./bin/test_tokenizer

	@echo "--- Running integration test: event api ---"
	./bin/test_event_api
	@echo "--- All tests finished successfully ---"

# 'test_build' target: builds all test executables
test_build: bin/test_bitmaps \
						bin/test_conversions \
						bin/test_db \
						bin/test_hash \
						bin/test_lock_striped_ht \
						bin/test_queue \
						bin/test_stack \
						bin/test_api \
						bin/test_cmd_context \
					  bin/test_ast \
					  bin/test_parser \
						bin/test_tokenizer \
						bin/test_event_api

# --- INDIVIDUAL TEST BUILD RULES ---

# Rule to build the bitmaps test executable
bin/test_bitmaps: 	tests/core/test_bitmaps.c \
										src/core/bitmaps.c \
										lib/roaring/roaring.c \
										${UNITY_SRC} | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the conversions test executable
bin/test_conversions: 	tests/core/test_conversions.c \
										src/core/conversions.c \
										${UNITY_SRC} | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the db test executable
bin/test_db: 	tests/core/test_db.c \
										src/core/db.c \
										$(wildcard lib/lmdb/*.c) \
										${UNITY_SRC} | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the hash test executable
bin/test_hash: 	tests/core/test_hash.c \
										src/core/hash.c \
										${UNITY_SRC} | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the lock_striped_ht test executable
bin/test_lock_striped_ht: 	tests/core/test_lock_striped_ht.c \
										src/core/lock_striped_ht.c \
										src/core/hash.c \
										${UNITY_SRC} | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS) $(LIBCK_A)

# Rule to build the queue test executable
bin/test_queue: tests/core/test_queue.c \
								src/core/queue.c \
								${UNITY_SRC} | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the stack test executable
bin/test_stack: tests/core/test_stack.c \
							  src/core/stack.c \
								${UNITY_SRC} | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the api test executable
bin/test_api: tests/engine/test_api.c \
							src/engine/api.c \
							src/query/ast.c \
							${UNITY_SRC} | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the cmd_context test executable
bin/test_cmd_context: tests/engine/test_cmd_context.c \
							src/engine/cmd_context/cmd_context.c \
							src/query/ast.c \
							${UNITY_SRC} | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the ast test executable
bin/test_ast: tests/query/test_ast.c \
              src/query/ast.c \
							${UNITY_SRC} | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the parser test executable
bin/test_parser: tests/query/test_parser.c \
 							   src/query/parser.c \
								 src/core/queue.c \
								 src/core/stack.c \
								 src/query/ast.c \
								 src/query/tokenizer.c \
								 src/core/conversions.c \
								 ${UNITY_SRC} | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# Rule to build the tokenizer test executable
bin/test_tokenizer: tests/query/test_tokenizer.c \
									  src/query/tokenizer.c \
										src/core/queue.c \
										${UNITY_SRC} | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

### --- INTEGRATION TESTS --- ###

# Rule to build the event api test executable
bin/test_event_api: tests/integration/test_event_api.c \
  $(TEST_APP_SRCS) ${UNITY_SRC} $(LIB_SRCS) | $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS) $(LIBCK_A)

# --- OBJECT FILE COMPILATION ---

# Generic rule to compile any .c file into its corresponding .o file
# inside the object directory, preserving the path.
# e.g., src/engine/api.c -> obj/src/engine/api.o
# e.g., lib/ck/src/ck_pr.c -> obj/lib/ck/src/ck_pr.o
$(OBJ_DIR)/%.o: %.c
	@echo "==> Compiling $<"
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# --- CLEANUP ---

clean:
	rm -rf $(OBJ_DIR)
	rm -rf $(BIN_DIR)
	@if [ -f lib/libuv/Makefile ]; then \
		$(MAKE) -C lib/libuv clean; \
	fi
	@if [ -f lib/ck/Makefile ]; then \
    $(MAKE) -C lib/ck clean; \
  fi


bear:
	bear -- make
	bear --append -- make test_build

# Phony targets
.PHONY: all test clean

