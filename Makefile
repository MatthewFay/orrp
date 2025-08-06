# Compiler and flags
CC = gcc
CFLAGS = -Iinclude -Itests/unity -Wall -Wextra -g
# Add include paths for libraries
CFLAGS += -Isrc/lib/roaring -Isrc/lib/lmdb
# LMDB requires the -lrt library for real-time extensions (e.g., mmap) on some systems.
# On some systems (like macOS), this functionality is part of the standard C library
# and -lrt is not needed and can cause "library not found" errors.
LDFLAGS =

# Source files
# Main application sources
APP_SRCS = src/main.c src/api.c src/engine.c src/core/bitmaps.c \
src/core/db.c src/networking/server.c \
src/query/tokenizer.c src/query/parser.c

# Library sources 
LIB_SRCS = src/lib/roaring/roaring.c src/lib/lmdb/mdb.c src/lib/lmdb/midl.c
# Combine all source files
SRCS = $(APP_SRCS) $(LIB_SRCS)

# Directories for build artifacts
BIN_DIR = bin
OBJ_DIR = obj

# Object files for application sources
APP_OBJS = $(patsubst src/%.c, $(OBJ_DIR)/src/%.o, $(APP_SRCS))
# Object files for library sources
LIB_OBJS = $(patsubst src/lib/%.c, $(OBJ_DIR)/lib/%.o, $(LIB_SRCS))
# Combine all object files
OBJS = $(APP_OBJS) $(LIB_OBJS)

# Test source files
TEST_SRCS = tests/test_runner.c tests/test_main.c tests/unity/unity.c
# Test object files also go into 'obj/tests/'
TEST_OBJS = $(patsubst tests/%.c, $(OBJ_DIR)/tests/%.o, $(TEST_SRCS))


# Target executables
TARGET = $(BIN_DIR)/orrp
TEST_TARGET = $(BIN_DIR)/test_runner

# Default target
all: $(TARGET)

# Rule to create the bin and obj directories
$(BIN_DIR):
	@mkdir -p $(BIN_DIR)

# The $(OBJ_DIR) rule now only creates the top-level obj directory.
# Subdirectories will be created by the compilation rules themselves.
$(OBJ_DIR):
	@mkdir -p $(OBJ_DIR)

# Build the main application
$(TARGET): $(BIN_DIR) $(OBJ_DIR) $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $(OBJS)

# Build and run the tests
test: $(TEST_TARGET)
	./$(TEST_TARGET)

$(TEST_TARGET): $(BIN_DIR) $(OBJ_DIR) $(TEST_OBJS)
	$(CC) $(LDFLAGS) -o $@ $(TEST_OBJS)

# Compile main application source files into object files in obj/src/
$(OBJ_DIR)/src/%.o: src/%.c
	@mkdir -p $(dir $@) # Create the specific output directory for this object file
	$(CC) $(CFLAGS) -c $< -o $@

# Compile library source files into object files in obj/lib/
$(OBJ_DIR)/lib/%.o: src/lib/%.c
	@mkdir -p $(dir $@) # Create the specific output directory for this object file
	$(CC) $(CFLAGS) -c $< -o $@

# Compile test source files into object files in obj/tests/
$(OBJ_DIR)/tests/%.o: tests/%.c
	@mkdir -p $(dir $@) # Create the specific output directory for this object file
	$(CC) $(CFLAGS) -c $< -o $@

# Clean up build artifacts
clean:
	rm -rf $(OBJ_DIR) # Remove the entire obj directory
	rm -rf $(BIN_DIR) # Remove the entire bin directory

# Phony targets
.PHONY: all test clean
