# Compiler and flags
CC = gcc
CFLAGS = -Iinclude -Itests/unity -Wall -Wextra -g
LDFLAGS =

# Source files
SRCS = src/main.c
# Add other source files here, e.g., SRCS += src/core/abc.c

# Directories for build artifacts
BIN_DIR = bin
OBJ_DIR = obj

# Object files will now go into their respective subdirectories within 'obj'
OBJS = $(patsubst src/%.c, $(OBJ_DIR)/src/%.o, $(SRCS))

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

# Compile main source files into object files in obj/src/
$(OBJ_DIR)/src/%.o: src/%.c
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
