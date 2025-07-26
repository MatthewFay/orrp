# orrp
orrp is a high-performance database engine.

## Project Structure
The project is organized into the following directories:

```
orrp/
├── bin/                # Compiled executable files
├── include/            # Public header files (.h)
├── obj/                # Intermediate object files (.o) generated during compilation
│   ├── src/            # Object files for main application source
│   └── tests/          # Object files for test source
├── src/                # Main application source files (.c)
├── tests/              # Test files and testing framework
│   ├── unity/          # Unity test framework source
│   ├── test_main.c     # Individual test cases
│   └── test_runner.c   # Main entry point for tests
├── Makefile            # Build script for compiling and testing
└── README.md           # Documentation
```

## How to Build and Run
You will need `gcc` and `make` installed on your system to build and run this project.

### Build the Main Application
To compile the main application, run the `make` command or `make all`. This will create the executable file named `orrp` inside the `bin/` directory.

To run the application:

`./bin/orrp`

### Run the Tests
To compile and run the test suite, use `make test`. This will create the `test_runner` executable in the `bin/` directory and then execute it.

You will see output from the Unity test framework, indicating the tests have passed.

### Clean the Project
To remove all compiled object files (from `obj/`) and executables (from `bin/`), use `make clean`.

