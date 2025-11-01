<p align="center">
    <img src="orrp.png" alt="orrp" />
</p>

# orrp
orrp is a high-performance database.

## Project Structure
The project is organized into the following directories:

```
orrp/
├── bin/                # Compiled executable files
├── include/            # Public header files (.h)
├── lib/                # Libraries
├── obj/                # Intermediate object files (.o) generated during compilation
│   ├── src/            # Object files for main application source
│   └── tests/          # Object files for test source
├── src/                # Main application source files (.c)
├── tests/              # Test files and testing framework
│   ├── unity/          # Unity test framework source
│   ├── test_main.c     # Individual test cases
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
To compile and run the test suite, use `make test`. This will create the test executables in the `bin/` directory and then execute them.

### Clean the Project
To remove all compiled object files (from `obj/`) and executables (from `bin/`), use `make clean`.

## Development
This project is configured for development using the **clangd** language server.

### Language Server Setup
To enable `clangd` for this project:

1.  **Install `bear`:** Install the `bear` build tool. `bear` will generate a file that tells `clangd` how to build the project.
    ```sh
    brew install bear
    ```
2.  **Generate `compile_commands.json`:** This file is required by `clangd`. First, clean the project, then run `bear` with the `make` command.
    ```sh
    make clean
    bear -- make
    ```

**Note:** If you add new source files or change the `Makefile`, you will need to re-run `bear -- make` to update the `compile_commands.json` file.

## Protocol

### Commands

All commands follow the same pattern: `<command keyword> ...key-value tags`.

#### EVENT

```
EVENT in:analytics_2025_08 id:user123 loc:ca action:login+ day:2025_08_16

```

#### QUERY

```
QUERY in:analytics_2025_01 exp:(loc:ca AND (action:login > 3))

QUERY in:analytics_2025_08 exp:(view:product_123 > 3 AND NOT purchase:product_123 AND day:2025_08_03) take:100
```

