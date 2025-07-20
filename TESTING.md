# Testing Guide for PgRPC

This document describes the testing infrastructure for the PgRPC project.

## Test Categories

### Unit Tests
- Located in individual source files (`src/unified_error.rs`, etc.)
- Run with: `cargo test --lib`
- Test individual functions and modules in isolation

### Integration Tests
- Located in `tests/integration/`
- Use real PostgreSQL database via testcontainers
- Test the complete code generation pipeline
- Run with: `cargo test --test integration_tests`

## Integration Testing System

### Overview
The integration tests use `testcontainers` to spin up a real PostgreSQL database for testing. This ensures that:
- Database introspection works correctly
- Generated code compiles and works with real data
- Error handling is tested with actual constraint violations
- The complete workflow from schema to code generation works

### Requirements
- Docker must be installed and running
- Tests require Docker to pull and run PostgreSQL containers

### Test Structure

#### Container Management
- Single shared PostgreSQL container across all tests
- Container is started once and reused for performance
- Database is cleaned between tests to prevent interference

#### Test Modules

1. **Schema Tests** (`schema_tests.rs`)
   - Test database introspection and schema discovery
   - Verify that tables, constraints, functions, and types are found correctly

2. **Code Generation Tests** (`codegen_tests.rs`)
   - Test that generated Rust code has the expected structure
   - Verify enum generation, struct generation, function generation
   - Test that generated code includes the unified error system

3. **Error Handling Tests** (`error_tests.rs`)
   - Test the unified error system implementation
   - Verify that constraint enums are generated correctly
   - Test that generated code compiles and includes proper error handling

4. **Workflow Tests** (`workflow_tests.rs`)
   - End-to-end testing of the complete pipeline
   - Test multiple schemas, configuration options, incremental changes
   - Verify the complete flow from database to working generated code

### Running Tests

#### Basic Test (No Docker Required)
```bash
cargo test integration::tests::test_basic_functionality --test integration_tests
```

#### All Integration Tests (Requires Docker)
```bash
# Run all integration tests (will start PostgreSQL container)
cargo test --test integration_tests -- --ignored

# Run specific test module
cargo test schema_tests --test integration_tests -- --ignored

# Run with output
cargo test --test integration_tests -- --ignored --nocapture
```

#### Why Tests are Ignored by Default
Most integration tests are marked with `#[ignore]` because they require Docker. This allows:
- CI/CD systems to run unit tests quickly without Docker
- Developers to run full integration tests when needed
- Tests to be organized by their requirements

### Test Database Schema
The tests create a realistic schema with:
- Tables with various constraint types (unique, check, foreign key, not null)
- Enums and composite types
- Views and functions
- Multiple schemas (`public`, `api`)

### Key Features Tested

1. **Unified Error System**
   - Constraint enum generation for each table
   - Proper error mapping from database errors to typed constraint violations
   - Integration with generated functions

2. **Code Generation Pipeline**
   - Schema introspection and parsing
   - Type generation (structs, enums)
   - Function generation with proper signatures
   - Cross-schema references

3. **Real Database Integration**
   - Actual PostgreSQL constraint violations
   - Real schema introspection
   - Generated code compilation and functionality

## Adding New Tests

To add new integration tests:

1. Add your test function to the appropriate module in `tests/integration/`
2. Mark it with `#[test]` and `#[ignore] // Requires Docker`
3. Use `with_clean_database(|client| { ... })` for database tests
4. Use the `indoc!` macro for readable SQL strings

Example:
```rust
#[test]
#[ignore] // Requires Docker
fn test_my_feature() {
    with_clean_database(|client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        // Your test code here
    });
}
```

## Troubleshooting

### Docker Issues
- Ensure Docker is running: `docker ps`
- Check Docker permissions if on Linux
- Container startup can take 30-60 seconds on first run

### Test Failures
- Check that schema files exist and are readable
- Verify that the test database schema matches expectations
- Run with `--nocapture` to see detailed output

### Container Cleanup
Containers are automatically cleaned up when tests finish, but you can manually clean up:
```bash
docker ps -a | grep postgres
docker rm -f <container_id>
```