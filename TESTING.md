# Testing

## Unit tests

```bash
cargo test --lib
```

No database needed. These cover the SQL analyzers (nullability, cardinality,
CHECK parsing, error analysis) against hand-built catalog fixtures.

## Integration tests

```bash
cargo test --test integration_tests            # everything
cargo test --test integration_tests trigger    # one module / name filter
```

Docker must be running. The harness (`tests/integration/mod.rs`) starts one
`postgres:16-alpine` container per test process and gives each test its own
database, so tests run in parallel without sharing state:

```rust
use super::*;

#[test]
fn my_test() {
    with_isolated_database(|client| {
        execute_sql(client, "create table t (id int primary key)").unwrap();
        // ...
    });
}
```

Use `with_isolated_database_and_container` when the test needs a connection
string (anything that runs `PgrpcBuilder`).

Tests that check generated code *behaviour* generate into a temporary crate,
then compile and run it; the helpers are in `tests/integration/compile_helpers.rs`
(`create_test_cargo_project_with`, `add_test_binary`, `compile_and_run`). These
are the slow tests: each temp crate builds its own dependencies.

New test files must be registered with a `pub mod` line in
`tests/integration/mod.rs`, otherwise they are silently not compiled.

## Ignored tests

```bash
cargo test --test integration_tests -- --ignored
```

- `postgis_tests` need a PostGIS-enabled image.
- `prepared_cache_bench` asserts on wall-clock speedups and is only meaningful
  on an idle machine.

## Fixtures

- `schema/` — the sample schema loaded by `setup_test_schema`.
- `tests/sql/` — larger SQL fixtures used by individual test modules.
