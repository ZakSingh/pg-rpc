//! Shared plumbing for tests that generate bindings into a throwaway crate,
//! compile a small binary against them and run it against the test database.

use crate::integration::compile_helpers::{
    add_test_binary, compile_and_run_with_args, create_test_cargo_project_with, print_output,
};
use tempfile::TempDir;

/// Wraps `body` (the statements of an async `main`, with a pooled `client`
/// in scope) in a binary that prints `OK: <name>` once it has run to the end.
pub fn binary_source(name: &str, imports: &str, body: &str) -> String {
    format!(
        r#"#![allow(unused_imports, unused_variables)]
use test_project::*;
use test_project::errors::{{AsDbError, PgRpcErrorExt}};
{imports}

#[tokio::main]
async fn main() {{
    let conn_string = std::env::args().nth(1).expect("conn string required");
    let mut config = deadpool_postgres::Config::new();
    config.url = Some(conn_string);
    let pool = config
        .create_pool(Some(deadpool_postgres::Runtime::Tokio1), tokio_postgres::NoTls)
        .expect("pool");
    let client = pool.get().await.expect("client");

{body}

    println!("OK: {name}");
}}
"#
    )
}

/// Generate into a fresh crate, configured by `configure`.
pub fn generate_project(
    conn_string: &str,
    configure: impl FnOnce(pgrpc::PgrpcBuilder) -> pgrpc::PgrpcBuilder,
) -> TempDir {
    create_test_cargo_project_with(conn_string, configure)
}

/// Write `queries` to a file kept alive by the returned directory, and return
/// the config that points at it.
pub fn queries_config(queries: &str) -> (TempDir, pgrpc::QueriesConfig) {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("queries.sql");
    std::fs::write(&path, queries).expect("write queries");
    let config = pgrpc::QueriesConfig {
        paths: vec![path.to_string_lossy().to_string()],
    };
    (dir, config)
}

/// Compile and run a binary built from [`binary_source`], panicking with the
/// compiler or program output unless it reaches its success marker.
pub fn run_binary(project: &TempDir, conn_string: &str, name: &str, imports: &str, body: &str) {
    add_test_binary(project.path(), name, &binary_source(name, imports, body));
    let output = compile_and_run_with_args(project.path(), name, &[conn_string]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() || !stdout.contains(&format!("OK: {name}")) {
        print_output(&output);
        panic!(
            "test binary `{name}` failed.\nstdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
