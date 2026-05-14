//! Regression test for the single-field `group__field` nesting bug.
//!
//! When a query column named `foo__bar` is `nullable_due_to_join`, pgrpc
//! groups it into a nested struct and wraps the group in `Option<Foo>`. The
//! original decoder checked whether the group was `None` by reading the
//! first grouped column as `Option<serde_json::Value>` — a sentinel that
//! only worked when the column's Postgres type was `json`/`jsonb`. For any
//! other type the decode failed at runtime with "cannot convert between the
//! Rust type `core::option::Option<serde_json::value::Value>` and the
//! Postgres type `text`" (or whichever real type the column had).
//!
//! Fix: the optional-group decoder now uses a shared `NullProbe` `FromSql`
//! impl whose `accepts(_)` returns `true` for every Postgres type, so the
//! null check goes through cleanly regardless of column type.
//!
//! Adapted from a suggestion by the bug reporter. Three adjustments from the
//! original sketch were needed to actually exercise the buggy path:
//!
//!   1. Add a second (plain) column. With only `foo__bar` in the select list,
//!      `:opt` returns `Option<String>` directly — no row struct, no grouping
//!      codepath, no bug.
//!
//!   2. Source `foo__bar` from the **right side** of the LEFT JOIN. The
//!      column has to be `nullable_due_to_join` for the group to be wrapped
//!      in `Option<…>`, which is the wrapper whose null-sentinel decode is
//!      buggy.
//!
//!   3. The LEFT JOIN'd side has to be a real table — pgrpc's nullability
//!      inference does not propagate `nullable_due_to_join` through derived
//!      tables / `VALUES` subqueries, so a `LEFT JOIN (VALUES ...)` form
//!      silently bypasses the bug. The user's real query LEFT JOIN's a real
//!      `address` table, which is what matters.
//!
//! Final repro:
//!
//!     CREATE TABLE driver (id int PRIMARY KEY);
//!     CREATE TABLE addr (id int PRIMARY KEY, postal_code text NOT NULL);
//!     INSERT INTO driver VALUES (1);
//!     INSERT INTO addr   VALUES (1, '94608');
//!
//!     -- name: Repro :opt
//!     SELECT d.id, a.postal_code AS foo__bar
//!     FROM driver d
//!     LEFT JOIN addr a ON a.id = d.id;
//!
//! `a.postal_code` is `nullable_due_to_join`, so `foo__bar` becomes a
//! single-field optional group. The seeded row makes the join match, so the
//! decoder actually executes the bad
//! `try_get::<Option<serde_json::Value>>` against a real `text` value.

use super::*;
use crate::integration::compile_helpers::{
    add_test_binary, compile_and_run_with_args, create_test_cargo_project, print_output,
    read_pretty,
};
use indoc::indoc;
use pgrpc::PgrpcBuilder;
use tempfile::TempDir;

const SCHEMA_SQL: &str = indoc! {r#"
    CREATE TABLE driver (id int PRIMARY KEY);
    CREATE TABLE addr (id int PRIMARY KEY, postal_code text NOT NULL);
    INSERT INTO driver VALUES (1);
    INSERT INTO addr   VALUES (1, '94608');
"#};

const REPRO_SQL: &str = indoc! {r#"
    -- name: Repro :opt
    SELECT d.id, a.postal_code AS foo__bar
    FROM driver d
    LEFT JOIN addr a ON a.id = d.id;
"#};

/// Codegen-only assertion: the optional-group decoder must use the
/// type-agnostic `NullProbe` sentinel, not the old `Option<serde_json::Value>`
/// probe that broke whenever the LEFT JOIN'd column wasn't json/jsonb.
#[test]
fn single_field_group_emits_null_probe_sentinel() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(client, SCHEMA_SQL).expect("create repro schema");

        let temp_dir = TempDir::new().expect("temp dir");
        let sql_file = temp_dir.path().join("repro.sql");
        std::fs::write(&sql_file, REPRO_SQL).expect("write sql");

        let output_dir = temp_dir.path().join("generated");

        PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir)
            .queries_config(pgrpc::QueriesConfig {
                paths: vec![sql_file.to_string_lossy().to_string()],
            })
            .build()
            .expect("code generation should succeed");

        let queries_code = read_pretty(output_dir.join("queries.rs"));
        eprintln!("--- generated queries.rs ---\n{}", queries_code);

        // The group is detected and emitted as a nested struct.
        assert!(
            queries_code.contains("ReproRowFoo"),
            "expected a nested `ReproRowFoo` struct for the `foo__*` group"
        );

        // And the parent field is Option-wrapped because the column is
        // nullable_due_to_join.
        assert!(
            queries_code.contains("Option<ReproRowFoo>"),
            "expected the `foo` field to be wrapped in Option<ReproRowFoo>"
        );

        // The fix: probe uses NullProbe, not the old json::Value sentinel.
        assert!(
            queries_code.contains("nullable::NullProbe"),
            "expected `NullProbe` sentinel in generated code"
        );
        assert!(
            !queries_code.contains("Option<serde_json::Value>"),
            "regression: optional-group decoder still uses the old `Option<serde_json::Value>` sentinel"
        );

        // And the shared module exists and exposes the probe.
        let nullable_code = read_pretty(output_dir.join("nullable.rs"));
        eprintln!("--- generated nullable.rs ---\n{}", nullable_code);
        assert!(
            nullable_code.contains("pub struct NullProbe"),
            "expected nullable.rs to define `pub struct NullProbe`"
        );
        assert!(
            nullable_code.contains("impl<'a> FromSql<'a> for NullProbe"),
            "expected NullProbe to implement FromSql"
        );
    });
}

/// End-to-end assertion: build the generated crate, run the query against the
/// real container, and confirm whether the decode actually fails at runtime.
/// This is the test that reproduces the user's 500 — if the bug is fixed, the
/// binary prints "decoded ok"; if it's present, the binary errors out and we
/// surface the message.
#[test]
fn single_field_group_decodes_at_runtime() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(client, SCHEMA_SQL).expect("create repro schema");

        let project = create_test_cargo_project(conn_string, vec!["public"]);
        let project_dir = project.path();

        let sql_file = project_dir.join("repro.sql");
        std::fs::write(&sql_file, REPRO_SQL).expect("write sql");

        // Regenerate so the test project picks up `repro.sql`.
        PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(project_dir.join("src/generated"))
            .queries_config(pgrpc::QueriesConfig {
                paths: vec![sql_file.to_string_lossy().to_string()],
            })
            .build()
            .expect("regen should succeed");

        // Make sure lib.rs re-exports queries.rs so the binary can call it.
        let lib_rs = project_dir.join("src/lib.rs");
        let mut lib = std::fs::read_to_string(&lib_rs).unwrap();
        if !lib.contains("pub use generated::queries") {
            lib.push_str("\n#[allow(unused_imports)] pub use generated::queries::*;\n");
            std::fs::write(&lib_rs, lib).unwrap();
        }

        let test_code = indoc! {r#"
            use std::error::Error;
            use deadpool_postgres::{Config, Runtime};
            use test_project::*;

            #[tokio::main]
            async fn main() {
                let conn_string = std::env::args().nth(1).expect("conn string required");

                let mut config = Config::new();
                config.url = Some(conn_string);
                let pool = config
                    .create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
                    .expect("pool");
                let client = pool.get().await.expect("client");

                match repro(&client).await {
                    Ok(Some(row)) => {
                        let foo = row.foo.expect("foo group should be Some — the join matched");
                        let bar = foo.bar.expect("bar should be present — the join matched");
                        assert_eq!(bar, "94608", "expected bar == \"94608\", got {:?}", bar);
                        println!("decoded ok: foo.bar = {}", bar);
                    }
                    Ok(None) => panic!("query unexpectedly returned no rows"),
                    Err(e) => {
                        eprintln!("DECODE ERROR: {}", e);
                        let mut src: Option<&dyn Error> = e.source();
                        while let Some(s) = src {
                            eprintln!("  caused by: {}", s);
                            src = s.source();
                        }
                        std::process::exit(2);
                    }
                }
            }
        "#};

        add_test_binary(project_dir, "test_single_field_group", test_code);

        let output =
            compile_and_run_with_args(project_dir, "test_single_field_group", &[conn_string]);

        if !output.status.success() {
            print_output(&output);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            panic!(
                "single-field optional-group decode failed at runtime.\n\
                 stdout:\n{}\n\nstderr:\n{}",
                stdout, stderr,
            );
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("decoded ok"),
            "expected `decoded ok` in stdout, got:\n{}",
            stdout
        );
    });
}
