//! Tests for strict (validated) domain newtypes.
//!
//! A domain marked `@pgrpc_strict` (or listed in `strict_domains`) generates a
//! newtype with a private inner field whose `Deserialize` impl routes through a
//! user-provided `TryFrom<Inner>` impl via `#[serde(try_from = "...")]`.

use super::*;
use crate::integration::compile_helpers::{
    add_test_binary, assert_execution_success, compile_and_run, create_test_cargo_project,
    read_pretty,
};
use tempfile::TempDir;

const STRICT_SCHEMA: &str = r#"
    CREATE SCHEMA strict_test;

    CREATE DOMAIN strict_test.zip AS text CHECK (VALUE ~ '^[0-9]{5}$');
    COMMENT ON DOMAIN strict_test.zip IS 'US zip code. @pgrpc_strict';

    CREATE DOMAIN strict_test.loose AS text;

    CREATE TABLE strict_test.addresses (
        id SERIAL PRIMARY KEY,
        zip strict_test.zip NOT NULL,
        alt strict_test.loose
    );

    -- Types are only generated when referenced by a function.
    CREATE FUNCTION strict_test.get_zip(p_id INT, p_alt strict_test.loose)
    RETURNS strict_test.zip AS $$
        SELECT zip FROM strict_test.addresses WHERE id = p_id
    $$ LANGUAGE sql;
"#;

#[test]
fn test_strict_domain_via_annotation() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(client, STRICT_SCHEMA).expect("Should create test schema");

        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("strict_test")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        let code = read_pretty(output_path.join("strict_test.rs"));

        // Strict domain: private field, TryFrom-routed Deserialize, explicit
        // constructor surface.
        assert!(
            code.contains("pub struct Zip(String);"),
            "strict domain should have a private inner field. Got:\n{}",
            code
        );
        assert!(
            code.contains(r#"#[serde(try_from = "String")]"#),
            "strict domain should route Deserialize through TryFrom"
        );
        assert!(code.contains("pub fn new_unchecked(value: String) -> Self"));
        assert!(code.contains("pub fn into_inner(self) -> String"));
        assert!(code.contains("pub fn as_inner(&self) -> &String"));

        // Non-strict domain: unchanged public field, no try_from routing.
        assert!(
            code.contains("pub struct Loose(pub String);"),
            "non-strict domain should keep its public inner field. Got:\n{}",
            code
        );
        assert!(!code.contains(r#"#[serde(try_from = "String")]
pub struct Loose"#));
    });
}

#[test]
fn test_strict_domain_via_config() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            CREATE SCHEMA strict_test;
            CREATE DOMAIN strict_test.zip AS text CHECK (VALUE ~ '^[0-9]{5}$');
            CREATE TABLE strict_test.addresses (
                id SERIAL PRIMARY KEY,
                zip strict_test.zip NOT NULL
            );

            -- Types are only generated when referenced by a function.
            CREATE FUNCTION strict_test.get_zip(p_id INT)
            RETURNS strict_test.zip AS $$
                SELECT zip FROM strict_test.addresses WHERE id = p_id
            $$ LANGUAGE sql;
        "#,
        )
        .expect("Should create test schema");

        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("strict_test")
            .strict_domain("strict_test.zip")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        let code = read_pretty(output_path.join("strict_test.rs"));

        assert!(
            code.contains("pub struct Zip(String);"),
            "config-marked strict domain should have a private inner field. Got:\n{}",
            code
        );
        assert!(code.contains(r#"#[serde(try_from = "String")]"#));
        assert!(code.contains("pub fn new_unchecked(value: String) -> Self"));
    });
}

/// End-to-end: a consumer crate provides the `TryFrom` impl, and serde
/// deserialization actually routes through it at runtime.
#[test]
fn test_strict_domain_end_to_end() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(client, STRICT_SCHEMA).expect("Should create test schema");

        let project = create_test_cargo_project(conn_string, vec!["strict_test"]);

        // The TryFrom impl must live in the same crate as the generated struct
        // (serde's try_from bound is checked where Deserialize is derived). This
        // mirrors real usage, where generated code is include!-d into the user's
        // crate alongside their impls.
        let lib_rs_path = project.path().join("src/lib.rs");
        let mut lib_rs = std::fs::read_to_string(&lib_rs_path).expect("read lib.rs");
        lib_rs.push_str(
            r#"
#[derive(Debug)]
pub struct ZipError;

impl std::fmt::Display for ZipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "zip must be 5 ascii digits")
    }
}

impl TryFrom<String> for generated::strict_test::Zip {
    type Error = ZipError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        if s.len() == 5 && s.bytes().all(|b| b.is_ascii_digit()) {
            Ok(Self::new_unchecked(s))
        } else {
            Err(ZipError)
        }
    }
}
"#,
        );
        std::fs::write(&lib_rs_path, lib_rs).expect("write lib.rs");

        add_test_binary(
            project.path(),
            "strict_domain_check",
            r#"
use test_project::generated::strict_test::Zip;

fn main() {
    let ok: Result<Zip, _> = serde_json::from_str("\"12345\"");
    assert!(ok.is_ok(), "valid zip should deserialize");

    let bad: Result<Zip, _> = serde_json::from_str("\"abcde\"");
    assert!(bad.is_err(), "invalid zip must be rejected by the TryFrom impl");

    let zip = Zip::try_from("54321".to_string()).expect("valid zip should construct");
    assert_eq!(zip.as_inner(), "54321");
    assert_eq!(zip.into_inner(), "54321");

    println!("strict domain e2e OK");
}
"#,
        );

        let output = compile_and_run(project.path(), "strict_domain_check");
        assert_execution_success(output, Some("strict domain e2e OK"));
    });
}

/// A strict domain used as a query parameter in comparison position forces the
/// query codegen to peel the domain wrapper. That unwrap must use `as_inner()`
/// (not `.0` field access), because `queries.rs` is a sibling module of the
/// schema module and cannot see the private field.
#[test]
fn test_strict_domain_query_param_unwrap() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            CREATE DOMAIN zip AS text CHECK (VALUE ~ '^[0-9]{5}$');
            COMMENT ON DOMAIN zip IS '@pgrpc_strict';
            CREATE TABLE addresses (
                id SERIAL PRIMARY KEY,
                zip zip NOT NULL
            );
        "#,
        )
        .expect("Should create test schema");

        let sql = indoc! {r#"
            -- name: GetAddressesByZip :many
            SELECT id FROM addresses WHERE zip = :zip;
        "#};

        let temp_dir = TempDir::new().expect("Should create temp directory");
        let sql_file = temp_dir.path().join("test.sql");
        std::fs::write(&sql_file, sql).expect("write sql file");
        let output_dir = temp_dir.path().join("generated");

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir)
            .queries_config(pgrpc::QueriesConfig {
                paths: vec![sql_file.to_string_lossy().to_string()],
            })
            .build()
            .expect("Code generation should succeed");

        // read_pretty parses with syn, so this also proves queries.rs is
        // structurally valid against the private field.
        let code = read_pretty(output_dir.join("queries.rs"));

        assert!(code.contains("pub async fn get_addresses_by_zip"));
        assert!(
            code.contains("as_inner()"),
            "domain unwrap in queries.rs must go through as_inner(). Got:\n{}",
            code
        );
        assert!(
            !code.contains(".0"),
            "domain unwrap must not use .0 field access (private for strict domains). Got:\n{}",
            code
        );
    });
}
