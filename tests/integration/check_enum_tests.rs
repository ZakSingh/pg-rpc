use indoc::indoc;
use pgrpc::PgrpcBuilder;
use std::fs;
use tempfile::TempDir;

use crate::integration::compile_helpers::prettify;
use crate::integration::{get_test_container, execute_sql};

#[test]
fn test_check_constraint_enum_generation() {
    let container = get_test_container();
    let db_name = container
        .create_test_database()
        .expect("Failed to create test database");

    let mut client = container
        .connect_to_database(&db_name)
        .expect("Failed to connect to test database");

    // Create a table with a CHECK constraint that defines an enum
    execute_sql(
        &mut client,
        indoc! {"
            CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                status TEXT NOT NULL CHECK (status IN ('pending', 'shipped', 'delivered', 'cancelled'))
            );
        "},
    )
    .expect("Failed to create table");

    // Generate code
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let conn_string = container.database_connection_string(&db_name);

    PgrpcBuilder::new()
        .connection_string(&conn_string)
        .schema("public")
        .output_path(output_dir.path())
        .build()
        .expect("Code generation should succeed");

    // Read the generated public.rs (prettified so substring matches are readable)
    let public_rs = prettify(
        &fs::read_to_string(output_dir.path().join("public.rs"))
            .expect("Should be able to read generated file"),
    );

    // Check that the enum was generated
    assert!(
        public_rs.contains("pub enum OrdersStatus"),
        "Should generate OrdersStatus enum"
    );

    // Check that variants are generated
    assert!(
        public_rs.contains("Pending"),
        "Should have Pending variant"
    );
    assert!(
        public_rs.contains("Shipped"),
        "Should have Shipped variant"
    );
    assert!(
        public_rs.contains("Delivered"),
        "Should have Delivered variant"
    );
    assert!(
        public_rs.contains("Cancelled"),
        "Should have Cancelled variant"
    );

    // Check that FromSql is implemented (not the derive macro version)
    assert!(
        public_rs.contains("impl<'a> postgres_types::FromSql<'a> for OrdersStatus"),
        "Should implement FromSql for OrdersStatus"
    );

    // Check that ToSql is implemented
    assert!(
        public_rs.contains("impl postgres_types::ToSql for OrdersStatus"),
        "Should implement ToSql for OrdersStatus"
    );

    // Check that it accepts TEXT type
    assert!(
        public_rs.contains("postgres_types::Type::TEXT"),
        "Should accept TEXT type"
    );
}

#[test]
fn test_check_constraint_or_chain_enum() {
    let container = get_test_container();
    let db_name = container
        .create_test_database()
        .expect("Failed to create test database");

    let mut client = container
        .connect_to_database(&db_name)
        .expect("Failed to connect to test database");

    // Create a table with CHECK constraint using OR chain pattern
    execute_sql(
        &mut client,
        indoc! {"
            CREATE TABLE tasks (
                id SERIAL PRIMARY KEY,
                priority TEXT NOT NULL CHECK (priority = 'low' OR priority = 'medium' OR priority = 'high')
            );
        "},
    )
    .expect("Failed to create table");

    // Generate code
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let conn_string = container.database_connection_string(&db_name);

    PgrpcBuilder::new()
        .connection_string(&conn_string)
        .schema("public")
        .output_path(output_dir.path())
        .build()
        .expect("Code generation should succeed");

    // Read the generated public.rs (prettified so substring matches are readable)
    let public_rs = prettify(
        &fs::read_to_string(output_dir.path().join("public.rs"))
            .expect("Should be able to read generated file"),
    );

    // Check that the enum was generated
    assert!(
        public_rs.contains("pub enum TasksPriority"),
        "Should generate TasksPriority enum"
    );

    // Check that variants are generated
    assert!(public_rs.contains("Low"), "Should have Low variant");
    assert!(public_rs.contains("Medium"), "Should have Medium variant");
    assert!(public_rs.contains("High"), "Should have High variant");
}

#[test]
fn test_multi_column_check_constraint_not_enum() {
    let container = get_test_container();
    let db_name = container
        .create_test_database()
        .expect("Failed to create test database");

    let mut client = container
        .connect_to_database(&db_name)
        .expect("Failed to connect to test database");

    // Create a table with a multi-column CHECK constraint (should NOT become an enum)
    execute_sql(
        &mut client,
        indoc! {"
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                price NUMERIC NOT NULL,
                discount NUMERIC NOT NULL,
                CONSTRAINT valid_discount CHECK (discount >= 0 AND discount <= price)
            );
        "},
    )
    .expect("Failed to create table");

    // Generate code
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let conn_string = container.database_connection_string(&db_name);

    PgrpcBuilder::new()
        .connection_string(&conn_string)
        .schema("public")
        .output_path(output_dir.path())
        .build()
        .expect("Code generation should succeed");

    // Read the generated public.rs if it exists; absence is acceptable since
    // nothing in this schema requires codegen output for the negative case.
    let public_rs = fs::read_to_string(output_dir.path().join("public.rs"))
        .map(|s| prettify(&s))
        .unwrap_or_default();

    // Should NOT generate an enum for this constraint
    assert!(
        !public_rs.contains("pub enum ProductsDiscount"),
        "Should NOT generate enum for multi-column constraint"
    );
    assert!(
        !public_rs.contains("pub enum ProductsPrice"),
        "Should NOT generate enum for multi-column constraint"
    );
}

#[test]
fn test_complex_check_constraint_not_enum() {
    let container = get_test_container();
    let db_name = container
        .create_test_database()
        .expect("Failed to create test database");

    let mut client = container
        .connect_to_database(&db_name)
        .expect("Failed to connect to test database");

    // Create a table with a complex CHECK constraint (should NOT become an enum)
    execute_sql(
        &mut client,
        indoc! {"
            CREATE TABLE items (
                id SERIAL PRIMARY KEY,
                status TEXT NOT NULL,
                CONSTRAINT status_and_more CHECK (status IN ('active', 'inactive') AND length(status) > 0)
            );
        "},
    )
    .expect("Failed to create table");

    // Generate code
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let conn_string = container.database_connection_string(&db_name);

    PgrpcBuilder::new()
        .connection_string(&conn_string)
        .schema("public")
        .output_path(output_dir.path())
        .build()
        .expect("Code generation should succeed");

    // Read the generated public.rs if it exists; absence is acceptable since
    // nothing in this schema requires codegen output for the negative case.
    let public_rs = fs::read_to_string(output_dir.path().join("public.rs"))
        .map(|s| prettify(&s))
        .unwrap_or_default();

    // Should NOT generate an enum for this complex constraint
    assert!(
        !public_rs.contains("pub enum ItemsStatus"),
        "Should NOT generate enum for complex constraint"
    );
}

/// An inferred enum is named `{table}_{column}`. When a real type in the
/// schema already has that name the inferred one is renamed, so the module
/// still compiles.
#[test]
fn test_check_enum_name_does_not_shadow_a_real_type() {
    use crate::integration::compile_helpers::{
        assert_compilation_success, compile_project, create_test_cargo_project, read_pretty,
    };

    let container = get_test_container();
    let db_name = container
        .create_test_database()
        .expect("Failed to create test database");
    let mut client = container
        .connect_to_database(&db_name)
        .expect("Failed to connect to test database");

    execute_sql(
        &mut client,
        indoc! {"
            CREATE SCHEMA ce;
            -- A real enum with the name the inferred one would get.
            CREATE TYPE ce.orders_status AS ENUM ('real');
            CREATE TABLE ce.orders (
                id SERIAL PRIMARY KEY,
                status TEXT NOT NULL CHECK (status IN ('pending', 'shipped'))
            );
            -- Two tables whose inferred names coincide: a_b + c, a + b_c.
            CREATE TABLE ce.a_b (c TEXT CHECK (c IN ('x', 'y')));
            CREATE TABLE ce.a (b_c TEXT CHECK (b_c IN ('p', 'q')));

            CREATE FUNCTION ce.echo(p_status ce.orders_status) RETURNS ce.orders_status
                LANGUAGE sql AS $$ SELECT p_status $$;
        "},
    )
    .expect("Failed to create schema");

    let conn_string = container.database_connection_string(&db_name);
    let project = create_test_cargo_project(&conn_string, vec!["ce"]);
    let ce_rs = read_pretty(project.path().join("src/generated/ce.rs"));

    assert_eq!(ce_rs.matches("pub enum OrdersStatus {").count(), 1, "{ce_rs}");
    assert!(ce_rs.contains("pub enum OrdersStatusCheck {"), "{ce_rs}");
    assert_eq!(ce_rs.matches("pub enum ABC {").count(), 1, "{ce_rs}");
    assert!(ce_rs.contains("pub enum ABCCheck {"), "{ce_rs}");

    assert_compilation_success(compile_project(project.path()));
}
