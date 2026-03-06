use indoc::indoc;
use pgrpc::PgrpcBuilder;
use std::fs;
use tempfile::TempDir;

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

    // Read the generated public.rs
    let public_rs = fs::read_to_string(output_dir.path().join("public.rs"))
        .expect("Should be able to read generated file");

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

    // Read the generated public.rs
    let public_rs = fs::read_to_string(output_dir.path().join("public.rs"))
        .expect("Should be able to read generated file");

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

    // Read the generated public.rs
    let public_rs = fs::read_to_string(output_dir.path().join("public.rs"))
        .expect("Should be able to read generated file");

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

    // Read the generated public.rs
    let public_rs = fs::read_to_string(output_dir.path().join("public.rs"))
        .expect("Should be able to read generated file");

    // Should NOT generate an enum for this complex constraint
    assert!(
        !public_rs.contains("pub enum ItemsStatus"),
        "Should NOT generate enum for complex constraint"
    );
}
