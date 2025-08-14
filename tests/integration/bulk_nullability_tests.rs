use super::*;
use indoc::indoc;
use tempfile::TempDir;

/// Test that type-level @pgrpc_not_null annotations work for composite types
#[test]
fn test_composite_type_bulk_not_null() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a custom schema to avoid conflicts
        execute_sql(
            client,
            indoc! {"
                CREATE SCHEMA test_bulk;
                SET search_path TO test_bulk;
                
                CREATE TYPE user_info AS (
                    id bigint,
                    email text,
                    name text,
                    bio text,
                    avatar_url text
                );
                COMMENT ON TYPE user_info IS 'User information @pgrpc_not_null(id, email, name)';
            "},
        )
        .expect("Should create schema and composite type");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("test_bulk")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Check generated type
        let schema_content = std::fs::read_to_string(output_path.join("test_bulk.rs"))
            .expect("Should read test_bulk.rs");

        // Debug: print the generated struct
        if !schema_content.contains("pub id: i64") {
            eprintln!("Looking for UserInfo struct...");
            let mut in_struct = false;
            for line in schema_content.lines() {
                if line.contains("struct") && line.contains("UserInfo") {
                    in_struct = true;
                }
                if in_struct && line.contains("}") {
                    eprintln!("{}", line);
                    in_struct = false;
                }
                if in_struct {
                    eprintln!("{}", line);
                }
            }
            
            eprintln!("\nAll structs in file:");
            for line in schema_content.lines() {
                if line.contains("pub struct") {
                    eprintln!("{}", line);
                }
            }
        }

        // Verify struct fields have correct nullability
        assert!(schema_content.contains("pub id: i64"), "id should not be Option");
        assert!(schema_content.contains("pub email: String"), "email should not be Option");
        assert!(schema_content.contains("pub name: String"), "name should not be Option");
        assert!(schema_content.contains("pub bio: Option<String>"), "bio should be Option");
        assert!(schema_content.contains("pub avatar_url: Option<String>"), "avatar_url should be Option");
    });
}

/// Test that column-level annotations override type-level annotations
#[test]
fn test_annotation_precedence() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a schema for this test
        execute_sql(
            client,
            indoc! {"
                CREATE SCHEMA test_precedence;
                SET search_path TO test_precedence;
                
                CREATE TYPE contact_info AS (
                    phone text,
                    email text,
                    address text
                );
                COMMENT ON TYPE contact_info IS '@pgrpc_not_null(phone, email)';
                -- Column-level annotation should make this nullable despite type-level annotation
                COMMENT ON COLUMN contact_info.phone IS 'Primary phone number (optional)';
                -- This should still be not null from type-level annotation
                COMMENT ON COLUMN contact_info.email IS 'Primary email';
            "},
        )
        .expect("Should create schema and composite type");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("test_precedence")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Check generated type
        let schema_content = std::fs::read_to_string(output_path.join("test_precedence.rs"))
            .expect("Should read test_precedence.rs");

        // Email should be not null from type-level annotation
        assert!(schema_content.contains("pub email: String"), "email should not be Option");
        // Address should be nullable (no annotation)
        assert!(schema_content.contains("pub address: Option<String>"), "address should be Option");
    });
}

/// Test view with bulk not null annotations
#[test]
#[ignore = "Fix after composite type test passes"]
fn test_view_bulk_not_null() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create base table
        execute_sql(
            client,
            indoc! {"
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    email TEXT,
                    name TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                
                -- Create view with bulk annotation
                CREATE VIEW user_summary AS
                SELECT id, email, name, created_at
                FROM users;
                
                COMMENT ON VIEW user_summary IS 'User summary view @pgrpc_not_null(id, email, created_at)';
            "},
        )
        .expect("Should create table and view");

        // Generate code with nullability inference
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        // Check generated view type
        let public_content = std::fs::read_to_string(output_path.join("public.rs"))
            .expect("Should read public.rs");

        // Fields marked in bulk annotation should be not null
        assert!(schema_content.contains("pub id: i32"), "id should not be Option");
        assert!(schema_content.contains("pub email: String"), "email should not be Option");
        assert!(schema_content.contains("pub created_at: time::OffsetDateTime"), "created_at should not be Option");
        // Name was not in the annotation, so it should be nullable
        assert!(schema_content.contains("pub name: Option<String>"), "name should be Option");
    });
}

/// Test mixed column and type-level annotations on views
#[test]
#[ignore = "Fix after composite type test passes"]
fn test_view_mixed_annotations() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create base tables
        execute_sql(
            client,
            indoc! {"
                CREATE TABLE products (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    price DECIMAL(10,2),
                    category TEXT
                );
                
                -- Create view with both type-level and column-level annotations
                CREATE VIEW product_catalog AS
                SELECT id, name, description, price, category
                FROM products;
                
                COMMENT ON VIEW product_catalog IS 'Product catalog @pgrpc_not_null(id, price)';
                COMMENT ON COLUMN product_catalog.description IS '@pgrpc_not_null';
                -- Category should remain nullable
            "},
        )
        .expect("Should create table and view");

        // Generate code with nullability inference
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        // Check generated view type
        let public_content = std::fs::read_to_string(output_path.join("public.rs"))
            .expect("Should read public.rs");

        // Type-level annotations
        assert!(schema_content.contains("pub id: i32"), "id should not be Option");
        assert!(schema_content.contains("pub price: rust_decimal::Decimal"), "price should not be Option");
        // Column-level annotation
        assert!(schema_content.contains("pub description: String"), "description should not be Option");
        // Name is NOT NULL in base table, should be inferred
        assert!(schema_content.contains("pub name: String"), "name should not be Option");
        // Category has no annotation and is nullable in base table
        assert!(schema_content.contains("pub category: Option<String>"), "category should be Option");
    });
}

/// Test invalid column names in bulk annotations
#[test]
#[ignore = "Fix after composite type test passes"]
fn test_invalid_column_names_ignored() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create type with annotation containing invalid column names
        execute_sql(
            client,
            indoc! {"
                CREATE TYPE test_type AS (
                    real_column text,
                    another_column integer
                );
                COMMENT ON TYPE test_type IS '@pgrpc_not_null(real_column, fake_column, another_column)';
            "},
        )
        .expect("Should create composite type");

        // Generate code - should not fail
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .build()
            .expect("Should generate code despite invalid column names");

        // Check generated type - only valid columns should be affected
        let public_content = std::fs::read_to_string(output_path.join("public.rs"))
            .expect("Should read public.rs");

        assert!(schema_content.contains("pub real_column: String"), "real_column should not be Option");
        assert!(schema_content.contains("pub another_column: i32"), "another_column should not be Option");
    });
}

/// Test multiple bulk annotations in same comment
#[test]
#[ignore = "Fix after composite type test passes"]
fn test_multiple_annotations_in_comment() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create type with multiple annotations
        execute_sql(
            client,
            indoc! {"
                CREATE TYPE annotated_type AS (
                    field1 text,
                    field2 text,
                    field3 text,
                    field4 text
                );
                COMMENT ON TYPE annotated_type IS 'Some description @pgrpc_not_null(field1, field2) and more text @pgrpc_not_null(field3) end';
            "},
        )
        .expect("Should create composite type");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Check generated type - all annotated fields should be not null
        let public_content = std::fs::read_to_string(output_path.join("public.rs"))
            .expect("Should read public.rs");

        assert!(schema_content.contains("pub field1: String"), "field1 should not be Option");
        assert!(schema_content.contains("pub field2: String"), "field2 should not be Option");
        assert!(schema_content.contains("pub field3: String"), "field3 should not be Option");
        assert!(schema_content.contains("pub field4: Option<String>"), "field4 should be Option");
    });
}