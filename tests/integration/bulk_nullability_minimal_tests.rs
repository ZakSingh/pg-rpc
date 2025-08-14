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
                
                CREATE TYPE test_bulk.user_info AS (
                    id bigint,
                    email text,
                    name text,
                    bio text,
                    avatar_url text
                );
                COMMENT ON TYPE test_bulk.user_info IS 'User information @pgrpc_not_null(id, email, name)';
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
        eprintln!("=== Generated test_bulk.rs content ===");
        eprintln!("{}", schema_content);
        eprintln!("=== End of content ===");

        // Verify struct fields have correct nullability
        assert!(schema_content.contains("pub id: i64"), "id should not be Option");
        assert!(schema_content.contains("pub email: String"), "email should not be Option");
        assert!(schema_content.contains("pub name: String"), "name should not be Option");
        assert!(schema_content.contains("pub bio: Option<String>"), "bio should be Option");
        assert!(schema_content.contains("pub avatar_url: Option<String>"), "avatar_url should be Option");
    });
}

/// Test that column-level annotations have precedence
#[test]
fn test_column_annotation_precedence() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a schema for this test
        execute_sql(
            client,
            indoc! {"
                CREATE SCHEMA test_precedence;
                
                CREATE TYPE test_precedence.contact_info AS (
                    phone text,
                    email text,
                    address text
                );
                COMMENT ON TYPE test_precedence.contact_info IS '@pgrpc_not_null(phone, email)';
                -- Phone has column-level @pgrpc_not_null, should override type-level
                COMMENT ON COLUMN test_precedence.contact_info.phone IS '@pgrpc_not_null';
                -- Email should still use type-level annotation
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

        // Both should be not null - phone from column annotation, email from type annotation
        assert!(schema_content.contains("pub phone: String"), "phone should not be Option");
        assert!(schema_content.contains("pub email: String"), "email should not be Option");
        // Address should be nullable (no annotation)
        assert!(schema_content.contains("pub address: Option<String>"), "address should be Option");
    });
}