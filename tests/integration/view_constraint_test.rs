use super::*;
use tempfile::TempDir;

/// Test that constraint error enums are not generated for views
#[test]
fn test_no_constraint_enums_for_views() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a schema with both tables and views that have constraints
        execute_sql(
            client,
            r#"
            CREATE SCHEMA constraint_test;
            SET search_path TO constraint_test;
            
            -- Create a domain type with constraint
            CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0);
            
            -- Create a table with constraints
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                age positive_int
            );
            
            -- Create a view with domain constraint columns
            CREATE VIEW active_users AS
            SELECT 
                id,
                email,
                age
            FROM users
            WHERE age > 18;
            
            -- Create another view (views inherit nullability from underlying columns)
            CREATE VIEW user_summary AS
            SELECT 
                id,
                email,
                COALESCE(age, 21)::positive_int as age
            FROM users;
        "#,
        )
        .expect("Should create test schema");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("constraint_test")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Check the errors.rs file for constraint enums
        let errors_content = std::fs::read_to_string(output_path.join("errors.rs"))
            .expect("Should read errors.rs file");

        // Tables should have constraint enums
        assert!(
            errors_content.contains("enum UsersConstraint"),
            "Should generate constraint enum for users table"
        );
        assert!(
            errors_content.contains("UsersEmailKey"),
            "Should include unique constraint variant"
        );

        // Views should NOT have constraint enums
        assert!(
            !errors_content.contains("enum ActiveUsersConstraint"),
            "Should NOT generate constraint enum for active_users view"
        );
        assert!(
            !errors_content.contains("enum UserSummaryConstraint"),
            "Should NOT generate constraint enum for user_summary view"
        );

        // Verify the unified error type only references table constraints
        assert!(
            errors_content.contains("UsersConstraint(UsersConstraint,"),
            "Unified error should include table constraints"
        );
        assert!(
            !errors_content.contains("ActiveUsersConstraint"),
            "Unified error should NOT include view constraints"
        );
        assert!(
            !errors_content.contains("UserSummaryConstraint"),
            "Unified error should NOT include view constraints"
        );

        println!("✅ Views correctly excluded from constraint enum generation!");
    });
}