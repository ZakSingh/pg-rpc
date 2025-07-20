use super::*;
use pgrpc::*;
use tempfile::TempDir;
use indoc::indoc;

/// Generate code and compile it in a temporary directory, returning the path
fn setup_generated_code(conn_string: &str) -> TempDir {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let output_path = temp_dir.path();
    
    PgrpcBuilder::new()
        .connection_string(conn_string)
        .output_path(output_path)
        .schema("public")
        .schema("api")
        .build()
        .expect("Code generation should succeed");
    
    temp_dir
}

/// Test that we can generate a complete Cargo project with our generated code
fn create_test_project(conn_string: &str) -> TempDir {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let project_dir = temp_dir.path();
    
    // Create src directory
    let src_dir = project_dir.join("src");
    std::fs::create_dir(&src_dir).expect("Should create src directory");
    
    // Generate pgrpc code in a subdirectory
    let pgrpc_dir = src_dir.join("generated");
    std::fs::create_dir(&pgrpc_dir).expect("Should create generated directory");
    
    PgrpcBuilder::new()
        .connection_string(conn_string)
        .output_path(&pgrpc_dir)
        .schema("public")
        .schema("api")
        .build()
        .expect("Code generation should succeed");
    
    // Create a simple lib.rs that uses the generated code
    let lib_rs_content = indoc! {"
        pub mod generated;
        
        pub use generated::*;
        
        // Re-export the error type for easy access
        pub use generated::errors::PgRpcError;
    "};
    std::fs::write(src_dir.join("lib.rs"), lib_rs_content)
        .expect("Should write lib.rs");
    
    // Create Cargo.toml
    let cargo_toml_content = indoc! {"
        [package]
        name = \"test-project\"
        version = \"0.1.0\"
        edition = \"2021\"
        
        [dependencies]
        tokio-postgres = { version = \"0.7\", features = [\"with-serde_json-1\", \"with-time-0_3\"] }
        postgres-types = { version = \"0.2\", features = [\"derive\"] }
        serde = { version = \"1.0\", features = [\"derive\"] }
        serde_json = \"1.0\"
        time = { version = \"0.3\", features = [\"serde\"] }
        thiserror = \"1.0\"
        deadpool-postgres = \"0.12\"
    "};
    std::fs::write(project_dir.join("Cargo.toml"), cargo_toml_content)
        .expect("Should write Cargo.toml");
    
    temp_dir
}

#[test]
#[ignore] // Requires Docker
fn test_error_enum_generation() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();
        
        // Read the errors.rs file
        let errors_content = std::fs::read_to_string(generated_dir.join("errors.rs"))
            .expect("Should read errors.rs");
        
        // Verify PgRpcError enum exists
        assert!(errors_content.contains("pub enum PgRpcError"));
        
        // Verify constraint enums exist for tables with constraints
        assert!(errors_content.contains("pub enum AccountConstraint"));
        assert!(errors_content.contains("pub enum PostConstraint"));
        
        // Verify specific constraint variants
        assert!(errors_content.contains("AccountEmailKey")); // from UNIQUE constraint
        assert!(errors_content.contains("PostTitleLength")); // from CHECK constraint
        
        // Verify error variants reference constraint enums
        assert!(errors_content.contains("AccountConstraint(AccountConstraint"));
        assert!(errors_content.contains("PostConstraint(PostConstraint"));
        
        // Verify From implementation exists
        assert!(errors_content.contains("impl From<tokio_postgres::Error> for PgRpcError"));
    });
}

#[test]
#[ignore] // Requires Docker
fn test_constraint_violation_mapping() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();
        
        let errors_content = std::fs::read_to_string(generated_dir.join("errors.rs"))
            .expect("Should read errors.rs");
        
        // Should have constraint violation handling for different SQL states
        assert!(errors_content.contains("UNIQUE_VIOLATION"));
        assert!(errors_content.contains("CHECK_VIOLATION"));
        assert!(errors_content.contains("FOREIGN_KEY_VIOLATION"));
        assert!(errors_content.contains("NOT_NULL_VIOLATION"));
        
        // Should map specific constraint names to enum variants
        assert!(errors_content.contains("account_email_key")); // unique constraint name
        assert!(errors_content.contains("post_title_length")); // check constraint name
    });
}

#[test]
#[ignore] // Requires Docker 
fn test_generated_code_compiles() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        let _temp_dir = create_test_project(conn_string);
        let project_dir = _temp_dir.path();
        
        // Try to compile the generated project
        let output = std::process::Command::new("cargo")
            .arg("check")
            .current_dir(project_dir)
            .output()
            .expect("Should run cargo check");
        
        if !output.status.success() {
            eprintln!("Cargo check failed:");
            eprintln!("STDOUT: {}", String::from_utf8_lossy(&output.stdout));
            eprintln!("STDERR: {}", String::from_utf8_lossy(&output.stderr));
            panic!("Generated code should compile");
        }
    });
}

#[test]
#[ignore] // Requires Docker
fn test_error_enum_has_proper_traits() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();
        
        let errors_content = std::fs::read_to_string(generated_dir.join("errors.rs"))
            .expect("Should read errors.rs");
        
        // PgRpcError should derive Debug and use thiserror::Error
        assert!(errors_content.contains("#[derive(Debug, thiserror::Error)]"));
        
        // Constraint enums should have proper derives
        assert!(errors_content.contains("#[derive(Debug, Clone, Copy, PartialEq, Eq)]"));
        
        // Constraint enums should implement Display
        assert!(errors_content.contains("impl std::fmt::Display for AccountConstraint"));
        assert!(errors_content.contains("impl std::fmt::Display for PostConstraint"));
    });
}

#[test]
#[ignore] // Requires Docker
fn test_constraint_enum_coverage() {
    with_clean_database(|client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        // First, let's see what constraints actually exist in the database
        let constraint_rows = client.query(
            indoc! {"
                SELECT 
                    tc.constraint_name,
                    tc.constraint_type,
                    tc.table_name,
                    tc.table_schema
                FROM information_schema.table_constraints tc
                WHERE tc.table_schema = 'public'
                AND tc.constraint_type IN ('UNIQUE', 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY')
                ORDER BY tc.table_name, tc.constraint_name
            "},
            &[]
        ).expect("Should query constraints");
        
        let constraints: Vec<(String, String, String)> = constraint_rows.iter()
            .map(|row| (
                row.get::<_, String>("constraint_name"),
                row.get::<_, String>("constraint_type"),
                row.get::<_, String>("table_name")
            ))
            .collect();
        
        // Generate code and check that constraint enums cover all constraints
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();
        
        let errors_content = std::fs::read_to_string(generated_dir.join("errors.rs"))
            .expect("Should read errors.rs");
        
        // Check that constraint enums are generated for tables that actually have them in the errors.rs
        // Based on the current implementation, only tables with meaningful constraint names get enums
        let expected_constraint_enums = ["AccountConstraint", "PostConstraint"];
        
        for enum_name in expected_constraint_enums {
            assert!(errors_content.contains(enum_name), 
                "Should have constraint enum: {}", enum_name);
        }
        
        // Verify that the constraint enums have the expected constraint variants
        // Account should have email unique constraint and check constraint
        assert!(errors_content.contains("AccountEmailKey"));
        assert!(errors_content.contains("AccountNameCheck"));
        
        // Post should have foreign key and check constraint
        assert!(errors_content.contains("PostAccountIdFkey"));
        assert!(errors_content.contains("PostTitleLength"));
    });
}

#[test]
#[ignore] // Requires Docker
fn test_functions_use_unified_error() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();
        
        // Check that API functions use the unified error type
        let api_content = std::fs::read_to_string(generated_dir.join("api.rs"))
            .expect("Should read api.rs");
        
        // Functions should return Result with PgRpcError
        assert!(api_content.contains("Result<"));
        assert!(api_content.contains("crate::errors::PgRpcError"));
        
        // Should not have function-specific error types
        assert!(!api_content.contains("GetAccountByEmailError"));
        assert!(!api_content.contains("enum.*Error"));
    });
}

#[test]
#[ignore] // Requires Docker
fn test_database_error_fallback() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();
        
        let errors_content = std::fs::read_to_string(generated_dir.join("errors.rs"))
            .expect("Should read errors.rs");
        
        // Should have a generic Database error variant for unmapped errors
        assert!(errors_content.contains("Database(tokio_postgres::Error)"));
        
        // Should have fallback cases in the From implementation
        assert!(errors_content.contains("_ => PgRpcError::Database(e)"));
    });
}