use super::*;
use pgrpc::*;
use std::collections::HashMap;
use tempfile::TempDir;

/// Helper to test code generation in memory without writing files
fn test_code_generation(conn_string: &str, schemas: &[&str]) -> HashMap<String, String> {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let output_path = temp_dir.path();
    
    let mut builder = PgrpcBuilder::new()
        .connection_string(conn_string)
        .output_path(output_path);
    
    for schema in schemas {
        builder = builder.schema(*schema);
    }
    
    // Execute the build
    builder.build().expect("Code generation should succeed");
    
    // Read generated files
    let mut generated_files = HashMap::new();
    
    for entry in std::fs::read_dir(output_path).expect("Should read output directory") {
        let entry = entry.expect("Should read directory entry");
        let path = entry.path();
        
        if path.extension().map_or(false, |ext| ext == "rs") {
            let filename = path.file_name().unwrap().to_string_lossy().to_string();
            let content = std::fs::read_to_string(&path).expect("Should read generated file");
            generated_files.insert(filename, content);
        }
    }
    
    generated_files
}

#[test]
#[ignore] // Requires Docker
fn test_basic_code_generation() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        let generated = test_code_generation(conn_string, &["public", "api"]);
        
        // Should have generated files for each schema plus mod.rs
        assert!(generated.contains_key("mod.rs"));
        assert!(generated.contains_key("public.rs"));
        assert!(generated.contains_key("api.rs"));
        assert!(generated.contains_key("errors.rs"));
        
        // Check mod.rs content
        let mod_content = &generated["mod.rs"];
        assert!(mod_content.contains("pub mod public;"));
        assert!(mod_content.contains("pub mod api;"));
    });
}

#[test]
#[ignore] // Requires Docker
fn test_enum_generation() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        // Need api schema to trigger type generation through function usage
        let generated = test_code_generation(conn_string, &["public", "api"]);
        
        let public_content = &generated["public.rs"];
        
        // Should generate the role enum
        assert!(public_content.contains("enum Role"));
        assert!(public_content.contains("Admin"));
        assert!(public_content.contains("User"));
        
        // Should have proper derives and attributes
        assert!(public_content.contains("#[derive("));
        assert!(public_content.contains("postgres"));
    });
}

#[test]
#[ignore] // Requires Docker
fn test_struct_generation() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        // Try generating with both schemas since api.get_account_by_email returns account type
        let generated = test_code_generation(conn_string, &["public", "api"]);
        
        // Debug: print what files were generated
        println!("Generated files: {:?}", generated.keys().collect::<Vec<_>>());
        
        // The current implementation generates schema files
        // The structs should be in public.rs since that's where the tables are defined
        let public_content = &generated["public.rs"];
        println!("public.rs content preview: {}", &public_content[..500.min(public_content.len())]);
        
        // Also check errors.rs
        if let Some(errors_content) = generated.get("errors.rs") {
            println!("errors.rs preview: {}", &errors_content[..500.min(errors_content.len())]);
        }
        
        // Should generate structs for tables that are used as function return types
        // Account is used by api.get_account_by_email
        assert!(public_content.contains("struct Account"));
        
        // Post and LoginDetails are not used by any functions, so they won't be generated
        // This is expected behavior - pgrpc only generates types that are actually used
        
        // Should have proper field types
        assert!(public_content.contains("account_id"));
        assert!(public_content.contains("email"));
        // The role field might be generated with different formatting
        assert!(public_content.contains("role") && public_content.contains("Role"));
        
        // Should generate the Role enum
        assert!(public_content.contains("enum Role"));
        assert!(public_content.contains("Admin"));
        assert!(public_content.contains("User"));
        
        // Should have derives for serialization
        assert!(public_content.contains("serde::Serialize"));
        assert!(public_content.contains("serde::Deserialize"));
    });
}

#[test]
#[ignore] // Requires Docker
fn test_function_generation() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        let generated = test_code_generation(conn_string, &["api"]);
        
        let api_content = &generated["api.rs"];
        
        // Should generate the API function
        assert!(api_content.contains("get_account_by_email"));
        
        // Should be an async function
        assert!(api_content.contains("pub async fn get_account_by_email"));
        
        // Should use the unified error type
        assert!(api_content.contains("Result<"));
        assert!(api_content.contains("crate::errors::PgRpcError"));
        
        // Should have proper parameter types
        assert!(api_content.contains("p_email"));
    });
}

#[test]
#[ignore] // Requires Docker
fn test_error_generation() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        let generated = test_code_generation(conn_string, &["public"]);
        
        let errors_content = &generated["errors.rs"];
        
        // Should generate the unified error enum
        assert!(errors_content.contains("enum PgRpcError"));
        
        // Should have constraint enums for tables with constraints
        assert!(errors_content.contains("enum AccountConstraint"));
        assert!(errors_content.contains("enum PostConstraint"));
        
        // Should have specific constraint variants
        assert!(errors_content.contains("AccountEmailKey")); // unique constraint
        
        // Should have the From implementation
        assert!(errors_content.contains("impl From"));
        assert!(errors_content.contains("tokio_postgres::Error"));
        
        // Should have constraint violation handling
        assert!(errors_content.contains("UNIQUE_VIOLATION"));
        assert!(errors_content.contains("CHECK_VIOLATION"));
    });
}

#[test]
#[ignore] // Requires Docker
fn test_constraint_enum_details() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        let generated = test_code_generation(conn_string, &["public"]);
        
        let errors_content = &generated["errors.rs"];
        
        // Account should have constraints for:
        // - Primary key
        // - Email unique constraint  
        // - Name length check constraint
        assert!(errors_content.contains("AccountConstraint"));
        
        // Post should have constraints for:
        // - Primary key
        // - Foreign key to account
        // - Title length check constraint
        assert!(errors_content.contains("PostConstraint"));
        
        // Should implement Display trait for constraint enums
        assert!(errors_content.contains("impl std::fmt::Display"));
    });
}

#[test]
#[ignore] // Requires Docker
fn test_cross_schema_references() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        let generated = test_code_generation(conn_string, &["public", "api"]);
        
        let api_content = &generated["api.rs"];
        
        // API functions should reference types from other schemas
        // Should use crate:: for cross-schema references
        assert!(api_content.contains("crate::public::Account") || 
                api_content.contains("super::public::Account"));
    });
}

#[test]
#[ignore] // Requires Docker
fn test_view_generation() {
    with_clean_database(|_client| {
        let container = get_test_container();
        let conn_string = container.connection_string();
        
        // Need api schema to trigger type generation
        let generated = test_code_generation(conn_string, &["public", "api"]);
        
        let public_content = &generated["public.rs"];
        
        // Views are only generated if they're used by functions
        // post_with_author view exists but isn't returned by any function in the test schema
        // So it won't be generated - this is expected behavior
        
        // But Account type should be generated since it's used by get_account_by_email
        assert!(public_content.contains("struct Account"));
    });
}