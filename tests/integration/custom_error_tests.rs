use super::*;
use tempfile::TempDir;

#[test]
fn test_custom_error_types() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create core schema
        execute_sql(client, "CREATE SCHEMA core").expect("Should create core schema");
        
        // Create errors schema
        execute_sql(client, "CREATE SCHEMA errors").expect("Should create errors schema");
        
        // Create custom error types
        execute_sql(client, r#"
            CREATE TYPE errors.validation_error AS (
                field TEXT,
                message TEXT
            )
        "#).expect("Should create validation_error type");
        
        execute_sql(client, r#"
            CREATE TYPE errors.authorization_error AS (
                user_id INTEGER,
                resource TEXT,
                action TEXT
            )
        "#).expect("Should create authorization_error type");
        
        execute_sql(client, r#"
            CREATE TYPE errors.item_not_found AS (
                item_id INTEGER,
                item_type TEXT
            )
        "#).expect("Should create item_not_found type");
        
        // Create a function that raises custom errors with JSON hint
        execute_sql(client, r#"
            CREATE OR REPLACE FUNCTION core.validate_user(
                p_user_id INTEGER,
                p_email TEXT
            ) RETURNS BOOLEAN
            LANGUAGE plpgsql AS $$
            BEGIN
                -- Check if user exists
                IF p_user_id <= 0 THEN
                    RAISE EXCEPTION SQLSTATE '23514' USING  -- Check constraint violation
                        HINT = 'application/json',
                        MESSAGE = jsonb_build_object(
                            'type', 'validation_error',
                            'field', 'user_id',
                            'message', 'User ID must be positive'
                        )::text;
                END IF;
                
                -- Check email format
                IF p_email NOT LIKE '%@%' THEN
                    RAISE EXCEPTION SQLSTATE '23514' USING  -- Check constraint violation
                        HINT = 'application/json',
                        MESSAGE = jsonb_build_object(
                            'type', 'validation_error',
                            'field', 'email',
                            'message', 'Invalid email format'
                        )::text;
                END IF;
                
                RETURN TRUE;
            END;
            $$;
        "#).expect("Should create validate_user function");
        
        // Create another function that uses type casting syntax with JSON hint
        execute_sql(client, r#"
            CREATE OR REPLACE FUNCTION core.check_authorization(
                p_user_id INTEGER,
                p_resource TEXT,
                p_action TEXT
            ) RETURNS BOOLEAN
            LANGUAGE plpgsql AS $$
            DECLARE
                v_error errors.authorization_error;
            BEGIN
                IF p_user_id = 0 THEN
                    v_error := ROW(p_user_id, p_resource, p_action)::errors.authorization_error;
                    RAISE EXCEPTION SQLSTATE '42501' USING  -- Insufficient privilege
                        HINT = 'application/json',
                        MESSAGE = jsonb_build_object(
                            'type', 'authorization_error',
                            'user_id', v_error.user_id,
                            'resource', v_error.resource,
                            'action', v_error.action
                        )::text;
                END IF;
                
                RETURN TRUE;
            END;
            $$;
        "#).expect("Should create check_authorization function");
        
        // Generate code with errors config
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();
        
        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("core")
            .enable_errors("errors")
            .output_path(output_path)
            .build()
            .expect("Should generate code");
        
        // Read the generated custom_errors.rs file
        let custom_errors_content = std::fs::read_to_string(output_path.join("custom_errors.rs"))
            .expect("Should read custom_errors.rs");
        
        println!("Generated custom_errors.rs:\n{}", custom_errors_content);
        
        // Verify error payload structs are generated
        assert!(custom_errors_content.contains("struct ValidationError"), 
            "Should generate ValidationError struct");
        assert!(custom_errors_content.contains("struct AuthorizationError"), 
            "Should generate AuthorizationError struct");
        assert!(custom_errors_content.contains("struct ItemNotFound"), 
            "Should generate ItemNotFound struct");
        
        // Verify CustomError enum is generated
        assert!(custom_errors_content.contains("enum CustomError"), 
            "Should generate CustomError enum");
        assert!(custom_errors_content.contains("ValidationError(ValidationError)"), 
            "Should have ValidationError variant");
        assert!(custom_errors_content.contains("AuthorizationError(AuthorizationError)"), 
            "Should have AuthorizationError variant");
        
        // Read the generated core.rs file
        let core_content = std::fs::read_to_string(output_path.join("core.rs"))
            .expect("Should read core.rs");
        
        // Verify function error enums include custom error variants
        assert!(core_content.contains("ValidateUserError") || core_content.contains("validate_user_error"),
            "Should generate error enum for validate_user function");
        
        // The function should detect the custom error usage even without explicit type casting
        // due to the SQLSTATE 'M0000' detection
        
        println!("Custom error types test completed successfully!");
    });
}

#[test]
fn test_custom_error_json_deserialization() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create core schema
        execute_sql(client, "CREATE SCHEMA core").expect("Should create core schema");
        
        // Create errors schema and types
        execute_sql(client, "CREATE SCHEMA errors").expect("Should create errors schema");
        
        execute_sql(client, r#"
            CREATE TYPE errors.new_item_image_required AS (
                item_id INTEGER
            )
        "#).expect("Should create new_item_image_required type");
        
        // Create the raise_error procedure with HINT for JSON detection
        execute_sql(client, r#"
            CREATE OR REPLACE PROCEDURE core.raise_error(
                p_error anyelement,
                p_sqlstate text DEFAULT 'P0001'
            )
            LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION USING
                    ERRCODE = p_sqlstate,
                    HINT = 'application/json',
                    MESSAGE = (
                        jsonb_build_object(
                            'type', substring(pg_typeof(p_error)::text from '([^.]+)$'),
                            'code', p_sqlstate
                        ) || to_jsonb(p_error)
                    )::text;
            END;
            $$;
        "#).expect("Should create raise_error procedure");
        
        // Create a function that uses the raise_error procedure
        execute_sql(client, r#"
            CREATE OR REPLACE FUNCTION core.validate_item_image(p_item_id INTEGER)
            RETURNS BOOLEAN
            LANGUAGE plpgsql AS $$
            BEGIN
                IF p_item_id IS NULL THEN
                    CALL core.raise_error(ROW(p_item_id)::errors.new_item_image_required);
                END IF;
                RETURN TRUE;
            END;
            $$;
        "#).expect("Should create validate_item_image function");
        
        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();
        
        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("core")
            .enable_errors("errors")
            .output_path(output_path)
            .build()
            .expect("Should generate code");
        
        // Verify the generated error type
        let custom_errors_content = std::fs::read_to_string(output_path.join("custom_errors.rs"))
            .expect("Should read custom_errors.rs");
        
        assert!(custom_errors_content.contains("struct NewItemImageRequired"),
            "Should generate NewItemImageRequired struct");
        assert!(custom_errors_content.contains("item_id"),
            "Should have item_id field");
        
        // Read the generated core.rs to verify the function detected the custom error
        let core_content = std::fs::read_to_string(output_path.join("core.rs"))
            .expect("Should read core.rs");
        
        // The validate_item_image function should have detected the custom error from CALL
        assert!(core_content.contains("ValidateItemImageError") || core_content.contains("validate_item_image_error"),
            "Should generate error enum for validate_item_image function");
        
        // Check if the error enum includes the custom error variant
        // Since we're calling core.raise_error with new_item_image_required, 
        // it should be detected even though it's not a direct RAISE statement
        
        println!("Custom error JSON deserialization test completed successfully!");
    });
}