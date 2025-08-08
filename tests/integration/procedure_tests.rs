use super::*;
use tempfile::TempDir;

#[test]
fn test_procedure_with_no_parameters() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a simple procedure with no parameters
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE PROCEDURE public.simple_procedure()
            LANGUAGE plpgsql AS $$
            BEGIN
                -- Just a simple procedure that does nothing
                RAISE NOTICE 'Simple procedure called';
            END;
            $$;
        "#,
        )
        .expect("Should create simple procedure");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let public_content =
            std::fs::read_to_string(output_path.join("public.rs")).expect("Should read public.rs");

        // Verify the procedure is generated with CALL
        assert!(
            public_content.contains("CALL public.simple_procedure"),
            "Should generate CALL statement for procedure"
        );
        assert!(
            public_content.contains("Result<(), SimpleProc"),
            "Should return unit type for void procedure"
        );

        println!("Simple procedure test completed successfully!");
    });
}

#[test]
fn test_procedure_with_in_parameters() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a procedure with IN parameters
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE PROCEDURE public.update_user_status(
                p_user_id INTEGER,
                p_status TEXT
            )
            LANGUAGE plpgsql AS $$
            BEGIN
                -- Simulate updating user status
                RAISE NOTICE 'Updating user % to status %', p_user_id, p_status;
            END;
            $$;
        "#,
        )
        .expect("Should create procedure with IN parameters");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Check what files were generated
        let entries = std::fs::read_dir(output_path)
            .expect("Should read output directory")
            .map(|res| res.map(|e| e.file_name().to_string_lossy().into_owned()))
            .collect::<Result<Vec<_>, _>>()
            .expect("Should read directory entries");
        println!("Generated files: {:?}", entries);

        // Read the generated code
        let public_content =
            std::fs::read_to_string(output_path.join("public.rs")).expect("Should read public.rs");

        // Debug: print the full content since it seems short
        println!("Full public.rs content:\n{}", public_content);

        // Also check if procedure is in any file
        for file in &["public.rs", "mod.rs", "errors.rs"] {
            let content = std::fs::read_to_string(output_path.join(file))
                .unwrap_or_else(|_| format!("Could not read {}", file));
            if content.contains("update_user_status") {
                println!("Found update_user_status in {}", file);
            }
        }

        // Look for the function in the generated content
        let has_function = public_content.contains("pub async fn update_user_status");
        assert!(
            has_function,
            "Should generate the update_user_status function"
        );

        // Since procedures with void return generate execute() calls, the CALL might be inline
        let has_call = public_content.contains("CALL public.update_user_status")
            || public_content.contains(r#"CALL public.update_user_status"#);

        if !has_call {
            println!("CALL statement not found. Here's what we have:");
            // Find and print the function definition
            if let Some(start) = public_content.find("pub async fn update_user_status") {
                let snippet =
                    &public_content[start..start.saturating_add(500).min(public_content.len())];
                println!("{}", snippet);
            }
        }

        assert!(has_call, "Should generate CALL statement for procedure");

        println!("IN parameters procedure test completed successfully!");
    });
}

#[test]
fn test_procedure_with_single_inout_parameter() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a procedure with a single INOUT parameter (procedures don't support pure OUT)
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE PROCEDURE public.get_next_id(
                INOUT p_next_id INTEGER DEFAULT NULL
            )
            LANGUAGE plpgsql AS $$
            BEGIN
                p_next_id := 42; -- Return a fixed value for testing
            END;
            $$;
        "#,
        )
        .expect("Should create procedure with INOUT parameter");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let public_content =
            std::fs::read_to_string(output_path.join("public.rs")).expect("Should read public.rs");

        // Verify the return type is the INOUT parameter type (nullable by default)
        assert!(
            public_content.contains("Result<Option<i32>, GetNextIdError>"),
            "Should return the INOUT parameter type directly (wrapped in Option as nullable)"
        );
        assert!(
            public_content.contains("row.try_get(0)"),
            "Should extract single INOUT parameter from row"
        );

        println!("Single INOUT parameter procedure test completed successfully!");
    });
}

#[test]
fn test_procedure_with_multiple_inout_parameters() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a procedure with multiple INOUT parameters
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE PROCEDURE public.get_user_info(
                IN p_user_id INTEGER,
                INOUT p_username TEXT DEFAULT NULL,
                INOUT p_email TEXT DEFAULT NULL,
                INOUT p_active BOOLEAN DEFAULT NULL
            )
            LANGUAGE plpgsql AS $$
            BEGIN
                p_username := 'testuser';
                p_email := 'test@example.com';
                p_active := true;
            END;
            $$;
        "#,
        )
        .expect("Should create procedure with multiple INOUT parameters");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let public_content =
            std::fs::read_to_string(output_path.join("public.rs")).expect("Should read public.rs");

        // Verify struct is generated for multiple INOUT parameters
        assert!(
            public_content.contains("struct GetUserInfoOut"),
            "Should generate struct for multiple INOUT parameters"
        );
        assert!(
            public_content.contains("p_username: String"),
            "Should have username field in struct"
        );
        assert!(
            public_content.contains("p_email: String"),
            "Should have email field in struct"
        );
        assert!(
            public_content.contains("p_active: bool"),
            "Should have active field in struct"
        );
        assert!(
            public_content.contains("Result<GetUserInfoOut, GetUserInfoError>"),
            "Should return the generated struct"
        );

        println!("Multiple INOUT parameters procedure test completed successfully!");
    });
}

#[test]
fn test_procedure_with_inout_parameters() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a procedure with INOUT parameters
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE PROCEDURE public.double_value(
                INOUT p_value INTEGER
            )
            LANGUAGE plpgsql AS $$
            BEGIN
                p_value := p_value * 2;
            END;
            $$;
        "#,
        )
        .expect("Should create procedure with INOUT parameter");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let public_content =
            std::fs::read_to_string(output_path.join("public.rs")).expect("Should read public.rs");

        // Verify INOUT parameter is both input and output
        assert!(
            public_content.contains("p_value: i32"),
            "Should have value as input parameter"
        );
        assert!(
            public_content.contains("Result<i32, DoubleValueError>"),
            "Should return the INOUT parameter"
        );

        println!("INOUT parameter procedure test completed successfully!");
    });
}

#[test]
fn test_procedure_error_handling() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a procedure that can raise errors
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE PROCEDURE public.validate_input(
                p_value INTEGER
            )
            LANGUAGE plpgsql AS $$
            BEGIN
                IF p_value < 0 THEN
                    RAISE EXCEPTION 'Value must be non-negative';
                END IF;
                
                IF p_value > 100 THEN
                    RAISE EXCEPTION 'Value too large' USING ERRCODE = '23514';
                END IF;
            END;
            $$;
        "#,
        )
        .expect("Should create procedure with error handling");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let public_content =
            std::fs::read_to_string(output_path.join("public.rs")).expect("Should read public.rs");

        // Verify error enum is generated
        assert!(
            public_content.contains("enum ValidateInputError"),
            "Should generate error enum for procedure"
        );
        assert!(
            public_content.contains("P0001"),
            "Should detect default error code"
        );
        assert!(
            public_content.contains("23514") || public_content.contains("CheckViolation"),
            "Should detect specific error code"
        );

        println!("Procedure error handling test completed successfully!");
    });
}
