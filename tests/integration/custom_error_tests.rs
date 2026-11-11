use super::*;
use tempfile::TempDir;
use crate::integration::compile_helpers::{
    assert_compilation_success, compile_project, create_test_cargo_project_with, read_pretty,
};

/// Strip whitespace (and the trailing comma prettyplease adds before a
/// closing paren when it wraps) so substring checks survive formatting.
fn compact(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .replace(",)", ")")
}

#[test]
fn test_custom_error_types() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create core schema
        execute_sql(client, "CREATE SCHEMA core").expect("Should create core schema");

        // Create errors schema
        execute_sql(client, "CREATE SCHEMA errors").expect("Should create errors schema");

        // Create custom error types
        execute_sql(
            client,
            r#"
            CREATE TYPE errors.validation_error AS (
                field TEXT,
                message TEXT
            )
        "#,
        )
        .expect("Should create validation_error type");

        execute_sql(
            client,
            r#"
            CREATE TYPE errors.authorization_error AS (
                user_id INTEGER,
                resource TEXT,
                action TEXT
            )
        "#,
        )
        .expect("Should create authorization_error type");

        execute_sql(
            client,
            r#"
            CREATE TYPE errors.item_not_found AS (
                item_id INTEGER,
                item_type TEXT
            )
        "#,
        )
        .expect("Should create item_not_found type");

        // Create a function that raises custom errors with JSON hint
        execute_sql(
            client,
            r#"
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
        "#,
        )
        .expect("Should create validate_user function");

        // Create another function that uses type casting syntax with JSON hint
        execute_sql(
            client,
            r#"
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
        "#,
        )
        .expect("Should create check_authorization function");

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
        let custom_errors_content = read_pretty(output_path.join("custom_errors.rs"));

        println!("Generated custom_errors.rs:\n{}", custom_errors_content);

        // Verify error payload structs are generated
        assert!(
            custom_errors_content.contains("struct ValidationError"),
            "Should generate ValidationError struct"
        );
        assert!(
            custom_errors_content.contains("struct AuthorizationError"),
            "Should generate AuthorizationError struct"
        );
        assert!(
            custom_errors_content.contains("struct ItemNotFound"),
            "Should generate ItemNotFound struct"
        );

        // Verify CustomError enum is generated
        assert!(
            custom_errors_content.contains("enum CustomError"),
            "Should generate CustomError enum"
        );
        assert!(
            custom_errors_content.contains("ValidationError(ValidationError)"),
            "Should have ValidationError variant"
        );
        assert!(
            custom_errors_content.contains("AuthorizationError(AuthorizationError)"),
            "Should have AuthorizationError variant"
        );

        // Read the generated core.rs file
        let core_content =
            read_pretty(output_path.join("core.rs"));

        // Hand-written `RAISE ... USING HINT = 'application/json', MESSAGE =
        // jsonb_build_object('type', '<name>', …)` is detected from the literal.
        let core_compact = compact(&core_content);
        assert!(
            core_compact.contains(&compact(
                "ValidationError(super::custom_errors::ValidationError, #[source] tokio_postgres::error::DbError)"
            )),
            "validate_user should carry the typed payload and the DbError:\n{core_content}"
        );
        assert!(
            core_compact.contains("AuthorizationError(super::custom_errors::AuthorizationError"),
            "check_authorization should carry its typed payload:\n{core_content}"
        );
        // The JSON-hint arm leads the From<tokio_postgres::Error> match.
        let validate_from = core_content
            .find("impl From<tokio_postgres::Error> for ValidateUserError")
            .expect("From impl");
        let hint_pos = core_content[validate_from..]
            .find("db_error.hint() == Some(\"application/json\")")
            .expect("hint arm");
        let code_pos = core_content[validate_from..]
            .find("SqlState::from_code(")
            .unwrap_or(usize::MAX);
        assert!(hint_pos < code_pos, "typed arm must precede SQLSTATE arms");

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

        execute_sql(
            client,
            r#"
            CREATE TYPE errors.new_item_image_required AS (
                item_id INTEGER
            )
        "#,
        )
        .expect("Should create new_item_image_required type");

        // Create the raise_error procedure with HINT for JSON detection
        execute_sql(
            client,
            r#"
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
        "#,
        )
        .expect("Should create raise_error procedure");

        // Create a function that uses the raise_error procedure, a caller of
        // it in another schema, and a trigger that raises the same way.
        execute_sql(
            client,
            r#"
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

            CREATE SCHEMA IF NOT EXISTS api;
            CREATE OR REPLACE FUNCTION api.publish_item(p_item_id INTEGER)
            RETURNS BOOLEAN
            LANGUAGE plpgsql AS $$
            BEGIN
                RETURN core.validate_item_image(p_item_id);
            END;
            $$;

            CREATE TABLE core.item (id SERIAL PRIMARY KEY, image TEXT);
            CREATE FUNCTION core.item_image_guard() RETURNS TRIGGER LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.image IS NULL THEN
                    CALL core.raise_error(ROW(NEW.id)::errors.new_item_image_required);
                END IF;
                RETURN NEW;
            END;
            $$;
            CREATE TRIGGER item_image_guard BEFORE INSERT ON core.item
                FOR EACH ROW EXECUTE FUNCTION core.item_image_guard();

            CREATE OR REPLACE FUNCTION api.insert_item(p_image TEXT) RETURNS VOID
            LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO core.item (image) VALUES (p_image);
            END;
            $$;
        "#,
        )
        .expect("Should create validate_item_image function and callers");

        // A query file calling the helper gets the typed variant too.
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let queries_path = temp_dir.path().join("queries.sql");
        std::fs::write(
            &queries_path,
            "-- name: CheckImage :one\nSELECT core.validate_item_image(:item_id) AS ok;\n",
        )
        .unwrap();

        // Generate into a compilable project so the variant shapes are checked
        // by rustc, not just by substring.
        let project = create_test_cargo_project_with(conn_string, |b| {
            b.schema("core")
                .schema("api")
                .enable_errors("errors")
                .queries_config(pgrpc::QueriesConfig {
                    paths: vec![queries_path.to_string_lossy().to_string()],
                })
        });
        let output_path = project.path().join("src").join("generated");

        // Verify the generated error type
        let custom_errors_content = read_pretty(output_path.join("custom_errors.rs"));

        assert!(
            custom_errors_content.contains("struct NewItemImageRequired"),
            "Should generate NewItemImageRequired struct"
        );
        assert!(
            custom_errors_content.contains("item_id"),
            "Should have item_id field"
        );

        // Read the generated core.rs to verify the function detected the custom error
        let core_content =
            read_pretty(output_path.join("core.rs"));

        // The validate_item_image function should have detected the custom error from CALL
        assert!(
            core_content.contains("ValidateItemImageError")
                || core_content.contains("validate_item_image_error"),
            "Should generate error enum for validate_item_image function"
        );

        let typed = compact("NewItemImageRequired(super::custom_errors::NewItemImageRequired, #[source] tokio_postgres::error::DbError)");
        assert!(
            compact(&core_content).contains(&typed),
            "validate_item_image should carry the typed payload:\n{core_content}"
        );

        // Through a call from another schema, and through a trigger on a
        // table the function writes.
        let api_content = read_pretty(output_path.join("api.rs"));
        for enum_name in ["PublishItemError", "InsertItemError"] {
            let start = api_content
                .find(&format!("pub enum {enum_name} {{"))
                .unwrap_or_else(|| panic!("{enum_name} missing"));
            let block = &api_content[start..];
            let block = &block[..block.find("\n}\n").unwrap()];
            assert!(compact(block).contains(&typed), "{enum_name}: {block}");
        }

        // Through a query file.
        let queries_content = read_pretty(output_path.join("queries.rs"));
        assert!(
            compact(&queries_content).contains(&typed),
            "CheckImageError should carry the typed payload:\n{queries_content}"
        );

        // PgRpcError keeps the DbError alongside the payload.
        let errors_content = read_pretty(output_path.join("errors.rs"));
        assert!(
            compact(&errors_content).contains(&compact(
                "CustomError(super::custom_errors::CustomError, #[source] tokio_postgres::error::DbError)"
            )),
            "{errors_content}"
        );

        assert_compilation_success(compile_project(project.path()));

        println!("Custom error JSON deserialization test completed successfully!");
    });
}
