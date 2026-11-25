use super::*;
use indoc::indoc;
use pgrpc::*;
use tempfile::TempDir;
use crate::integration::compile_helpers::{
    assert_compilation_success, compile_project, create_test_cargo_project, read_pretty,
};

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

#[test]
fn test_error_enum_generation() {
    with_isolated_database_and_container(|_client, _container, conn_string| {
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();

        // Read the errors.rs file
        let errors_content = read_pretty(generated_dir.join("errors.rs"));

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

        // From implementation no longer exists on PgRpcError (moved to function-specific errors)
        assert!(!errors_content.contains("impl From<tokio_postgres::Error> for PgRpcError"));
    });
}

#[test]
fn test_constraint_violation_mapping() {
    with_isolated_database_and_container(|_client, _container, conn_string| {
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();

        let errors_content = read_pretty(generated_dir.join("errors.rs"));

        // Constraint violation handling moved to function-specific error enums
        // Let's check that constraint enums are still generated
        assert!(errors_content.contains("AccountConstraint"));
        assert!(errors_content.contains("PostConstraint"));

        // Check that the main error enum still has variants for constraints
        assert!(errors_content.contains("AccountConstraint(AccountConstraint"));
        assert!(errors_content.contains("PostConstraint(PostConstraint"));
    });
}

#[test]
fn test_function_specific_error_enums() {
    with_isolated_database_and_container(|_client, _container, conn_string| {
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();

        // Read the api schema file which contains our test function
        let api_content =
            read_pretty(generated_dir.join("api.rs"));

        // Functions should have their own error enums
        // Looking for GetAccountByEmailError since that's our test function
        assert!(
            api_content.contains("GetAccountByEmailError"),
            "Should contain function-specific error enum for get_account_by_email"
        );

        // Function error enums should have From<tokio_postgres::Error>
        assert!(
            api_content.contains("impl From<tokio_postgres::Error> for "),
            "Should have From<tokio_postgres::Error> implementations"
        );

        // Function error enums should have Other variant for catch-all
        assert!(
            api_content.contains("Other(super::errors::PgRpcError)"),
            "Should have Other variant wrapping PgRpcError"
        );

        // Function error enums should have constraint-specific handling
        assert!(
            api_content.contains("UNIQUE_VIOLATION")
                || api_content.contains("CHECK_VIOLATION")
                || api_content.contains("Other(super::errors::PgRpcError::Database(e))"),
            "Should have constraint handling or database error fallback"
        );
    });
}

#[test]
fn test_generated_code_compiles() {
    with_isolated_database_and_container(|_client, _container, conn_string| {
        let project = create_test_cargo_project(conn_string, vec!["public", "api"]);
        assert_compilation_success(compile_project(project.path()));
    });
}

#[test]
fn test_error_enum_has_proper_traits() {
    with_isolated_database_and_container(|_client, _container, conn_string| {
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();

        let errors_content = read_pretty(generated_dir.join("errors.rs"));

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
fn test_constraint_enum_coverage() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // First, let's see what constraints actually exist in the database
        let constraint_rows = client
            .query(
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
                &[],
            )
            .expect("Should query constraints");

        let constraints: Vec<(String, String, String)> = constraint_rows
            .iter()
            .map(|row| {
                (
                    row.get::<_, String>("constraint_name"),
                    row.get::<_, String>("constraint_type"),
                    row.get::<_, String>("table_name"),
                )
            })
            .collect();
        assert!(
            constraints.iter().any(|(name, _, table)| name == "account_email_key" && table == "account"),
            "test schema should define account_email_key: {constraints:?}"
        );

        // Generate code and check that constraint enums cover all constraints
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();

        let errors_content = read_pretty(generated_dir.join("errors.rs"));

        // Check that constraint enums are generated for tables that actually have them in the errors.rs
        // Based on the current implementation, only tables with meaningful constraint names get enums
        let expected_constraint_enums = ["AccountConstraint", "PostConstraint"];

        for enum_name in expected_constraint_enums {
            assert!(
                errors_content.contains(enum_name),
                "Should have constraint enum: {}",
                enum_name
            );
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
fn test_functions_use_specific_errors() {
    with_isolated_database_and_container(|_client, _container, conn_string| {
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();

        // Check that API functions use function-specific error types
        let api_content =
            read_pretty(generated_dir.join("api.rs"));

        // Functions should return Result with function-specific error
        assert!(api_content.contains("Result<"));
        assert!(api_content.contains("GetAccountByEmailError"));

        // Should have From conversions to/from PgRpcError
        assert!(
            api_content.contains("impl From<GetAccountByEmailError> for super::errors::PgRpcError")
        );
        assert!(
            api_content.contains("impl From<super::errors::PgRpcError> for GetAccountByEmailError")
        );
    });
}

#[test]
fn test_database_error_fallback() {
    with_isolated_database_and_container(|_client, _container, conn_string| {
        let _temp_dir = setup_generated_code(conn_string);
        let generated_dir = _temp_dir.path();

        let errors_content = read_pretty(generated_dir.join("errors.rs"));

        // Should have a generic Database error variant for unmapped errors
        assert!(errors_content.contains("Database(tokio_postgres::Error)"));

        // From implementation no longer exists on PgRpcError
        // Fallback is now in function-specific error enums
    });
}

#[test]
fn test_custom_exception_ergonomics() {
    with_isolated_database_and_container(|_client, _container, conn_string| {
        let _temp_dir = create_test_cargo_project(conn_string, vec!["public", "api"]);
        let project_dir = _temp_dir.path();

        // Generate with custom exceptions using the builder API
        PgrpcBuilder::new()
            .connection_string(conn_string)
            .output_path(project_dir.join("src/generated"))
            .schema("public")
            .schema("api")
            .exception("P0001", "Custom application error")
            .exception("P0002", "Invalid user input")
            .exception("P0003", "Business rule violation")
            .build()
            .expect("Code generation should succeed");

        let errors_content = read_pretty(project_dir.join("src/generated/errors.rs"));

        // Check that custom exception variants are generated with DbError instead of String
        assert!(errors_content
            .contains("CustomApplicationError(#[source] tokio_postgres::error::DbError)"));
        assert!(
            errors_content.contains("InvalidUserInput(#[source] tokio_postgres::error::DbError)")
        );
        assert!(errors_content
            .contains("BusinessRuleViolation(#[source] tokio_postgres::error::DbError)"));

        // Check that is_* helper methods are generated for custom exceptions
        assert!(errors_content.contains("is_custom_application_error"));
        assert!(errors_content.contains("is_invalid_user_input"));
        assert!(errors_content.contains("is_business_rule_violation"));

        // Check that AsDbError trait is implemented
        assert!(errors_content.contains("impl AsDbError for PgRpcError"));

        // Check that the trait definitions are included
        assert!(errors_content.contains("trait AsDbError"));
        assert!(errors_content.contains("trait PgRpcErrorExt"));

        // From implementation for SQL codes now exists in function-specific error enums
        // The PgRpcError enum itself just has the variants

        // Check that error messages include custom descriptions in the #[error] attributes (without format strings)
        assert!(errors_content.contains("#[error(\"Custom application error\")]"));
        assert!(errors_content.contains("#[error(\"Invalid user input\")]"));
        assert!(errors_content.contains("#[error(\"Business rule violation\")]"));
    });
}

// ---------------------------------------------------------------------------
// Static error analysis: relation kinds, index kinds, statement shapes and
// referential actions.
// ---------------------------------------------------------------------------

/// Tables and functions exercising the shapes the error analysis must see.
/// The functions are never executed, only analyzed.
fn setup_shop(client: &mut Client) -> Result<(), postgres::Error> {
    execute_sql(
        client,
        indoc! {"
        CREATE SCHEMA shop;
        CREATE SCHEMA shop_api;

        -- A partitioned table: violations are reported with the partition's
        -- table and constraint names.
        CREATE TABLE shop.event (
            id INT NOT NULL,
            at DATE NOT NULL,
            PRIMARY KEY (id, at)
        ) PARTITION BY RANGE (at);
        CREATE TABLE shop.event_2024 PARTITION OF shop.event
            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

        CREATE FUNCTION shop_api.add_event(p_id INT, p_at DATE) RETURNS VOID
        LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO shop.event (id, at) VALUES (p_id, p_at);
        END $$;

        -- Partial and expression unique indexes, and a CHECK naming no column.
        CREATE TABLE shop.member (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            deleted_at TIMESTAMPTZ,
            CONSTRAINT member_not_ancient CHECK (now() > '2000-01-01')
        );
        CREATE UNIQUE INDEX member_live_email ON shop.member (email) WHERE deleted_at IS NULL;
        CREATE UNIQUE INDEX member_lower_email ON shop.member (lower(email));

        CREATE FUNCTION shop_api.create_account(p_email TEXT) RETURNS VOID
        LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO shop.member (email) VALUES (p_email);
        END $$;

        -- Writes only the predicate column of the partial index.
        CREATE FUNCTION shop_api.restore_account(p_id INT) RETURNS VOID
        LANGUAGE plpgsql AS $$
        BEGIN
            UPDATE shop.member SET deleted_at = NULL WHERE id = p_id;
        END $$;

        CREATE TABLE shop.item (
            id SERIAL PRIMARY KEY,
            sku TEXT NOT NULL UNIQUE,
            qty INT NOT NULL DEFAULT 0 CONSTRAINT item_qty_nonneg CHECK (qty >= 0),
            label TEXT NOT NULL DEFAULT 'new' CONSTRAINT item_label_present CHECK (length(label) > 0)
        );
        CREATE TABLE shop.incoming (sku TEXT, qty INT);

        -- `label` and `qty` are defaulted, not listed.
        CREATE FUNCTION shop_api.add_item(p_sku TEXT) RETURNS VOID
        LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO shop.item (sku) VALUES (p_sku);
        END $$;

        -- The SET column is not among the inserted ones.
        CREATE FUNCTION shop_api.upsert_item(p_sku TEXT, p_qty INT) RETURNS VOID
        LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO shop.item (sku) VALUES (p_sku)
            ON CONFLICT (sku) DO UPDATE SET qty = shop.item.qty + p_qty;
        END $$;

        CREATE FUNCTION shop_api.merge_items() RETURNS VOID
        LANGUAGE plpgsql AS $$
        BEGIN
            MERGE INTO shop.item t USING shop.incoming s ON t.sku = s.sku
            WHEN MATCHED THEN UPDATE SET qty = s.qty
            WHEN NOT MATCHED THEN INSERT (sku, qty) VALUES (s.sku, s.qty);
        END $$;

        -- An auto-updatable view over item.
        CREATE VIEW shop.stocked_item AS
            SELECT id, sku AS code, qty FROM shop.item WHERE qty > 0;

        CREATE FUNCTION shop_api.recode_stocked_item(p_id INT, p_code TEXT) RETURNS VOID
        LANGUAGE plpgsql AS $$
        BEGIN
            UPDATE shop.stocked_item SET code = p_code WHERE id = p_id;
        END $$;

        CREATE FUNCTION shop_api.dynamic_lookup(p_sku TEXT) RETURNS INT
        LANGUAGE plpgsql AS $$
        DECLARE v_id INT;
        BEGIN
            EXECUTE 'select id from shop.item where sku = $1' INTO STRICT v_id USING p_sku;
            RETURN v_id;
        END $$;

        CREATE FUNCTION shop_api.checked(p_n INT) RETURNS INT
        LANGUAGE plpgsql AS $$
        BEGIN
            ASSERT p_n > 0, 'p_n must be positive';
            RETURN p_n;
        END $$;

        -- parent <- child (CASCADE) <- grandchild (RESTRICT)
        -- parent <- memo (SET NULL)
        CREATE TABLE shop.parent (id INT PRIMARY KEY);
        CREATE TABLE shop.child (
            id INT PRIMARY KEY,
            parent_id INT REFERENCES shop.parent(id) ON DELETE CASCADE ON UPDATE CASCADE
        );
        CREATE TABLE shop.grandchild (
            id INT PRIMARY KEY,
            child_id INT NOT NULL REFERENCES shop.child(id) ON DELETE RESTRICT
        );
        CREATE TABLE shop.memo (
            id INT PRIMARY KEY,
            parent_id INT REFERENCES shop.parent(id) ON DELETE SET NULL,
            CONSTRAINT memo_orphans_negative CHECK (parent_id IS NOT NULL OR id < 0)
        );

        CREATE FUNCTION shop.child_delete_guard() RETURNS TRIGGER
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'child is locked' USING ERRCODE = 'P0701';
        END $$;
        CREATE TRIGGER child_delete_guard BEFORE DELETE ON shop.child
            FOR EACH ROW EXECUTE FUNCTION shop.child_delete_guard();

        CREATE FUNCTION shop.memo_update_guard() RETURNS TRIGGER
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'memo is frozen' USING ERRCODE = 'P0702';
        END $$;
        CREATE TRIGGER memo_update_guard BEFORE UPDATE ON shop.memo
            FOR EACH ROW EXECUTE FUNCTION shop.memo_update_guard();

        CREATE FUNCTION shop.child_update_guard() RETURNS TRIGGER
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'child cannot move' USING ERRCODE = 'P0703';
        END $$;
        CREATE TRIGGER child_update_guard BEFORE UPDATE ON shop.child
            FOR EACH ROW EXECUTE FUNCTION shop.child_update_guard();

        CREATE FUNCTION shop_api.delete_parent(p_id INT) RETURNS VOID
        LANGUAGE plpgsql AS $$
        BEGIN
            DELETE FROM shop.parent WHERE id = p_id;
        END $$;

        CREATE FUNCTION shop_api.renumber_parent(p_id INT, p_new_id INT) RETURNS VOID
        LANGUAGE plpgsql AS $$
        BEGIN
            UPDATE shop.parent SET id = p_new_id WHERE id = p_id;
        END $$;
    "},
    )
}

/// Generated `shop_api.rs` and `errors.rs` for [`setup_shop`].
fn generate_shop(client: &mut Client, conn_string: &str) -> (String, String) {
    setup_shop(client).expect("setup");
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("generated");
    PgrpcBuilder::new()
        .connection_string(conn_string)
        .schema("shop_api")
        .output_path(&output_path)
        .build()
        .expect("build");
    (
        read_pretty(output_path.join("shop_api.rs")),
        read_pretty(output_path.join("errors.rs")),
    )
}

/// The `pub enum <name> { … }` block of a prettified file.
fn enum_block<'a>(src: &'a str, name: &str) -> &'a str {
    let start = src
        .find(&format!("pub enum {name} {{"))
        .unwrap_or_else(|| panic!("enum {name} not found"));
    let end = src[start..].find("\n}\n").unwrap() + start;
    &src[start..end]
}

/// The `impl From<tokio_postgres::Error> for <name>` block of a prettified
/// file: the constraint names a function's errors are matched against.
fn from_impl<'a>(src: &'a str, name: &str) -> &'a str {
    let start = src
        .find(&format!("impl From<tokio_postgres::Error> for {name} {{"))
        .unwrap_or_else(|| panic!("From impl for {name} not found"));
    let end = src[start..].find("\n}\n").unwrap() + start;
    &src[start..end]
}

#[test]
fn partitioned_table_is_analyzed_with_its_partitions() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Used to abort generation: relkind 'p' hit `unimplemented!`.
        let (api, errors) = generate_shop(client, conn_string);

        let add_event = enum_block(&api, "AddEventError");
        assert!(add_event.contains("EventConstraint("), "{add_event}");
        // A duplicate key is reported as `event_2024_pkey` on `event_2024`.
        assert!(add_event.contains("Event2024Constraint("), "{add_event}");
        assert!(
            from_impl(&api, "AddEventError").contains("\"event_2024_pkey\""),
            "partition primary key arm"
        );
        assert!(errors.contains("pub enum Event2024Constraint"), "{errors}");
    });
}

#[test]
fn partial_and_expression_unique_indexes_get_variants() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        let (api, errors) = generate_shop(client, conn_string);

        let account = enum_block(&errors, "MemberConstraint");
        for variant in ["MemberLiveEmail", "MemberLowerEmail", "MemberNotAncient"] {
            assert!(account.contains(variant), "{variant} missing: {account}");
        }

        let create = from_impl(&api, "CreateAccountError");
        for name in ["member_live_email", "member_lower_email", "member_not_ancient"] {
            assert!(create.contains(&format!("\"{name}\"")), "{name} missing: {create}");
        }

        // Un-deleting writes only `deleted_at`, which neither index lists as
        // a column, and can still collide.
        let restore = from_impl(&api, "RestoreAccountError");
        for name in ["member_live_email", "member_lower_email"] {
            assert!(restore.contains(&format!("\"{name}\"")), "{name} missing: {restore}");
        }
    });
}

#[test]
fn statement_shapes_reach_the_right_constraints() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        let (api, _errors) = generate_shop(client, conn_string);

        // INSERT: constraints on defaulted columns apply.
        let add_item = from_impl(&api, "AddItemError");
        for name in ["item_sku_key", "item_qty_nonneg", "item_label_present"] {
            assert!(add_item.contains(&format!("\"{name}\"")), "{name} missing: {add_item}");
        }

        // ON CONFLICT (sku) absorbs the sku key; DO UPDATE SET qty checks qty.
        let upsert = from_impl(&api, "UpsertItemError");
        assert!(upsert.contains("\"item_qty_nonneg\""), "{upsert}");
        assert!(!upsert.contains("\"item_sku_key\""), "{upsert}");

        // MERGE writes its target.
        let merge = from_impl(&api, "MergeItemsError");
        for name in ["item_sku_key", "item_qty_nonneg"] {
            assert!(merge.contains(&format!("\"{name}\"")), "{name} missing: {merge}");
        }

        // UPDATE through a view: `code` is `item.sku`.
        let recode = enum_block(&api, "RecodeStockedItemError");
        assert!(recode.contains("ItemConstraint("), "{recode}");
        let recode = from_impl(&api, "RecodeStockedItemError");
        assert!(recode.contains("\"item_sku_key\""), "{recode}");
        assert!(!recode.contains("\"item_qty_nonneg\""), "{recode}");

        // EXECUTE ... INTO STRICT and ASSERT.
        let lookup = enum_block(&api, "DynamicLookupError");
        assert!(lookup.contains("NoDataFound("), "{lookup}");
        assert!(lookup.contains("TooManyRows("), "{lookup}");
        let checked = enum_block(&api, "CheckedError");
        assert!(checked.contains("AssertFailure("), "{checked}");
    });
}

#[test]
fn referential_actions_are_followed() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        let (api, _errors) = generate_shop(client, conn_string);

        let delete_parent = enum_block(&api, "DeleteParentError");
        // CASCADE deletes children: their DELETE trigger fires, and the
        // RESTRICT key on grandchild can block it.
        assert!(delete_parent.contains("P0701("), "{delete_parent}");
        assert!(delete_parent.contains("GrandchildConstraint("), "{delete_parent}");
        // SET NULL updates memos: their UPDATE trigger fires and their
        // constraints on the nulled column are re-checked.
        assert!(delete_parent.contains("P0702("), "{delete_parent}");
        assert!(delete_parent.contains("MemoConstraint("), "{delete_parent}");
        // Children are deleted, not updated.
        assert!(!delete_parent.contains("P0703("), "{delete_parent}");

        let arms = from_impl(&api, "DeleteParentError");
        for name in ["grandchild_child_id_fkey", "memo_orphans_negative"] {
            assert!(arms.contains(&format!("\"{name}\"")), "{name} missing: {arms}");
        }

        // ON UPDATE CASCADE rewrites child.parent_id, firing its UPDATE trigger.
        let renumber = enum_block(&api, "RenumberParentError");
        assert!(renumber.contains("P0703("), "{renumber}");
        assert!(!renumber.contains("P0701("), "{renumber}");
    });
}
