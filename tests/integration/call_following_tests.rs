//! Error analysis follows calls into helper functions, including helpers in
//! schemas that are indexed for analysis but not generated.

use super::*;
use crate::integration::compile_helpers::read_pretty;
use indoc::indoc;
use pgrpc::*;
use tempfile::TempDir;

fn setup(client: &mut Client) -> Result<(), postgres::Error> {
    execute_sql(
        client,
        indoc! {"
        -- `helpers` is NOT in the codegen schema list.
        CREATE SCHEMA helpers;

        CREATE TABLE helpers.widget (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            qty INT CONSTRAINT widget_qty_positive CHECK (qty >= 0)
        );

        CREATE FUNCTION helpers.insert_widget(p_name TEXT, p_qty INT) RETURNS INT
        LANGUAGE plpgsql AS $$
        DECLARE v_id INT;
        BEGIN
            INSERT INTO helpers.widget (name, qty) VALUES (p_name, p_qty) RETURNING id INTO v_id;
            IF p_qty > 100 THEN
                RAISE EXCEPTION 'too many' USING ERRCODE = 'P0404';
            END IF;
            RETURN v_id;
        END $$;

        -- SQL-language helper one level deeper.
        CREATE FUNCTION helpers.deep(p_name TEXT) RETURNS INT
        LANGUAGE sql AS $$ SELECT helpers.insert_widget(p_name, 1) $$;

        -- Generic raise helper: the ERRCODE is a parameter, so nothing static
        -- can be said about what it raises.
        CREATE FUNCTION helpers.dynamic_raise(p_code TEXT) RETURNS VOID
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'dynamic' USING ERRCODE = p_code;
        END $$;

        CREATE SCHEMA call_api;

        CREATE FUNCTION call_api.via_perform(p_name TEXT) RETURNS VOID
        LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM helpers.insert_widget(p_name, 1);
        END $$;

        CREATE FUNCTION call_api.via_assign(p_name TEXT) RETURNS INT
        LANGUAGE plpgsql AS $$
        DECLARE v INT;
        BEGIN
            v := helpers.deep(p_name);
            RETURN v;
        END $$;

        CREATE FUNCTION call_api.via_dynamic(p_code TEXT) RETURNS VOID
        LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM helpers.dynamic_raise(p_code);
        END $$;

        CREATE FUNCTION call_api.untouched() RETURNS INT
        LANGUAGE sql AS $$ SELECT 1 $$;

        -- Mutual recursion must not hang the analysis.
        CREATE FUNCTION call_api.recursive_a(n INT) RETURNS INT
        LANGUAGE plpgsql AS $$
        BEGIN
            IF n > 0 THEN RETURN call_api.recursive_b(n - 1); END IF;
            RETURN 0;
        END $$;

        CREATE FUNCTION call_api.recursive_b(n INT) RETURNS INT
        LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM helpers.insert_widget('r', n);
            RETURN call_api.recursive_a(n);
        END $$;
    "},
    )
}

/// Pull the `enum <name> { … }` block out of a prettified file.
fn enum_block<'a>(src: &'a str, name: &str) -> &'a str {
    let start = src
        .find(&format!("pub enum {name} {{"))
        .unwrap_or_else(|| panic!("enum {name} not found"));
    let end = src[start..].find("\n}\n").unwrap() + start;
    &src[start..end]
}

#[test]
fn callee_errors_surface_on_callers() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup(client).expect("setup");

        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("generated");
        PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("call_api")
            .output_path(&output_path)
            .build()
            .expect("build");

        // Analysis-only schema gets no file of its own.
        assert!(
            !output_path.join("helpers.rs").exists(),
            "helpers is not a codegen schema"
        );

        let api = read_pretty(output_path.join("call_api.rs"));

        // One level: PERFORM helpers.insert_widget(...)
        let via_perform = enum_block(&api, "ViaPerformError");
        assert!(via_perform.contains("WidgetConstraint("), "{via_perform}");
        assert!(via_perform.contains("P0404("), "{via_perform}");

        // Two levels, through an assignment and a SQL-language helper.
        let via_assign = enum_block(&api, "ViaAssignError");
        assert!(via_assign.contains("WidgetConstraint("), "{via_assign}");
        assert!(via_assign.contains("P0404("), "{via_assign}");

        // The From<tokio_postgres::Error> arms name the callee's constraints.
        assert!(api.contains("\"widget_name_key\""), "unique constraint arm");
        assert!(api.contains("\"widget_qty_positive\""), "check constraint arm");

        // A helper that raises a parameterised ERRCODE adds nothing static.
        let via_dynamic = enum_block(&api, "ViaDynamicError");
        assert!(!via_dynamic.contains("PCode("), "{via_dynamic}");
        assert!(!via_dynamic.contains("P0001("), "{via_dynamic}");

        // Functions that call nothing are unchanged.
        let untouched = enum_block(&api, "UntouchedError");
        assert!(!untouched.contains("Constraint("), "{untouched}");

        // Recursion terminates and both sides see the helper's errors.
        for name in ["RecursiveAError", "RecursiveBError"] {
            let block = enum_block(&api, name);
            assert!(block.contains("WidgetConstraint("), "{name}: {block}");
            assert!(block.contains("P0404("), "{name}: {block}");
        }

        // The constraint enum for the helper's table is still emitted into
        // errors.rs so the variants have something to refer to.
        let errors = read_pretty(output_path.join("errors.rs"));
        assert!(errors.contains("pub enum WidgetConstraint"), "{errors}");
    });
}

#[test]
fn query_files_follow_calls_too() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup(client).expect("setup");

        let temp_dir = TempDir::new().unwrap();
        let sql_path = temp_dir.path().join("queries.sql");
        std::fs::write(
            &sql_path,
            indoc! {"
                -- name: InsertViaHelper :one
                SELECT helpers.insert_widget(:name, :qty) AS id;

                -- name: InsertViaDeep :one
                SELECT id FROM helpers.deep(:name) AS t(id);

                -- name: RaiseDynamically :exec
                SELECT helpers.dynamic_raise(:code);

                -- name: PlainSelect :many
                SELECT id, name FROM helpers.widget;
            "},
        )
        .unwrap();

        let output_path = temp_dir.path().join("generated");
        PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("call_api")
            .queries_config(pgrpc::QueriesConfig {
                paths: vec![sql_path.to_string_lossy().to_string()],
            })
            .output_path(&output_path)
            .build()
            .expect("build");

        let queries = read_pretty(output_path.join("queries.rs"));

        for name in ["InsertViaHelperError", "InsertViaDeepError"] {
            let block = enum_block(&queries, name);
            assert!(block.contains("WidgetConstraint("), "{name}: {block}");
            assert!(block.contains("P0404("), "{name}: {block}");
        }
        assert!(queries.contains("\"widget_name_key\""), "unique constraint arm");

        let dynamic = enum_block(&queries, "RaiseDynamicallyError");
        assert!(!dynamic.contains("PCode("), "{dynamic}");
        assert!(!dynamic.contains("P0001("), "{dynamic}");

        let plain = enum_block(&queries, "PlainSelectError");
        assert!(!plain.contains("Constraint("), "{plain}");
    });
}
