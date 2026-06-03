//! Stress tests for query introspection on advanced SQL.
//!
//! Each test exercises a real-world query shape that the existing `query_tests`
//! module doesn't cover: recursive CTEs, multi-step CTE pipelines, set-returning
//! functions (`unnest`, `generate_series`, `jsonb_to_recordset`), window
//! functions, lateral subqueries, and FULL OUTER JOIN + COALESCE.
//!
//! Properties asserted per test (in addition to test-specific shape checks):
//!   1. Cardinality — the test specifies `:one` / `:many` / `:opt` explicitly
//!      so introspection only has to match the declared shape.
//!   2. Nullability — substring checks on generated `pub <field>: <type>` lines.
//!   3. Type inference — Rust types match expectations (i32, String, Option<T>,
//!      `time::OffsetDateTime`, etc.).
//!   4. Codegen correctness — the generated `queries.rs` is parsed by
//!      `prettify` (via `syn::parse_file`) before assertions, which is itself a
//!      structural validity check. A couple of tests also run a real `cargo
//!      check` over a project that depends on the generated code.

use super::*;
use crate::integration::compile_helpers::{
    assert_compilation_success, compile_project, create_test_cargo_project, read_pretty,
};
use indoc::indoc;
use pgrpc::PgrpcBuilder;
use std::path::Path;
use tempfile::TempDir;

/// When `PGRPC_DUMP_QUERIES=1` is set, every test prints the generated row/fn
/// definitions to stderr. Lets us inspect codegen output without permanently
/// littering tests with `eprintln!`.
fn maybe_dump(code: &str) {
    if std::env::var("PGRPC_DUMP_QUERIES").is_ok() {
        for line in code.lines() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("pub struct ")
                || trimmed.starts_with("pub async fn ")
                || trimmed.starts_with("pub ")
            {
                eprintln!("DUMP {}", line);
            }
        }
    }
}

/// Build pgrpc with a single SQL file and return the path to the generated
/// `queries.rs`. Centralised so each test stays focused on the SQL + assertions.
fn generate_queries(conn_string: &str, sql: &str) -> (TempDir, String) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let sql_file = temp_dir.path().join("test.sql");
    std::fs::write(&sql_file, sql).expect("write sql file");

    let output_dir = temp_dir.path().join("generated");

    PgrpcBuilder::new()
        .connection_string(conn_string)
        .schema("public")
        .output_path(&output_dir)
        .queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file.to_string_lossy().to_string()],
        })
        .build()
        .expect("Code generation should succeed");

    let queries_file = output_dir.join("queries.rs");
    let code = read_pretty(&queries_file);
    maybe_dump(&code);
    (temp_dir, code)
}

/// Recursive CTE for tree traversal. The `depth` column comes from a literal
/// `1` (NOT NULL) recursing via `depth + 1` (still NOT NULL). `parent_id` is a
/// self-referencing FK and remains nullable for the root.
#[test]
fn test_recursive_cte_tree_traversal() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client
            .execute(
                "CREATE TABLE categories (
                    id SERIAL PRIMARY KEY,
                    parent_id INT REFERENCES categories(id),
                    name TEXT NOT NULL
                )",
                &[],
            )
            .unwrap();

        let sql = indoc! {r#"
            -- name: CategoryTree :many
            WITH RECURSIVE tree AS (
                SELECT id, parent_id, name, 1 AS depth
                FROM categories
                WHERE id = :root_id

                UNION ALL

                SELECT c.id, c.parent_id, c.name, t.depth + 1
                FROM categories c
                JOIN tree t ON c.parent_id = t.id
            )
            SELECT id, parent_id, name, depth FROM tree ORDER BY depth, id;
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        assert!(
            code.contains("pub async fn category_tree"),
            "should generate async fn"
        );
        assert!(
            code.contains("Result<Vec<CategoryTreeRow>"),
            "many query should return Vec<_>"
        );
        assert!(
            code.contains("pub struct CategoryTreeRow"),
            "should generate Row struct"
        );
        // The analyzer merges the two UNION ALL arms of the recursive CTE
        // and propagates NOT NULL only when both arms agree. `id` and `name`
        // are NOT NULL on the base relation in both arms.
        assert!(code.contains("pub id: i32"));
        assert!(code.contains("pub name: String"));
        // `parent_id` is nullable on the base relation (the root has no parent).
        assert!(code.contains("pub parent_id: Option<i32>"));
        // `depth` is computed (`1` then `depth + 1`); we don't yet trace
        // arithmetic on literals back to NOT NULL, so it stays Option.
        assert!(code.contains("pub depth: Option<i32>"));
    });
}

/// Multi-step CTE pipeline: an INSERT…RETURNING feeds a second CTE that
/// joins it to another table. Exercises:
///   * data-modifying CTE in the middle of a pipeline,
///   * `RETURNING` columns flowing through a join,
///   * a parameter (`:tag`) used by an inner CTE.
#[test]
fn test_multi_cte_pipeline_with_data_modification() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client
            .execute(
                "CREATE TABLE articles (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    body TEXT
                )",
                &[],
            )
            .unwrap();
        client
            .execute(
                "CREATE TABLE tags (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE
                )",
                &[],
            )
            .unwrap();
        client
            .execute(
                "CREATE TABLE article_tags (
                    article_id INT NOT NULL REFERENCES articles(id),
                    tag_id INT NOT NULL REFERENCES tags(id),
                    PRIMARY KEY (article_id, tag_id)
                )",
                &[],
            )
            .unwrap();

        let sql = indoc! {r#"
            -- name: PublishWithTag :one
            WITH new_article AS (
                INSERT INTO articles (title, body)
                VALUES (:title, :body)
                RETURNING id, title, body
            ),
            tag_lookup AS (
                SELECT id FROM tags WHERE name = :tag
            ),
            link AS (
                INSERT INTO article_tags (article_id, tag_id)
                SELECT new_article.id, tag_lookup.id
                FROM new_article, tag_lookup
                RETURNING article_id
            )
            SELECT
                na.id AS article_id,
                na.title,
                na.body,
                tl.id AS tag_id
            FROM new_article na
            CROSS JOIN tag_lookup tl;
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        assert!(code.contains("pub async fn publish_with_tag"));
        assert!(
            code.contains("Result<PublishWithTagRow"),
            "one query returns the row type directly"
        );
        assert!(code.contains("pub struct PublishWithTagRow"));
        assert!(code.contains("pub article_id: i32"));
        assert!(code.contains("pub title: String"));
        // `body` is a nullable column in `articles`, so it stays Option<String>.
        assert!(code.contains("pub body: Option<String>"));
        assert!(
            code.contains("pub tag_id: i32"),
            "tag_lookup.id comes from a NOT NULL PK column"
        );
    });
}

/// `unnest(...)` of multiple arrays with `WITH ORDINALITY`. Exercises:
///   * set-returning function as the FROM source,
///   * `WITH ORDINALITY` producing a `bigint` index column,
///   * NOT NULL inference for unnested scalar columns.
#[test]
fn test_unnest_with_ordinality() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // No tables needed — `unnest` operates on inline arrays.
        let _ = client;

        let sql = indoc! {r#"
            -- name: ZipNamesAndAges :many
            SELECT name, age, ord::int4 AS ord
            FROM unnest(:names::text[], :ages::int4[])
                 WITH ORDINALITY AS t(name, age, ord);
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        assert!(code.contains("pub async fn zip_names_and_ages"));
        assert!(code.contains("Result<Vec<ZipNamesAndAgesRow>"));
        assert!(code.contains("pub struct ZipNamesAndAgesRow"));
        // `unnest` propagates whatever NULLs were in the source array, so we
        // conservatively keep the columns nullable. This is the right choice —
        // proving "this array contains no NULLs" requires per-call evidence.
        assert!(code.contains("pub name: Option<String>"));
        assert!(code.contains("pub age: Option<i32>"));
        // The ordinality column from WITH ORDINALITY is always NOT NULL.
        assert!(code.contains("pub ord: i32"));
    });
}

/// `jsonb_to_recordset` with an explicit column list. The column types are
/// declared inline in the `AS t(...)` clause and must round-trip into the
/// generated struct.
#[test]
fn test_jsonb_to_recordset() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        let _ = client;

        let sql = indoc! {r#"
            -- name: ParseEvents :many
            SELECT id, kind, occurred_at
            FROM jsonb_to_recordset(:events::jsonb)
                 AS t(id int4, kind text, occurred_at timestamptz);
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        assert!(code.contains("pub async fn parse_events"));
        assert!(code.contains("Result<Vec<ParseEventsRow>"));
        assert!(code.contains("pub struct ParseEventsRow"));
        // jsonb_to_recordset columns are nullable — any row may be missing keys.
        assert!(
            code.contains("pub id: Option<i32>"),
            "json-derived columns should be Option"
        );
        assert!(code.contains("pub kind: Option<String>"));
        assert!(code.contains("pub occurred_at: Option<time::OffsetDateTime>"));
    });
}

/// Window functions: `ROW_NUMBER()` is always NOT NULL, `LAG()` is nullable
/// at the partition boundary, the aggregate `SUM() OVER` over a `bigint`
/// counter is `Option<i64>` (PG returns NULL for empty windows).
#[test]
fn test_window_functions_nullability() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client
            .execute(
                "CREATE TABLE events (
                    id SERIAL PRIMARY KEY,
                    actor_id INT NOT NULL,
                    occurred_at TIMESTAMPTZ NOT NULL,
                    points INT NOT NULL
                )",
                &[],
            )
            .unwrap();

        let sql = indoc! {r#"
            -- name: EventStream :many
            SELECT
                id,
                actor_id,
                ROW_NUMBER() OVER (PARTITION BY actor_id ORDER BY occurred_at)::int4
                    AS rn,
                LAG(occurred_at) OVER (PARTITION BY actor_id ORDER BY occurred_at)
                    AS previous_at,
                SUM(points) OVER (PARTITION BY actor_id ORDER BY occurred_at)::int4
                    AS running_total
            FROM events
            ORDER BY actor_id, occurred_at;
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        assert!(code.contains("pub async fn event_stream"));
        assert!(code.contains("Result<Vec<EventStreamRow>"));
        // `id` and `actor_id` come straight from the base table — NOT NULL on both.
        assert!(code.contains("pub id: i32"));
        assert!(code.contains("pub actor_id: i32"));
        // ROW_NUMBER() is in our NOT NULL window-function allowlist.
        assert!(code.contains("pub rn: i32"));
        // LAG with no default returns NULL at the first row of each partition.
        assert!(code.contains("pub previous_at: Option<time::OffsetDateTime>"));
        // SUM() OVER an empty window returns NULL.
        assert!(code.contains("pub running_total: Option<i32>"));
    });
}

/// FULL OUTER JOIN where each side's columns become nullable, plus a
/// COALESCE that *recovers* NOT NULL when one side is guaranteed by the
/// outer-join logic.
#[test]
fn test_full_outer_join_with_coalesce() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client
            .execute(
                "CREATE TABLE budgets (
                    department TEXT NOT NULL PRIMARY KEY,
                    amount NUMERIC NOT NULL
                )",
                &[],
            )
            .unwrap();
        client
            .execute(
                "CREATE TABLE spends (
                    department TEXT NOT NULL PRIMARY KEY,
                    amount NUMERIC NOT NULL
                )",
                &[],
            )
            .unwrap();

        let sql = indoc! {r#"
            -- name: BudgetVsSpend :many
            SELECT
                COALESCE(b.department, s.department) AS department,
                b.amount AS budget,
                s.amount AS spend,
                COALESCE(b.amount, 0::numeric) AS budget_or_zero
            FROM budgets b
            FULL OUTER JOIN spends s USING (department);
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        assert!(code.contains("pub async fn budget_vs_spend"));
        assert!(code.contains("Result<Vec<BudgetVsSpendRow>"));
        // KNOWN GAP: `COALESCE(b.department, s.department)` over a FULL OUTER
        // JOIN of `b` and `s` is provably NOT NULL — every FOJ row has at
        // least one side present — but our analyzer doesn't yet recognize
        // that pattern. It correctly notes both inputs are
        // nullable_due_to_join and conservatively marks the result nullable.
        assert!(code.contains("pub department: Option<String>"));
        // Each side becomes nullable through the FULL OUTER JOIN.
        assert!(code.contains("pub budget: Option<rust_decimal::Decimal>"));
        assert!(code.contains("pub spend: Option<rust_decimal::Decimal>"));
        // COALESCE with a NOT NULL literal fallback recovers NOT NULL — the
        // literal `0::numeric` is fully NOT NULL (no join can eliminate it),
        // so the existing "any arg fully NOT NULL ⇒ COALESCE NOT NULL" rule
        // applies.
        assert!(
            code.contains("pub budget_or_zero: rust_decimal::Decimal"),
            "COALESCE(_, 0) should restore NOT NULL"
        );
    });
}

/// LATERAL subquery. The lateral side returns 0 or 1 rows per outer row
/// via `ORDER BY ... LIMIT 1`, so its columns are nullable from the outer
/// query's perspective. The outer cardinality is unchanged.
#[test]
fn test_lateral_join_with_limit_one() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client
            .execute(
                "CREATE TABLE accounts (
                    id SERIAL PRIMARY KEY,
                    email TEXT NOT NULL
                )",
                &[],
            )
            .unwrap();
        client
            .execute(
                "CREATE TABLE logins (
                    id SERIAL PRIMARY KEY,
                    account_id INT NOT NULL REFERENCES accounts(id),
                    succeeded BOOLEAN NOT NULL,
                    at TIMESTAMPTZ NOT NULL
                )",
                &[],
            )
            .unwrap();

        let sql = indoc! {r#"
            -- name: AccountsWithLatestLogin :many
            SELECT
                a.id,
                a.email,
                latest.at AS latest_at,
                latest.succeeded AS latest_succeeded
            FROM accounts a
            LEFT JOIN LATERAL (
                SELECT at, succeeded
                FROM logins l
                WHERE l.account_id = a.id
                ORDER BY l.at DESC
                LIMIT 1
            ) latest ON TRUE
            ORDER BY a.id;
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        assert!(code.contains("pub async fn accounts_with_latest_login"));
        assert!(code.contains("Result<Vec<AccountsWithLatestLoginRow>"));
        assert!(code.contains("pub id: i32"));
        assert!(code.contains("pub email: String"));
        // LEFT JOIN LATERAL → lateral columns are Option<_>.
        assert!(code.contains("pub latest_at: Option<time::OffsetDateTime>"));
        assert!(code.contains("pub latest_succeeded: Option<bool>"));
    });
}

/// Arithmetic on NOT NULL operands stays NOT NULL.
///
/// `is_aexpr_nullable` already recurses into both sides for `+ - * / % ^`,
/// so a query like `SELECT count(*) + 1 FROM t` should produce a NOT NULL
/// result without any analyzer changes. This test pins that invariant.
#[test]
fn test_arithmetic_on_not_null_operands() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client
            .execute("CREATE TABLE counters (n INT NOT NULL)", &[])
            .unwrap();

        let sql = indoc! {r#"
            -- name: ArithmeticShapes :one
            SELECT
                count(*) + 1 AS plus_literal,
                (count(*) * 2)::int4 AS times_literal,
                count(*) + count(*) AS sum_of_aggs,
                length('hello') + 1 AS literal_only
            FROM counters;
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        // Every column is arithmetic on operands that are themselves NOT NULL
        // (count(*) is in our NOT NULL allowlist, integer literals are NOT NULL,
        // length() of a literal string is NOT NULL).
        assert!(code.contains("pub plus_literal: i64"));
        assert!(code.contains("pub times_literal: i32"));
        assert!(code.contains("pub sum_of_aggs: i64"));
        assert!(code.contains("pub literal_only: i32"));
    });
}

/// `(SELECT <expr>)` scalar subqueries.
///
/// Without a FROM clause the subquery always returns exactly one row, so its
/// output nullability matches the projected expression. With a FROM clause we
/// can't know in general whether any rows match, so the result stays nullable —
/// except for the special case of a single aggregate over no GROUP BY, which
/// always produces one row.
#[test]
fn test_scalar_subquery_nullability() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client
            .execute("CREATE TABLE items (id SERIAL PRIMARY KEY, label TEXT NOT NULL)", &[])
            .unwrap();

        let sql = indoc! {r#"
            -- name: ScalarSubqueries :one
            SELECT
                (SELECT 1) AS literal,
                (SELECT NOW()) AS literal_now,
                (SELECT count(*) FROM items) AS item_count,
                (SELECT max(id) FROM items) AS max_id,
                (SELECT label FROM items WHERE id = 1) AS maybe_label
            ;
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        // (SELECT <literal>) — no FROM, exactly one row, projected literal is NOT NULL.
        assert!(code.contains("pub literal: i32"));
        assert!(code.contains("pub literal_now: time::OffsetDateTime"));
        // count() over no GROUP BY: always one row, count is NOT NULL.
        assert!(code.contains("pub item_count: i64"));
        // max() over no GROUP BY: always one row, but max's own nullability is
        // nullable (returns NULL on empty input).
        assert!(code.contains("pub max_id: Option<i32>"));
        // Subquery with FROM + WHERE — could match zero rows, conservatively nullable.
        assert!(code.contains("pub maybe_label: Option<String>"));
    });
}

/// `-- @pgrpc_not_null(col1, col2)` lets the user assert columns are NOT NULL
/// when pgrpc can't prove it. Useful for unnest/jsonb_to_recordset/lateral
/// subquery output where the user's invariants are stronger than what the
/// schema can express.
#[test]
fn test_not_null_annotation_overrides_inference() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        let _ = client;

        let sql = indoc! {r#"
            -- name: UnnestWithAnnotation :many
            -- @pgrpc_not_null(name, age)
            SELECT name, age
            FROM unnest(:names::text[], :ages::int4[]) AS t(name, age);
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        // Without the annotation these would be `Option<String>` / `Option<i32>`
        // (unnest output is conservatively nullable). The annotation flips them
        // to bare `String` / `i32`.
        assert!(code.contains("pub name: String"));
        assert!(code.contains("pub age: i32"));
    });
}

/// Unqualified columns from the preserved (left) side of a LEFT JOIN that uses
/// a `USING (...)` clause must stay NOT NULL. Only the right-side table is made
/// nullable by a LEFT JOIN; columns selected without a table qualifier still
/// resolve to their owning relation and keep its base nullability.
///
/// Regression: previously every unqualified column in a `USING`-join query was
/// emitted as `Option<_>` because the analyzer couldn't attribute the bare
/// column reference to the preserved relation.
#[test]
fn test_left_join_using_preserves_left_side_not_null() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // `seller` is a right-side table joined to the pre-existing `account`
        // table via a USING (account_id) LEFT JOIN.
        client
            .execute(
                "CREATE TABLE seller (
                    account_id INT PRIMARY KEY REFERENCES account(account_id),
                    can_sell BOOLEAN NOT NULL
                )",
                &[],
            )
            .unwrap();

        let sql = indoc! {r#"
            -- name: SearchAccounts :many
            SELECT account_id,
                   name,
                   email,
                   s.can_sell
            FROM account
                 LEFT JOIN seller s USING (account_id);
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        // Preserved-side columns (unqualified, owned by `account`) stay NOT NULL.
        // `account_id` is the USING join key: it is COALESCE(account.account_id,
        // s.account_id), and the left input is NOT NULL, so it is NOT NULL too.
        assert!(code.contains("pub account_id: i32"), "account_id should be NOT NULL");
        assert!(code.contains("pub name: String"), "name should be NOT NULL");
        // Right side of the LEFT JOIN is nullable.
        assert!(code.contains("pub can_sell: Option<bool>"), "can_sell should be nullable");
    });
}

/// `array_agg(...)` under a `GROUP BY` never returns NULL: every group has at
/// least one row by construction, so the aggregate always produces a non-empty
/// array. (A bare `array_agg` over a possibly-empty set with no GROUP BY *can*
/// return NULL — that case stays nullable.)
///
/// Regression: `array_agg` was unconditionally treated as nullable, so grouped
/// aggregates came out `Option<Vec<_>>` when they can never be NULL.
#[test]
fn test_array_agg_under_group_by_is_not_null() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client
            .execute(
                "CREATE TABLE orders (
                    order_id INT NOT NULL,
                    sku TEXT NOT NULL
                )",
                &[],
            )
            .unwrap();

        let sql = indoc! {r#"
            -- name: OrderSkus :many
            SELECT order_id, array_agg(sku) AS skus
            FROM orders
            GROUP BY order_id;
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        assert!(code.contains("pub order_id: i32"));
        // array_agg under GROUP BY: each group has >=1 row, never NULL.
        assert!(
            code.contains("pub skus: Vec<String>"),
            "array_agg under GROUP BY should be NOT NULL Vec, got nullable"
        );
    });
}

/// End-to-end compilation check. Ensures the union of advanced query shapes
/// produces code that not only matches expected substrings but also passes
/// `cargo check`. Catches regressions in derive macros, trait bounds, and
/// crate use-statements that substring checks would miss.
#[test]
fn test_complex_queries_generated_code_compiles() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client
            .execute(
                "CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    parent_id INT REFERENCES items(id),
                    label TEXT NOT NULL,
                    price NUMERIC NOT NULL
                )",
                &[],
            )
            .unwrap();
        client
            .execute(
                "CREATE TABLE item_events (
                    item_id INT NOT NULL REFERENCES items(id),
                    at TIMESTAMPTZ NOT NULL,
                    note TEXT
                )",
                &[],
            )
            .unwrap();

        // A grab bag covering recursive CTE, unnest, lateral, window, and full join.
        let project = create_test_cargo_project(conn_string, vec!["public"]);
        let sql_file = project.path().join("complex.sql");
        std::fs::write(
            &sql_file,
            indoc! {r#"
                -- name: ItemTree :many
                WITH RECURSIVE t AS (
                    SELECT id, parent_id, label, 1::int4 AS depth
                    FROM items WHERE parent_id IS NULL
                    UNION ALL
                    SELECT i.id, i.parent_id, i.label, t.depth + 1
                    FROM items i JOIN t ON i.parent_id = t.id
                )
                SELECT id, parent_id, label, depth FROM t;

                -- name: ExpandLabels :many
                SELECT label
                FROM unnest(:labels::text[]) AS t(label);

                -- name: ItemsWithLastEvent :many
                SELECT i.id, i.label, e.at AS last_event_at
                FROM items i
                LEFT JOIN LATERAL (
                    SELECT at FROM item_events e
                    WHERE e.item_id = i.id
                    ORDER BY at DESC LIMIT 1
                ) e ON TRUE;
            "#},
        )
        .unwrap();

        // Regenerate so the project picks up the new queries.
        PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(project.path().join("src/generated"))
            .queries_config(pgrpc::QueriesConfig {
                paths: vec![sql_file.to_string_lossy().to_string()],
            })
            .build()
            .expect("regen should succeed");

        // Pull queries.rs into the lib so cargo check sees it.
        let lib_rs: &Path = &project.path().join("src/lib.rs");
        let mut lib = std::fs::read_to_string(lib_rs).unwrap();
        if !lib.contains("pub use generated::queries") {
            lib.push_str("\n#[allow(unused_imports)] pub use generated::queries::*;\n");
            std::fs::write(lib_rs, lib).unwrap();
        }

        let output = compile_project(project.path());
        assert_compilation_success(output);
    });
}

/// `GREATEST`/`LEAST` parse into a `MinMaxExpr` AST node (not a `FuncCall`).
/// They ignore NULL inputs and return NULL *only* when every argument is NULL,
/// so the result is NOT NULL as soon as a single argument is NOT NULL — the
/// same rule as `COALESCE`. Regression test for the analyzer previously
/// defaulting `MinMaxExpr` to nullable.
#[test]
fn test_greatest_least_nullability() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client
            .execute(
                "CREATE TABLE timestamps (
                    id INT PRIMARY KEY,
                    a TIMESTAMPTZ NOT NULL,
                    b TIMESTAMPTZ NOT NULL,
                    c TIMESTAMPTZ
                )",
                &[],
            )
            .unwrap();

        let sql = indoc! {r#"
            -- name: MinMaxNullability :many
            SELECT
                GREATEST(a, b) AS greatest_both_not_null,
                LEAST(a, b) AS least_both_not_null,
                GREATEST(a, c) AS greatest_one_nullable,
                LEAST(c) AS least_all_nullable
            FROM timestamps;
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        assert!(code.contains("pub async fn min_max_nullability"));

        // At least one NOT NULL argument ⇒ the result is NOT NULL, because
        // GREATEST/LEAST skip NULL inputs.
        assert!(
            code.contains("pub greatest_both_not_null: time::OffsetDateTime"),
            "GREATEST(not_null, not_null) should be NOT NULL"
        );
        assert!(
            code.contains("pub least_both_not_null: time::OffsetDateTime"),
            "LEAST(not_null, not_null) should be NOT NULL"
        );
        assert!(
            code.contains("pub greatest_one_nullable: time::OffsetDateTime"),
            "GREATEST(not_null, nullable) should be NOT NULL — one NOT NULL arg is enough"
        );

        // Every argument nullable ⇒ the result is nullable.
        assert!(
            code.contains("pub least_all_nullable: Option<time::OffsetDateTime>"),
            "LEAST(nullable) with no NOT NULL arg should stay nullable"
        );
    });
}

/// A cast to a domain type (`expr::currency`, where `currency` is a domain over
/// the composite `_currency`) should generate the *domain* type, matching
/// `pg_typeof` and matching a plain column reference of the same domain.
///
/// Prepared-statement metadata reports the domain's base type for a cast
/// expression, so the analyzer must recover the domain from the cast's target
/// type name. Regression test: the cast column previously generated the bare
/// base composite (`_Currency`) while a plain domain column generated
/// `Currency`.
#[test]
fn test_cast_to_domain_preserves_domain_type() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client
            .execute(
                "CREATE TYPE _currency AS (amount BIGINT, code TEXT)",
                &[],
            )
            .unwrap();
        client
            .execute("CREATE DOMAIN currency AS _currency", &[])
            .unwrap();
        client
            .execute(
                r#"CREATE TABLE "order" (
                    id INT PRIMARY KEY,
                    application_fee currency NOT NULL,
                    item_subtotal currency NOT NULL
                )"#,
                &[],
            )
            .unwrap();

        let sql = indoc! {r#"
            -- name: CaptureBreakdown :many
            SELECT
                o.id,
                LEAST(o.application_fee, o.item_subtotal)::currency AS fee_capped,
                o.item_subtotal AS plain_domain
            FROM "order" o;
        "#};

        let (_dir, code) = generate_queries(conn_string, sql);

        assert!(code.contains("pub async fn capture_breakdown"));

        // Plain domain column → domain type (this already worked).
        assert!(
            code.contains("pub plain_domain: super::public::Currency"),
            "plain domain column should be the domain type"
        );

        // Cast to the domain → the domain type, NOT the base composite.
        assert!(
            code.contains("pub fee_capped: super::public::Currency"),
            "cast to domain `currency` should generate the domain type, not the base composite"
        );
        assert!(
            !code.contains("pub fee_capped: super::public::_Currency"),
            "cast to domain must not unwrap to the base composite type"
        );
    });
}

/// A cast can target a domain in a schema we are *not* generating code for
/// (e.g. `expr::finance.currency` while only `public` is generated). The
/// DomainIndex must therefore cover all non-system schemas, independent of the
/// codegen schema list, and resolve the schema-qualified cast type name.
#[test]
fn test_cast_to_domain_in_other_schema() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client.execute("CREATE SCHEMA finance", &[]).unwrap();
        client
            .execute("CREATE TYPE finance._currency AS (amount BIGINT, code TEXT)", &[])
            .unwrap();
        client
            .execute("CREATE DOMAIN finance.currency AS finance._currency", &[])
            .unwrap();
        client
            .execute(
                r#"CREATE TABLE public."order" (
                    id INT PRIMARY KEY,
                    application_fee finance.currency NOT NULL,
                    item_subtotal finance.currency NOT NULL
                )"#,
                &[],
            )
            .unwrap();

        let sql = indoc! {r#"
            -- name: CaptureBreakdownXschema :many
            SELECT
                o.id,
                LEAST(o.application_fee, o.item_subtotal)::finance.currency AS fee_capped
            FROM public."order" o;
        "#};

        // Only generate `public` — the cast targets a domain in `finance`,
        // which is deliberately not in the codegen schema list.
        let temp_dir = TempDir::new().expect("temp dir");
        let sql_file = temp_dir.path().join("test.sql");
        std::fs::write(&sql_file, sql).expect("write sql");
        let output_dir = temp_dir.path().join("generated");
        PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir)
            .queries_config(pgrpc::QueriesConfig {
                paths: vec![sql_file.to_string_lossy().to_string()],
            })
            .build()
            .expect("codegen should succeed");
        let code = read_pretty(&output_dir.join("queries.rs"));

        // Resolved to the `finance` domain, not its base composite.
        assert!(
            code.contains("pub fee_capped: super::finance::Currency"),
            "qualified cast to a domain in a non-codegen schema should resolve to that domain. Got:\n{}",
            code
        );
        assert!(
            !code.contains("_Currency"),
            "must not unwrap the cross-schema domain to its base composite"
        );
    });
}
