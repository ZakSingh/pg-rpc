//! Integration tests for CHECK-constraint-driven nullability refinement.
//!
//! The feature: when a SELECT's WHERE clause narrows a row down to a subset
//! that a biconditional CHECK constraint proves has a column non-NULL, pgrpc
//! should generate `T` (not `Option<T>`) for that column in the query-result
//! struct.
//!
//! These tests are written as full end-to-end runs: spin up Postgres, define
//! an STI-style product table with biconditional CHECKs, point pgrpc at a
//! query file, and inspect the generated Rust.

use super::*;
use crate::integration::compile_helpers::read_pretty;
use pgrpc::*;
use tempfile::TempDir;

/// Build the STI product schema used by every test in this module.
///
/// One discriminator (`product_type`), four variants, biconditional CHECKs
/// linking each subtype-specific column to its discriminator value. This is
/// the exact pattern the real-world product table uses.
fn setup_product_schema(client: &mut postgres::Client) {
    client
        .batch_execute(
            r#"
            CREATE TYPE product_type AS ENUM ('miniature', 'paint', 'book', 'misc');

            CREATE TABLE product (
                product_id     SERIAL PRIMARY KEY,
                name           TEXT NOT NULL,
                product_type   product_type NOT NULL,

                mini_material  TEXT,
                mini_scale_mm  INTEGER,

                paint_color    TEXT,
                paint_ml       INTEGER,

                book_format    TEXT,
                book_pages     INTEGER,

                CONSTRAINT mini_material_not_null
                    CHECK ((product_type = 'miniature') = (mini_material IS NOT NULL)),
                CONSTRAINT paint_color_not_null
                    CHECK ((product_type = 'paint') = (paint_color IS NOT NULL)),
                CONSTRAINT book_format_not_null
                    CHECK ((product_type = 'book') = (book_format IS NOT NULL))
            );
            "#,
        )
        .expect("Failed to set up product schema");
}

/// Generate code for the given SQL into a temp directory and return the
/// prettified contents of queries.rs.
fn generate_queries_rs(conn_string: &str, sql: &str) -> String {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let sql_file_path = temp_dir.path().join("test.sql");
    std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

    let output_dir = temp_dir.path().join("generated");
    let builder = PgrpcBuilder::new()
        .connection_string(conn_string)
        .schema("public")
        .output_path(&output_dir)
        .infer_view_nullability(true)
        .queries_config(QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

    builder.build().expect("Code generation should succeed");
    read_pretty(output_dir.join("queries.rs"))
}

/// Find a struct definition in the generated code and return just its body.
/// Panics if the struct is missing, since that's always a real failure here.
fn extract_struct(code: &str, struct_name: &str) -> String {
    let needle = format!("pub struct {}", struct_name);
    let start = code
        .find(&needle)
        .unwrap_or_else(|| panic!("Could not find struct `{}` in generated code:\n{}", struct_name, code));
    let after = &code[start..];
    let end = after.find('}').unwrap_or(after.len()) + 1;
    after[..end].to_string()
}

/// `WHERE product_type = 'paint'` is the canonical case: the biconditional CHECK
/// on `paint_color` proves that column non-NULL whenever this predicate holds.
#[test]
fn where_eq_paint_refines_paint_color_to_non_option() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_product_schema(client);

        let sql = r#"
-- name: GetPaints :many
SELECT product_id, name, product_type, paint_color
FROM product
WHERE product_type = 'paint';
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetPaintsRow");
        println!("Generated GetPaintsRow:\n{}", s);

        // Always-NOT NULL columns should stay non-Option.
        assert!(s.contains("pub product_id: i32"), "product_id should be NOT NULL: {}", s);
        assert!(s.contains("pub name: String"), "name should be NOT NULL: {}", s);

        // The refinement payoff: paint_color is column-level NULL on the base
        // table, but the WHERE + CHECK combo proves it non-NULL here.
        assert!(
            s.contains("pub paint_color: String"),
            "paint_color should be refined to String (not Option<String>) under \
             WHERE product_type = 'paint': {}",
            s
        );
        assert!(
            !s.contains("paint_color: Option"),
            "paint_color must not be Option<…>: {}",
            s
        );
    });
}

/// Without a discriminator predicate, pgrpc must not refine subtype columns —
/// they remain `Option<…>` exactly as declared.
#[test]
fn no_where_keeps_subtype_columns_optional() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_product_schema(client);

        let sql = r#"
-- name: GetAllProducts :many
SELECT product_id, name, product_type, paint_color, mini_material
FROM product;
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetAllProductsRow");
        println!("Generated GetAllProductsRow:\n{}", s);

        // No WHERE = no refinement.
        assert!(
            s.contains("pub paint_color: Option<String>"),
            "paint_color should stay Option<String> without a WHERE clause: {}",
            s
        );
        assert!(
            s.contains("pub mini_material: Option<String>"),
            "mini_material should stay Option<String>: {}",
            s
        );
    });
}

/// `WHERE product_type = 'paint'` proves `paint_color` non-NULL, but it doesn't
/// say anything about `mini_material` — that one stays optional.
#[test]
fn refinement_is_scoped_to_matching_column() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_product_schema(client);

        let sql = r#"
-- name: GetPaintsWithMini :many
SELECT product_id, paint_color, mini_material
FROM product
WHERE product_type = 'paint';
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetPaintsWithMiniRow");
        println!("Generated GetPaintsWithMiniRow:\n{}", s);

        assert!(
            s.contains("pub paint_color: String"),
            "paint_color must be refined to String: {}",
            s
        );
        // Under product_type='paint', the mini_material biconditional in fact
        // proves mini_material IS NULL — but we don't currently emit IS NULL
        // facts (we'd need a "this column is NULL" representation). For now we
        // verify the conservative behavior: stays Option<…>, not over-refined.
        assert!(
            s.contains("pub mini_material: Option<String>"),
            "mini_material should remain Option<String> (no over-refinement): {}",
            s
        );
    });
}

/// An unrecognized predicate (e.g. `product_id > 100`) must not enable any
/// refinement — pgrpc should only act on patterns it understands.
#[test]
fn unrecognized_predicate_does_not_refine() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_product_schema(client);

        let sql = r#"
-- name: GetExpensive :many
SELECT product_id, paint_color
FROM product
WHERE product_id > 100;
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetExpensiveRow");
        println!("Generated GetExpensiveRow:\n{}", s);

        assert!(
            s.contains("pub paint_color: Option<String>"),
            "paint_color should stay Option<String> when WHERE is irrelevant: {}",
            s
        );
    });
}

/// An OR in the WHERE clause must not produce a definite refinement, even if
/// each disjunct on its own would. `product_type = 'paint' OR product_type = 'book'`
/// doesn't prove anything column-level.
#[test]
fn or_predicate_does_not_refine() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_product_schema(client);

        let sql = r#"
-- name: GetPaintOrBook :many
SELECT product_id, paint_color, book_format
FROM product
WHERE product_type = 'paint' OR product_type = 'book';
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetPaintOrBookRow");
        println!("Generated GetPaintOrBookRow:\n{}", s);

        assert!(
            s.contains("pub paint_color: Option<String>"),
            "paint_color should stay optional under OR predicate: {}",
            s
        );
        assert!(
            s.contains("pub book_format: Option<String>"),
            "book_format should stay optional under OR predicate: {}",
            s
        );
    });
}

/// Multiple AND conjuncts: an irrelevant one alongside the discriminator
/// shouldn't block refinement on the discriminator-driven column.
#[test]
fn and_conjuncts_compose() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_product_schema(client);

        let sql = r#"
-- name: GetExpensivePaints :many
SELECT product_id, paint_color
FROM product
WHERE product_type = 'paint' AND product_id > 100;
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetExpensivePaintsRow");
        println!("Generated GetExpensivePaintsRow:\n{}", s);

        assert!(
            s.contains("pub paint_color: String"),
            "paint_color should be refined when discriminator predicate is one \
             of multiple AND conjuncts: {}",
            s
        );
    });
}

/// Set up a miniswap-style `item` table whose CHECK constraints encode
/// NULL-keyed dependencies (biconditional + implication).
fn setup_item_schema(client: &mut postgres::Client) {
    client
        .batch_execute(
            r#"
            CREATE TABLE item (
                item_id        SERIAL PRIMARY KEY,
                name           TEXT NOT NULL,

                sync_src       TEXT,
                last_synced_at TIMESTAMPTZ,
                external_sku   TEXT,

                weight_limit       NUMERIC,
                weight_limit_unit  TEXT,

                CONSTRAINT sync_src_requires_last_synced
                    CHECK ((sync_src IS NULL) = (last_synced_at IS NULL)),
                CONSTRAINT sync_src_requires_sku
                    CHECK ((sync_src IS NULL) OR (external_sku IS NOT NULL)),
                CONSTRAINT weight_limit_unit_consistency
                    CHECK ((weight_limit IS NULL) OR (weight_limit_unit IS NOT NULL))
            );
            "#,
        )
        .expect("Failed to set up item schema");
}

/// `WHERE sync_src IS NOT NULL` against a biconditional CHECK
/// `(sync_src IS NULL) = (last_synced_at IS NULL)` should refine
/// `last_synced_at` to non-Option.
#[test]
fn where_is_not_null_refines_biconditional_partner() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_item_schema(client);

        let sql = r#"
-- name: GetSyncedItems :many
SELECT item_id, name, sync_src, last_synced_at
FROM item
WHERE sync_src IS NOT NULL;
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetSyncedItemsRow");
        println!("Generated GetSyncedItemsRow:\n{}", s);

        assert!(
            s.contains("pub last_synced_at: time::OffsetDateTime"),
            "last_synced_at should be refined to non-Option under \
             WHERE sync_src IS NOT NULL: {}",
            s
        );
        // sync_src in the SELECT list also becomes non-Option because the WHERE
        // directly proves it.
        assert!(
            s.contains("pub sync_src: String"),
            "sync_src itself should be non-Option (WHERE proves it): {}",
            s
        );
    });
}

/// The reverse direction of the biconditional should also work:
/// `WHERE last_synced_at IS NOT NULL` proves `sync_src IS NOT NULL`.
#[test]
fn biconditional_refinement_works_both_directions() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_item_schema(client);

        let sql = r#"
-- name: GetItemsWithSyncTime :many
SELECT item_id, sync_src, last_synced_at
FROM item
WHERE last_synced_at IS NOT NULL;
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetItemsWithSyncTimeRow");
        println!("Generated GetItemsWithSyncTimeRow:\n{}", s);

        assert!(
            s.contains("pub sync_src: String"),
            "sync_src should be refined under WHERE last_synced_at IS NOT NULL \
             (the biconditional applies in both directions): {}",
            s
        );
    });
}

/// `WHERE sync_src IS NOT NULL` against a one-way implication
/// `(sync_src IS NULL) OR (external_sku IS NOT NULL)` should refine
/// `external_sku` to non-Option.
#[test]
fn where_is_not_null_refines_implication_target() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_item_schema(client);

        let sql = r#"
-- name: GetSyncedItemSkus :many
SELECT item_id, sync_src, external_sku
FROM item
WHERE sync_src IS NOT NULL;
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetSyncedItemSkusRow");
        println!("Generated GetSyncedItemSkusRow:\n{}", s);

        assert!(
            s.contains("pub external_sku: String"),
            "external_sku should be refined under WHERE sync_src IS NOT NULL \
             via the one-way implication: {}",
            s
        );
    });
}

/// The implication is one-way: knowing `external_sku IS NOT NULL` does NOT
/// prove `sync_src IS NOT NULL`. Don't over-refine.
#[test]
fn one_way_implication_does_not_reverse() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_item_schema(client);

        let sql = r#"
-- name: GetItemsWithSku :many
SELECT item_id, sync_src, external_sku
FROM item
WHERE external_sku IS NOT NULL;
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetItemsWithSkuRow");
        println!("Generated GetItemsWithSkuRow:\n{}", s);

        // external_sku is directly proven non-NULL by the WHERE.
        assert!(s.contains("pub external_sku: String"), "{}", s);
        // sync_src must remain Option — the implication doesn't run backwards.
        assert!(
            s.contains("pub sync_src: Option<String>"),
            "sync_src must NOT be refined — the implication is one-way: {}",
            s
        );
    });
}

/// The miniswap `weight_limit_unit_consistency` pattern.
#[test]
fn weight_limit_consistency_pattern() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_item_schema(client);

        let sql = r#"
-- name: GetItemsWithWeightLimit :many
SELECT item_id, weight_limit, weight_limit_unit
FROM item
WHERE weight_limit IS NOT NULL;
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetItemsWithWeightLimitRow");
        println!("Generated GetItemsWithWeightLimitRow:\n{}", s);

        assert!(
            s.contains("pub weight_limit_unit: String"),
            "weight_limit_unit should be refined under WHERE weight_limit IS NOT NULL: {}",
            s
        );
    });
}

/// Mixing an equality predicate and an IS NOT NULL predicate on the same query.
/// Each should independently refine the columns its CHECK proves.
#[test]
fn mixed_equality_and_is_not_null_predicates() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Reuse product schema for paint_color refinement, but extend it with a
        // NULL-keyed dependency to test both refinement families together.
        client
            .batch_execute(
                r#"
                CREATE TYPE product_type AS ENUM ('paint', 'misc');
                CREATE TABLE product (
                    product_id      SERIAL PRIMARY KEY,
                    product_type    product_type NOT NULL,
                    paint_color     TEXT,
                    notes_id        INTEGER,
                    notes_body      TEXT,
                    CONSTRAINT paint_color_not_null
                        CHECK ((product_type = 'paint') = (paint_color IS NOT NULL)),
                    CONSTRAINT notes_consistency
                        CHECK ((notes_id IS NULL) = (notes_body IS NULL))
                );
                "#,
            )
            .expect("Failed to set up mixed-pattern schema");

        let sql = r#"
-- name: GetPaintsWithNotes :many
SELECT product_id, paint_color, notes_body
FROM product
WHERE product_type = 'paint' AND notes_id IS NOT NULL;
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetPaintsWithNotesRow");
        println!("Generated GetPaintsWithNotesRow:\n{}", s);

        assert!(
            s.contains("pub paint_color: String"),
            "paint_color should be refined via the equality predicate: {}",
            s
        );
        assert!(
            s.contains("pub notes_body: String"),
            "notes_body should be refined via the IS NOT NULL predicate: {}",
            s
        );
    });
}

/// `WHERE alias.product_type = 'paint'` should refine when the qualifier matches
/// the FROM alias.
#[test]
fn refinement_works_with_aliased_table() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_product_schema(client);

        let sql = r#"
-- name: GetPaintsAliased :many
SELECT p.product_id, p.paint_color
FROM product p
WHERE p.product_type = 'paint';
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetPaintsAliasedRow");
        println!("Generated GetPaintsAliasedRow:\n{}", s);

        assert!(
            s.contains("pub paint_color: String"),
            "paint_color should be refined via aliased qualifier `p.product_type`: {}",
            s
        );
    });
}

/// Set up a miniswap-style `order` table whose payment-status discriminator
/// uses a CASE-keyed CHECK constraint rather than per-column biconditionals.
fn setup_order_schema(client: &mut postgres::Client) {
    client
        .batch_execute(
            r#"
            CREATE TYPE order_payment_status AS ENUM
                ('pending', 'authorized', 'captured', 'canceled');

            CREATE TABLE "order" (
                order_id                 SERIAL PRIMARY KEY,
                payment_status           order_payment_status NOT NULL,

                payment_intent_id        TEXT,
                charge_id                TEXT,
                authorization_expires_at TIMESTAMPTZ,
                payment_captured_at      TIMESTAMPTZ,
                credit_transfer_id       TEXT,
                cancellation_reason      TEXT,

                CONSTRAINT order_payment_fields_check CHECK (
                    CASE payment_status
                        WHEN 'authorized' THEN (
                            payment_intent_id IS NOT NULL
                            AND charge_id IS NOT NULL
                            AND authorization_expires_at IS NOT NULL
                        )
                        WHEN 'captured' THEN (
                            payment_captured_at IS NOT NULL
                            AND (
                                (payment_intent_id IS NOT NULL AND charge_id IS NOT NULL)
                                OR credit_transfer_id IS NOT NULL
                            )
                        )
                        WHEN 'pending' THEN (
                            payment_intent_id IS NULL
                            AND charge_id IS NULL
                            AND authorization_expires_at IS NULL
                            AND payment_captured_at IS NULL
                        )
                        WHEN 'canceled' THEN cancellation_reason IS NOT NULL
                        ELSE true
                    END
                )
            );
            "#,
        )
        .expect("Failed to set up order schema");
}

/// `WHERE payment_status = 'authorized'` against the CASE-keyed CHECK should
/// refine all three columns the 'authorized' arm proves non-NULL.
#[test]
fn case_arm_refines_all_conjuncts() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_order_schema(client);

        let sql = r#"
-- name: GetAuthorizedOrders :many
SELECT order_id, payment_intent_id, charge_id, authorization_expires_at
FROM "order"
WHERE payment_status = 'authorized';
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetAuthorizedOrdersRow");
        println!("Generated GetAuthorizedOrdersRow:\n{}", s);

        assert!(
            s.contains("pub payment_intent_id: String"),
            "payment_intent_id should be refined to String: {}",
            s
        );
        assert!(
            s.contains("pub charge_id: String"),
            "charge_id should be refined to String: {}",
            s
        );
        assert!(
            s.contains("pub authorization_expires_at: time::OffsetDateTime"),
            "authorization_expires_at should be refined to non-Option time: {}",
            s
        );
    });
}

/// A different discriminator value picks a different arm. `'canceled'` only
/// proves `cancellation_reason` non-NULL — payment_* must stay Option.
#[test]
fn case_refinement_picks_matching_arm() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_order_schema(client);

        let sql = r#"
-- name: GetCanceledOrders :many
SELECT order_id, payment_intent_id, cancellation_reason
FROM "order"
WHERE payment_status = 'canceled';
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetCanceledOrdersRow");
        println!("Generated GetCanceledOrdersRow:\n{}", s);

        assert!(
            s.contains("pub cancellation_reason: String"),
            "cancellation_reason should be refined under 'canceled' arm: {}",
            s
        );
        assert!(
            s.contains("pub payment_intent_id: Option<String>"),
            "payment_intent_id must NOT be refined under 'canceled' arm: {}",
            s
        );
    });
}

/// The 'captured' arm has an OR inside it, so only the unconditional
/// `payment_captured_at IS NOT NULL` conjunct should refine. The OR-branch
/// columns (`payment_intent_id`, `charge_id`, `credit_transfer_id`) must
/// remain Option since none is individually guaranteed.
#[test]
fn case_arm_with_or_only_refines_unconditional_conjuncts() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_order_schema(client);

        let sql = r#"
-- name: GetCapturedOrders :many
SELECT order_id, payment_captured_at, payment_intent_id, credit_transfer_id
FROM "order"
WHERE payment_status = 'captured';
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetCapturedOrdersRow");
        println!("Generated GetCapturedOrdersRow:\n{}", s);

        assert!(
            s.contains("pub payment_captured_at: time::OffsetDateTime"),
            "payment_captured_at should be refined (unconditional conjunct): {}",
            s
        );
        assert!(
            s.contains("pub payment_intent_id: Option<String>"),
            "payment_intent_id must stay Option (only proven by the OR-branch): {}",
            s
        );
        assert!(
            s.contains("pub credit_transfer_id: Option<String>"),
            "credit_transfer_id must stay Option (only proven by the OR-branch): {}",
            s
        );
    });
}

/// No WHERE on the discriminator = no refinement from the CASE.
#[test]
fn case_no_where_no_refinement() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_order_schema(client);

        let sql = r#"
-- name: GetAllOrders :many
SELECT order_id, payment_intent_id, cancellation_reason
FROM "order";
"#;

        let code = generate_queries_rs(conn_string, sql);
        let s = extract_struct(&code, "GetAllOrdersRow");
        println!("Generated GetAllOrdersRow:\n{}", s);

        assert!(
            s.contains("pub payment_intent_id: Option<String>"),
            "payment_intent_id should stay Option without a WHERE: {}",
            s
        );
        assert!(
            s.contains("pub cancellation_reason: Option<String>"),
            "cancellation_reason should stay Option without a WHERE: {}",
            s
        );
    });
}
