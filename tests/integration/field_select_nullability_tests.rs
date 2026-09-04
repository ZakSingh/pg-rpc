//! Nullability of composite attribute projections in views: `(p.col).attr`.
//!
//! Postgres reports every view column as nullable, so when a view projects a
//! single attribute out of a composite-typed column the only evidence that the
//! projection can't be NULL lives on the composite type itself:
//!
//!   * `@pgrpc_not_null` on the attribute's comment,
//!   * a bulk `@pgrpc_not_null(a, b)` on the type's comment, or
//!   * a domain over the composite whose CHECK asserts `(VALUE).attr IS NOT NULL`.
//!
//! These are the same sources used when the composite becomes a generated
//! struct, so a view column projecting the whole composite and a view column
//! projecting one of its attributes must agree on nullability.

use super::*;
use crate::integration::compile_helpers::read_pretty;
use tempfile::TempDir;

const FIELD_SELECT_SCHEMA: &str = r#"
    -- The bug report's exact shape: annotated attributes, wrapped in a domain
    -- whose CHECK asserts the same thing.
    CREATE TYPE _distance AS (amount numeric, unit text);
    COMMENT ON COLUMN _distance.amount IS 'Magnitude, in `unit`. @pgrpc_not_null';
    COMMENT ON COLUMN _distance.unit   IS 'Unit of `amount`. @pgrpc_not_null';

    CREATE DOMAIN distance AS _distance
        CONSTRAINT distance_valid CHECK (
            VALUE IS NULL OR ((VALUE).amount IS NOT NULL AND (VALUE).unit IS NOT NULL));

    -- Bare composite (no domain): one annotated attribute, one not.
    CREATE TYPE _weight AS (grams numeric, note text);
    COMMENT ON COLUMN _weight.grams IS '@pgrpc_not_null';

    -- No annotations at all; only the domain CHECK proves `amount` NOT NULL.
    CREATE TYPE _price AS (amount numeric, currency text);
    CREATE DOMAIN price AS _price
        CONSTRAINT price_valid CHECK (VALUE IS NULL OR (VALUE).amount IS NOT NULL);

    -- Bulk annotation on the type rather than per attribute.
    CREATE TYPE _size AS (width numeric, height numeric, depth numeric);
    COMMENT ON TYPE _size IS 'Box dimensions. @pgrpc_not_null(width, height)';

    -- Composite nested inside a composite, both levels annotated.
    CREATE TYPE _span AS (start_at timestamptz, length distance);
    COMMENT ON COLUMN _span.length IS '@pgrpc_not_null';

    CREATE TABLE package (
        package_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        length          distance NOT NULL,
        optional_length distance,
        weight          _weight NOT NULL,
        price           price NOT NULL,
        size            _size NOT NULL,
        span            _span NOT NULL
    );

    CREATE SCHEMA pkg;

    CREATE VIEW pkg.package_dims AS
        SELECT p.package_id,
               p.length,
               (p.length).amount          AS length_amount,
               (p.length).unit            AS length_unit,
               (p.optional_length).amount AS optional_length_amount,
               (p.weight).grams           AS weight_grams,
               (p.weight).note            AS weight_note,
               (p.price).amount           AS price_amount,
               (p.price).currency         AS price_currency,
               (p.size).width             AS size_width,
               (p.size).depth             AS size_depth,
               (p.span).length.amount     AS span_length_amount,
               (p.span).start_at          AS span_start_at
        FROM package p;
"#;

fn generate(conn_string: &str) -> (TempDir, String) {
    let temp_dir = TempDir::new().expect("Should create temp directory");

    pgrpc::PgrpcBuilder::new()
        .connection_string(conn_string)
        .schema("public")
        .schema("pkg")
        .output_path(temp_dir.path())
        .infer_view_nullability(true)
        .build()
        .expect("Should generate code");

    let code = read_pretty(temp_dir.path().join("pkg.rs"));
    (temp_dir, code)
}

fn assert_field(code: &str, decl: &str) {
    assert!(
        code.contains(decl),
        "expected generated view struct to contain `{}`. Got:\n{}",
        decl,
        code
    );
}

/// Reproduction of the bug report: `(p.length).amount` where `length` is a
/// NOT NULL column typed as a domain over a composite whose attributes are
/// annotated `@pgrpc_not_null`. The projected attribute must not be `Option`.
#[test]
fn test_field_select_honors_composite_attribute_not_null() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(client, FIELD_SELECT_SCHEMA).expect("Should create schema");

        let (_dir, code) = generate(conn_string);

        assert!(code.contains("pub struct PackageDims"), "Got:\n{}", code);

        // Whole-column projection was already correct; the attribute
        // projections must agree with it.
        assert!(
            !code.contains("pub length: Option<"),
            "`length` (NOT NULL column) should not be Option. Got:\n{}",
            code
        );

        // Domain over composite, attribute annotated + domain CHECK (the report).
        assert_field(&code, "pub length_amount: rust_decimal::Decimal,");
        assert_field(&code, "pub length_unit: String,");

        // Bare composite column, annotated attribute (one hop, no domain).
        assert_field(&code, "pub weight_grams: rust_decimal::Decimal,");

        // Domain CHECK is the only evidence.
        assert_field(&code, "pub price_amount: rust_decimal::Decimal,");

        // Bulk `@pgrpc_not_null(width, height)` on the type.
        assert_field(&code, "pub size_width: rust_decimal::Decimal,");

        // Two hops through nested composites in a single projection.
        assert_field(&code, "pub span_length_amount: rust_decimal::Decimal,");
    });
}

/// The conservative default must survive: anything not provably NOT NULL
/// stays `Option`.
#[test]
fn test_field_select_stays_nullable_without_evidence() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(client, FIELD_SELECT_SCHEMA).expect("Should create schema");

        let (_dir, code) = generate(conn_string);

        // Annotated attribute, but the base column itself is nullable.
        assert_field(&code, "pub optional_length_amount: Option<rust_decimal::Decimal>,");

        // Unannotated attributes with no domain CHECK.
        assert_field(&code, "pub weight_note: Option<String>,");
        assert_field(&code, "pub price_currency: Option<String>,");

        // Attribute not listed in the bulk annotation.
        assert_field(&code, "pub size_depth: Option<rust_decimal::Decimal>,");

        // Unannotated attribute of a nested composite.
        assert_field(
            &code,
            "pub span_start_at: Option<time::OffsetDateTime>,",
        );
    });
}
