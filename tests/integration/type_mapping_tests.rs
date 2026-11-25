//! Base type mappings that have to agree with what `postgres-types` accepts,
//! types the generator cannot map at all, and the derives chosen for domains.

use super::*;
use crate::integration::compile_helpers::{assert_compilation_success, compile_project, read_pretty};
use crate::integration::run_helpers::{generate_project, queries_config, run_binary};
use indoc::indoc;

/// `timestamp`, `oid` and `"char"` decode and encode with the Rust types the
/// generator picks for them, as function results, parameters and row fields.
#[test]
fn timestamp_oid_and_char_round_trip() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema tm;
            create table tm.event (
                id int primary key,
                at timestamp not null,
                ended_at timestamp,
                kind "char" not null,
                rel oid not null
            );
            insert into tm.event values (1, '2024-01-02 03:04:05.25', null, 'x', 42);
            create view tm.event_view as select * from tm.event;

            create function tm.echo_ts(p_at timestamp) returns timestamp
                language sql as $$ select p_at $$;
            create function tm.echo_oid(p_oid oid) returns oid
                language sql as $$ select p_oid $$;
            create function tm.echo_char(p_c "char") returns "char"
                language sql as $$ select p_c $$;
            create function tm.events() returns setof tm.event_view
                language sql as $$ select * from tm.event_view $$;
            "#,
        )
        .expect("schema");

        let project = generate_project(conn_string, |b| b.schema("tm"));
        run_binary(
            &project,
            conn_string,
            "type_mapping",
            "use time::macros::datetime;",
            r#"
    let at: time::PrimitiveDateTime = datetime!(2024-01-02 03:04:05.25);
    let echoed: Option<time::PrimitiveDateTime> = tm::echo_ts().at(&at).exec(&client).await.expect("timestamp");
    assert_eq!(echoed, Some(at));

    let echoed: Option<u32> = tm::echo_oid().oid(4_000_000_000).exec(&client).await.expect("oid");
    assert_eq!(echoed, Some(4_000_000_000));

    let echoed: Option<i8> = tm::echo_char().c(b'q' as i8).exec(&client).await.expect("char");
    assert_eq!(echoed, Some(b'q' as i8));

    let events = tm::events().exec(&client).await.expect("events");
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.at, at);
    assert_eq!(event.ended_at, None);
    assert_eq!(event.kind, b'x' as i8);
    assert_eq!(event.rel, 42);

    // Timestamps without a zone serialize as ISO 8601 without an offset.
    let json = serde_json::to_value(event).expect("serialize");
    assert_eq!(json["at"], "2024-01-02T03:04:05.25");
    assert_eq!(json["ended_at"], serde_json::Value::Null);
    let back: tm::EventView = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back.at, at);
    let whole: tm::EventView = serde_json::from_value(serde_json::json!({
        "id": 1, "at": "2024-01-02T03:04:05", "kind": 120, "rel": 42
    })).expect("deserialize without fractional seconds");
    assert_eq!(whole.at, datetime!(2024-01-02 03:04:05));
"#,
        );
    });
}

/// Types with no Rust mapping (ranges, intervals, …) do not abort
/// generation: the functions, queries and columns that use them are left
/// out, and everything else still compiles and runs.
#[test]
fn unsupported_types_are_skipped() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema un;
            create domain un.span as interval;
            create type un.mixed as (id int, during tstzrange, label text);
            create table un.booking (
                id int primary key,
                during tstzrange,
                days int4multirange,
                took interval,
                price money,
                net cidr,
                mac macaddr,
                flags bit(4),
                doc xml,
                at_tz timetz,
                span un.span,
                spans interval[],
                label text not null
            );
            insert into un.booking (id, label, took) values (1, 'first', '1 hour');
            create view un.booking_view as select * from un.booking;

            create function un.bookings() returns setof un.booking_view
                language sql as $$ select * from un.booking_view $$;
            create function un.mixed(p_id int) returns un.mixed
                language sql as $$ select row(p_id, null, 'm')::un.mixed $$;
            create function un.label(p_id int) returns text
                language sql as $$ select label from un.booking where id = p_id $$;

            create function un.takes_range(p_r int4range) returns int
                language sql as $$ select 1 $$;
            create function un.returns_interval() returns interval
                language sql as $$ select interval '1 day' $$;
            create function un.returns_span() returns un.span
                language sql as $$ select interval '1 day' $$;
            create function un.returns_intervals() returns interval[]
                language sql as $$ select array[interval '1 day'] $$;
            create function un.out_money(out p_a int, out p_b money)
                language sql as $$ select 1, 2::money $$;
            "#,
        )
        .expect("schema");

        let (_queries_dir, queries) = queries_config(indoc! {"
            -- name: booking_label :one
            select label from un.booking where id = :id;

            -- name: booking_took :one
            select took from un.booking where id = :id;

            -- name: booking_by_range :many
            select id from un.booking where during && :during;
        "});

        let project = generate_project(conn_string, |b| b.schema("un").queries_config(queries));
        let un_rs = read_pretty(project.path().join("src/generated/un.rs"));
        let queries_rs = read_pretty(project.path().join("src/generated/queries.rs"));

        for skipped in [
            "fn takes_range",
            "fn returns_interval",
            "fn returns_span",
            "fn returns_intervals",
            "fn out_money",
            "struct Span",
        ] {
            assert!(!un_rs.contains(skipped), "`{skipped}` should be skipped:\n{un_rs}");
        }
        for skipped in ["fn booking_took", "fn booking_by_range"] {
            assert!(!queries_rs.contains(skipped), "`{skipped}` should be skipped");
        }
        assert!(queries_rs.contains("fn booking_label"));
        assert!(!un_rs.contains("compile_error"), "nothing should fall through:\n{un_rs}");

        run_binary(
            &project,
            conn_string,
            "unsupported",
            "",
            r#"
    let rows = un::bookings().exec(&client).await.expect("view rows");
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].id, rows[0].label.as_str()), (1, "first"));

    let mixed = un::mixed().id(5).exec(&client).await.expect("composite");
    assert_eq!((mixed.id, mixed.label.as_deref()), (Some(5), Some("m")));

    assert_eq!(un::label().id(1).exec(&client).await.expect("label").as_deref(), Some("first"));
    assert_eq!(queries::booking_label(&client, 1).await.expect("query"), "first");
"#,
        );
    });
}

/// A domain newtype only derives what its inner type can: `Vec<f64>` has no
/// `Eq`/`Hash`, and a composite has neither `PartialEq`.
#[test]
fn domain_derives_follow_the_inner_type() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema dd;
            create type dd.point as (x float8, y float8);
            create type dd.color as enum ('red', 'green');
            create domain dd.weights as float8[];
            create domain dd.ratio as float8;
            create domain dd.ratios as dd.ratio[];
            create domain dd.points as dd.point[];
            create domain dd.palette as dd.color[];
            create domain dd.tags as text[];
            create domain dd.doc as jsonb;
            create domain dd.lexemes as tsvector;
            create domain dd.started as time;

            create function dd.f(
                p_a dd.weights, p_b dd.ratios, p_c dd.points, p_d dd.palette,
                p_e dd.tags, p_f dd.doc, p_g dd.lexemes, p_h dd.started
            ) returns int language sql as $$ select 1 $$;
            "#,
        )
        .expect("schema");

        let project = generate_project(conn_string, |b| b.schema("dd"));
        add_derive_checks(project.path());
        assert_compilation_success(compile_project(project.path()));
    });
}

/// Asserts, at compile time, the traits each domain is expected to have.
fn add_derive_checks(project_dir: &std::path::Path) {
    let lib_rs = project_dir.join("src/lib.rs");
    let mut lib = std::fs::read_to_string(&lib_rs).expect("read lib.rs");
    lib.push_str(indoc! {r#"

        fn eq_hash<T: Eq + std::hash::Hash>() {}
        fn partial_eq<T: PartialEq>() {}

        #[allow(dead_code)]
        fn derive_checks() {
            partial_eq::<generated::dd::Weights>();
            partial_eq::<generated::dd::Ratios>();
            eq_hash::<generated::dd::Palette>();
            eq_hash::<generated::dd::Tags>();
            eq_hash::<generated::dd::Doc>();
            eq_hash::<generated::dd::Lexemes>();
            eq_hash::<generated::dd::Started>();
        }
    "#});
    std::fs::write(&lib_rs, lib).expect("write lib.rs");
}

/// `[types]` maps a Postgres type to a Rust type of the user's own, which
/// also rescues types pgrpc has no mapping for.
#[test]
fn configured_type_mapping_is_used() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema tmap;
            create function tmap.echo_mac(p_mac macaddr) returns macaddr
                language sql as $$ select p_mac $$;
            create function tmap.echo_span(p_span interval) returns interval
                language sql as $$ select p_span $$;
            "#,
        )
        .expect("schema");

        let generate = |configure: fn(pgrpc::PgrpcBuilder) -> pgrpc::PgrpcBuilder| {
            let out = tempfile::TempDir::new().expect("temp dir");
            configure(
                pgrpc::PgrpcBuilder::new()
                    .connection_string(conn_string)
                    .schema("tmap")
                    .output_path(out.path()),
            )
            .build()
            .expect("generation should succeed");
            crate::integration::compile_helpers::read_pretty(out.path().join("tmap.rs"))
        };

        // Unmapped: both functions use unsupported types and are left out.
        let unmapped = generate(|b| b);
        assert!(!unmapped.contains("fn echo_mac"), "{unmapped}");
        assert!(!unmapped.contains("fn echo_span"), "{unmapped}");

        // Mapped by bare name and by schema-qualified name.
        let mapped = generate(|b| {
            b.type_mapping("macaddr", "crate::net::Mac")
                .type_mapping("pg_catalog.interval", "String")
        });
        assert!(mapped.contains("fn echo_mac"), "{mapped}");
        assert!(mapped.contains("crate::net::Mac"), "{mapped}");
        assert!(mapped.contains("fn echo_span"), "{mapped}");

        // A mapping that is not a Rust type fails generation.
        let out = tempfile::TempDir::new().expect("temp dir");
        let err = pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("tmap")
            .type_mapping("macaddr", "not a type!")
            .output_path(out.path())
            .build()
            .expect_err("invalid mapping should fail");
        assert!(format!("{err:#}").contains("macaddr"), "{err:#}");
    });
}
