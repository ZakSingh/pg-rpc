//! Constraint violations are attributed by schema, table and constraint
//! name together: tables of one name in two schemas, one constraint name on
//! two tables, exclusion constraints and domain CHECKs.

use super::*;
use crate::integration::compile_helpers::read_pretty;
use crate::integration::run_helpers::{generate_project, queries_config, run_binary};
use indoc::indoc;

#[test]
fn violations_are_matched_by_schema_table_and_constraint() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema ca;
            create schema cb;

            -- The same table name in two schemas, with the same constraint names.
            create table ca.item (id int primary key, qty int not null, constraint positive check (qty > 0));
            create table cb.item (id int primary key, qty int not null, constraint positive check (qty > 10));
            -- The same CHECK name on another table of one schema.
            create table ca.other (id int primary key, qty int not null, constraint positive check (qty > 0));
            -- A table whose name nothing else shares keeps its plain enum name.
            create table ca.booking (
                id int primary key,
                during int4range not null,
                constraint no_overlap exclude using gist (during with &&)
            );
            create domain ca.percent as int constraint percent_range check (value between 0 and 100);
            create table ca.score (id int primary key, pct ca.percent not null);

            create function ca.add_item(p_id int, p_qty int) returns void
                language sql as $$ insert into ca.item (id, qty) values (p_id, p_qty) $$;
            create function cb.add_item(p_id int, p_qty int) returns void
                language sql as $$ insert into cb.item (id, qty) values (p_id, p_qty) $$;
            create function ca.add_both(p_id int, p_qty int, p_other_qty int) returns void
                language plpgsql as $$
                begin
                    insert into ca.item (id, qty) values (p_id, p_qty);
                    insert into ca.other (id, qty) values (p_id, p_other_qty);
                end $$;
            create function ca.book(p_id int, p_start int, p_end int) returns void
                language sql as $$ insert into ca.booking (id, during) values (p_id, int4range(p_start, p_end)) $$;
            create function ca.add_score(p_id int, p_pct int) returns void
                language sql as $$ insert into ca.score (id, pct) values (p_id, p_pct) $$;
            "#,
        )
        .expect("schema");

        let (_queries_dir, queries) = queries_config(indoc! {"
            -- name: insert_b_item :exec
            insert into cb.item (id, qty) values (:id, :qty);

            -- name: insert_booking :exec
            insert into ca.booking (id, during) values (:id, int4range(:lo, :hi));
        "});

        let project = generate_project(conn_string, |b| {
            b.schema("ca").schema("cb").queries_config(queries)
        });

        let errors_rs = read_pretty(project.path().join("src/generated/errors.rs"));
        for expected in [
            "pub enum CaItemConstraint",
            "pub enum CbItemConstraint",
            "pub enum OtherConstraint",
            "pub enum BookingConstraint",
        ] {
            assert!(errors_rs.contains(expected), "missing `{expected}`");
        }
        assert!(!errors_rs.contains("pub enum ItemConstraint"));

        run_binary(
            &project,
            conn_string,
            "constraint_scope",
            "",
            r#"
    ca::add_item().id(1).qty(5).exec(&client).await.expect("ca.item");
    cb::add_item().id(1).qty(50).exec(&client).await.expect("cb.item");

    // Same table name, same constraint name, different schemas.
    match ca::add_item().id(2).qty(0).exec(&client).await {
        Err(ca::AddItemError::CaItemConstraint(errors::CaItemConstraint::Positive, _)) => {}
        other => panic!("ca.item check: {other:?}"),
    }
    match cb::add_item().id(2).qty(5).exec(&client).await {
        Err(cb::AddItemError::CbItemConstraint(errors::CbItemConstraint::Positive, _)) => {}
        other => panic!("cb.item check: {other:?}"),
    }
    match ca::add_item().id(1).qty(5).exec(&client).await {
        Err(ca::AddItemError::CaItemConstraint(errors::CaItemConstraint::PrimaryKey, _)) => {}
        other => panic!("ca.item primary key: {other:?}"),
    }

    // One function, two tables, one constraint name: the table decides.
    match ca::add_both().id(3).qty(0).other_qty(5).exec(&client).await {
        Err(ca::AddBothError::CaItemConstraint(errors::CaItemConstraint::Positive, _)) => {}
        other => panic!("add_both on item: {other:?}"),
    }
    match ca::add_both().id(3).qty(5).other_qty(0).exec(&client).await {
        Err(ca::AddBothError::OtherConstraint(errors::OtherConstraint::Positive, _)) => {}
        other => panic!("add_both on other: {other:?}"),
    }

    // Exclusion constraints get a typed variant.
    ca::book().id(1).start(1).end(5).exec(&client).await.expect("first booking");
    match ca::book().id(2).start(3).end(8).exec(&client).await {
        Err(ca::BookError::BookingConstraint(errors::BookingConstraint::NoOverlap, db)) => {
            assert_eq!(db.code(), &tokio_postgres::error::SqlState::EXCLUSION_VIOLATION);
        }
        other => panic!("exclusion: {other:?}"),
    }

    // A domain CHECK is reported against the domain, and mapped to the
    // table whose column has that domain.
    match ca::add_score().id(1).pct(150).exec(&client).await {
        Err(ca::AddScoreError::ScoreConstraint(errors::ScoreConstraint::PercentRange, db)) => {
            assert_eq!(db.datatype(), Some("percent"));
        }
        other => panic!("domain check: {other:?}"),
    }

    // The query generator shares the mapping.
    match queries::insert_b_item(&client, 7, 1).await {
        Err(queries::InsertBItemError::CbItemConstraint(errors::CbItemConstraint::Positive, _)) => {}
        other => panic!("query check: {other:?}"),
    }
    match queries::insert_booking(&client, 9, 4, 6).await {
        Err(queries::InsertBookingError::BookingConstraint(errors::BookingConstraint::NoOverlap, _)) => {}
        other => panic!("query exclusion: {other:?}"),
    }
"#,
        );
    });
}
