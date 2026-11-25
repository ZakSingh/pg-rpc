//! `TsVector` / `TsQuery` and the date/timestamp serde codecs are generated
//! once, in the shared `tsvector` and `datetime_serde` modules, and every
//! schema module, `queries` and `tasks` refer to that one definition.

use super::*;
use crate::integration::run_helpers::{generate_project, queries_config, run_binary};

#[test]
fn tsvector_is_one_type_across_schemas_and_queries() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema ta;
            create schema tb;
            create table tb.doc (id int primary key, body tsvector, day date not null, at timestamp);
            insert into tb.doc values (1, 'fat:1 cat:2', '2024-05-06', '2024-05-06 07:08:09');

            create function ta.make(p_text text) returns tsvector
                language sql as $$ select p_text::tsvector $$;
            create function tb.shown(p_vector tsvector) returns text
                language sql as $$ select p_vector::text $$;
            create function tb.docs() returns setof tb.doc
                language sql as $$ select * from tb.doc $$;
            "#,
        )
        .expect("schema");

        let (_dir, queries) = queries_config(
            "-- name: Docs :many\nselect id, body, day, at from tb.doc order by id;\n",
        );
        let project =
            generate_project(conn_string, |b| b.schema("ta").schema("tb").queries_config(queries));
        run_binary(
            &project,
            conn_string,
            "shared_tsvector",
            "",
            r#"
    // A value read through one schema is accepted by another.
    let made = ta::make().text("fat:1 cat:2").exec(&client).await.expect("make").expect("vector");
    let shown = tb::shown().vector(&made).exec(&client).await.expect("shown").expect("text");
    assert_eq!(shown, "'cat':2 'fat':1");

    // The paths that existed when each schema had its own copy still resolve,
    // and all name the shared type.
    let _: &test_project::generated::tsvector::TsVector = &made;
    let _: &ta::tsvector::TsVector = &made;
    let _: &tb::tsvector::TsVector = &made;

    // Query rows and composite rows carry it too, and serialize their dates
    // through the shared codecs.
    let rows = queries::docs(&client).await.expect("docs");
    assert_eq!(rows[0].body.as_ref(), Some(&made));
    let json = serde_json::to_value(&rows[0]).expect("json");
    assert_eq!(json["day"], "2024-05-06");
    assert_eq!(json["at"], "2024-05-06T07:08:09.0");

    let docs = tb::docs().exec(&client).await.expect("docs");
    assert_eq!(docs[0].body.as_ref(), Some(&made));
    let json = serde_json::to_value(&docs[0]).expect("json");
    assert_eq!(json["day"], "2024-05-06");
    let back: tb::Doc = serde_json::from_value(json).expect("from json");
    assert_eq!(back.day, docs[0].day);
    assert_eq!(back.at, docs[0].at);
"#,
        );
    });
}

#[test]
fn schema_named_like_a_shared_module_is_rejected() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(client, "create schema tsvector; create schema datetime_serde;")
            .expect("schema");

        for schema in ["tsvector", "datetime_serde"] {
            let out = tempfile::TempDir::new().expect("temp dir");
            let error = pgrpc::PgrpcBuilder::new()
                .connection_string(conn_string)
                .schema(schema)
                .output_path(out.path())
                .build()
                .expect_err("a reserved module name");
            assert!(format!("{error:#}").contains("is reserved"), "{error:#}");
        }
    });
}
