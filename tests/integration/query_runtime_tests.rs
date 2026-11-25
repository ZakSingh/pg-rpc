//! Runtime behaviour of generated query functions: a `:one` query that
//! matches nothing, and DEFAULT substitution next to text that merely looks
//! like a parameter.

use super::*;
use crate::integration::run_helpers::{generate_project, queries_config, run_binary};
use indoc::indoc;

#[test]
fn one_query_without_rows_and_default_rewriting() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema q;
            create table q.thing (
                id int primary key,
                label text not null default 'none',
                note text not null
            );
            insert into q.thing values (1, 'one', 'first');
            "#,
        )
        .expect("schema");

        let (_queries_dir, queries) = queries_config(indoc! {"
            -- name: thing_label :one
            select label from q.thing where id = :id;

            -- name: thing_row :one
            select id, label from q.thing where id = :id;

            -- name: thing_row_opt :opt
            select id, label from q.thing where id = :id;

            -- name: thing_labels :many
            select label from q.thing order by id;

            -- name: add_thing :one
            insert into q.thing (id, label, note) -- $2 is the label
            values (:id, :label, 'costs $2') /* not $1 */
            returning label;
        "});

        let project = generate_project(conn_string, |b| b.schema("q").queries_config(queries));
        run_binary(
            &project,
            conn_string,
            "query_runtime",
            "use test_project::errors::PgRpcError;",
            r#"
    // Rows that exist decode as before.
    assert_eq!(queries::thing_label(&client, 1).await.expect("label"), "one");
    let row = queries::thing_row(&client, 1).await.expect("row");
    assert_eq!((row.id, row.label.as_str()), (1, "one"));
    assert!(queries::thing_row_opt(&client, 9).await.expect("opt").is_none());
    assert_eq!(queries::thing_labels(&client).await.expect("many"), vec!["one".to_string()]);

    // A `:one` query with no row is an error the caller can match on.
    match queries::thing_label(&client, 9).await {
        Err(queries::ThingLabelError::Other(PgRpcError::NoRows { item })) => {
            assert_eq!(item, "thing_label");
        }
        other => panic!("expected NoRows for a scalar :one query, got {other:?}"),
    }
    match queries::thing_row(&client, 9).await {
        Err(queries::ThingRowError::Other(PgRpcError::NoRows { item })) => {
            assert_eq!(item, "thing_row");
        }
        other => panic!("expected NoRows for a struct :one query, got {other:?}"),
    }
    let err = queries::thing_row(&client, 9).await.unwrap_err();
    assert!(err.as_db_error().is_none());
    assert_eq!(PgRpcError::from(err).to_string(), "thing_row: query returned no rows");

    // Omitting the label takes the column default; the `$2` inside the
    // string literal and the ones in comments are left alone.
    let label = queries::add_thing(&client, 2, None::<&str>).await.expect("insert with default");
    assert_eq!(label, "none");
    let label = queries::add_thing(&client, 3, "three").await.expect("insert with value");
    assert_eq!(label, "three");
    let notes: Vec<String> = client
        .query("select note from q.thing where id in (2, 3) order by id", &[])
        .await
        .expect("notes")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(notes, vec!["costs $2".to_string(), "costs $2".to_string()]);
"#,
        );
    });
}
