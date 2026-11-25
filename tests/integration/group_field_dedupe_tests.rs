//! Names that fold to one Rust identifier are made unique inside `prefix__`
//! groups as they are everywhere else: for views, function OUT / TABLE
//! columns and query rows alike. A group that shares its name with a plain
//! column gets a suffix too.

use super::*;
use crate::integration::run_helpers::{generate_project, queries_config, run_binary};

#[test]
fn clashing_names_inside_groups_get_distinct_fields() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema gd;
            create table gd.t (id int primary key, a int not null, b int not null);
            insert into gd.t values (1, 10, 20);

            create view gd.v as
                select t.id,
                       t.id as owner,
                       t.a  as "owner__val",
                       t.b  as "owner__Val",
                       o.a  as "opt__val",
                       o.b  as "opt__Val"
                from gd.t t left join gd.t o on o.id = t.id;

            create function gd.rows() returns setof gd.v
                language sql as $$ select * from gd.v $$;
            create function gd.packed() returns gd.v
                language sql as $$ select v from gd.v v limit 1 $$;
            create function gd.echo(p_v gd.v) returns gd.v
                language sql as $$ select p_v $$;

            create function gd.outs(out id int, out owner int, out "owner__val" int, out "owner__Val" int)
                language sql as $$ select t.id, t.id, t.a, t.b from gd.t t $$;

            create function gd.tabled()
                returns table (id int, "g__val" int, "g__Val" int)
                language sql as $$ select t.id, t.a, t.b from gd.t t $$;
            "#,
        )
        .expect("schema");

        let (_dir, queries) = queries_config(
            r#"
-- name: Grouped :one
select t.id, t.id as owner, t.a as "owner__val", t.b as "owner__Val" from gd.t t;
"#,
        );
        let project = generate_project(conn_string, |b| b.schema("gd").queries_config(queries));
        run_binary(
            &project,
            conn_string,
            "group_dedupe",
            "",
            r#"
    let json = |value: serde_json::Value, path: &[&str]| {
        path.iter().fold(value, |v, key| v[*key].clone())
    };

    // Fields are read through their distinct Rust names; views and queries
    // still serialize each under its column name.
    let rows = gd::rows().exec(&client).await.expect("rows");
    let row = &rows[0];
    let pair = serde_json::json!([row.owner2.val, row.owner2.val2]);
    assert_eq!(pair, serde_json::json!([10, 20]));
    let opt = row.opt.as_ref().expect("joined row");
    assert_eq!(serde_json::json!([opt.val, opt.val2]), serde_json::json!([10, 20]));
    let as_json = serde_json::to_value(row).expect("json");
    assert_eq!(json(as_json.clone(), &["owner"]), 1);
    assert_eq!(json(as_json.clone(), &["owner2", "val"]), 10);
    assert_eq!(json(as_json.clone(), &["owner2", "Val"]), 20);

    // The same shape through the composite codec, both ways.
    let packed = gd::packed().exec(&client).await.expect("packed").expect("a row");
    assert_eq!(serde_json::json!([packed.owner2.val, packed.owner2.val2]), serde_json::json!([10, 20]));
    let echoed = gd::echo().v(&packed).exec(&client).await.expect("echo").expect("a row");
    assert_eq!(serde_json::to_value(&echoed).expect("json"), as_json);

    let outs = gd::outs().exec(&client).await.expect("outs");
    assert_eq!(
        serde_json::json!([outs.id, outs.owner, outs.owner2.val, outs.owner2.val2]),
        serde_json::json!([1, 1, 10, 20])
    );

    let tabled = gd::tabled().exec(&client).await.expect("tabled");
    assert_eq!(serde_json::json!([tabled[0].g.val, tabled[0].g.val2]), serde_json::json!([10, 20]));

    let grouped = queries::grouped(&client).await.expect("grouped");
    assert_eq!((grouped.id, grouped.owner, grouped.owner2.val, grouped.owner2.val2), (1, 1, 10, 20));
    let json = serde_json::to_value(&grouped).expect("json");
    assert_eq!(json["owner2"]["val"], 10);
    assert_eq!(json["owner2"]["Val"], 20);
"#,
        );
    });
}
