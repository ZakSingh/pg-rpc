//! SQL names that are not Rust identifiers as they stand: a schema with a
//! dash, labels and columns that are keywords, symbols or contain spaces,
//! names that fold to the same Rust name, and names starting with a digit.

use super::*;
use crate::integration::compile_helpers::read_pretty;
use crate::integration::run_helpers::{generate_project, run_binary};

#[test]
fn awkward_sql_names_generate_and_run() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema "my-schema";
            create schema idn;

            create type "my-schema".grade as enum ('A+', 'A-', '+', '-', 'self', 'B');
            create type "my-schema".weird as (
                "self" int, "my col" text, "type" text, "foo-bar" int, "foo_bar" int, "1st" int
            );
            create table "my-schema".thing (
                id int primary key,
                "self" int,
                qty int not null,
                constraint "1_positive" check (qty > 0)
            );
            insert into "my-schema".thing values (1, null, 5), (2, 1, 7);

            create view "my-schema".thing_view as
                select t.id, t."self", o.id as "other__my id", o.qty as "other__self"
                from "my-schema".thing t
                         left join "my-schema".thing o on o.id = t."self";

            -- `p_id` and `id` would both be `id`; `p_` has nothing after its
            -- prefix; `Self` cannot be a raw identifier.
            create function "my-schema".make(p_id int, id int, p_ int, "Self" int)
                returns "my-schema".weird
                language sql as $$ select row(p_id, 'x', 't', id, p_, "Self")::"my-schema".weird $$;

            create function "my-schema".things() returns setof "my-schema".thing_view
                language sql as $$ select * from "my-schema".thing_view order by id $$;

            create function "my-schema".add_thing(p_id int, p_qty int) returns void
                language sql as $$ insert into "my-schema".thing (id, qty) values (p_id, p_qty) $$;

            -- OUT columns that collide once their prefixes are dropped.
            create function "my-schema".outs(out o_id int, out id int, out "type" text)
                language plpgsql as $$ begin o_id := 1; id := 2; "type" := 't'; end $$;

            -- Another schema's module has to import this one by its Rust name.
            create function idn.pick(p_grade "my-schema".grade) returns "my-schema".grade
                language sql as $$ select p_grade $$;
            "#,
        )
        .expect("schema");

        let project = generate_project(conn_string, |b| b.schema("my-schema").schema("idn"));
        let idn_rs = read_pretty(project.path().join("src/generated/idn.rs"));
        assert!(idn_rs.contains("use super::my_schema;"), "{idn_rs}");

        run_binary(
            &project,
            conn_string,
            "idents",
            "use test_project::my_schema::{Grade, Weird};",
            r#"
    // Labels that fold together, or to nothing, are all distinct variants.
    let grades = [
        (Grade::A, "A+"),
        (Grade::A2, "A-"),
        (Grade::X2b, "+"),
        (Grade::X2d, "-"),
        (Grade::Self_, "self"),
        (Grade::B, "B"),
    ];
    for (grade, label) in grades {
        assert_eq!(serde_json::to_value(grade).unwrap(), label);
        let echoed = idn::pick().grade(grade).exec(&client).await.expect("enum round trip");
        assert_eq!(echoed, Some(grade), "{label}");
    }

    // Arguments keep distinct names; the composite's fields do too.
    let made: Weird = my_schema::make().p_id(1).id(2).p(3).self_(4).exec(&client).await
        .expect("make");
    assert_eq!(made.self_, Some(1));
    assert_eq!(made.my_col.as_deref(), Some("x"));
    assert_eq!(made.r#type.as_deref(), Some("t"));
    assert_eq!(made.foo_bar, Some(2));
    assert_eq!(made.foo_bar2, Some(3));
    assert_eq!(made._1st, Some(4));
    let json = serde_json::to_value(&made).unwrap();
    assert_eq!(json["self"], 1);
    assert_eq!(json["my col"], "x");
    assert_eq!(json["foo-bar"], 2);
    assert_eq!(json["foo_bar"], 3);
    assert_eq!(json["1st"], 4);

    // A view whose grouped columns have quoted names.
    let things = my_schema::things().exec(&client).await.expect("view rows");
    assert_eq!(things.len(), 2);
    assert!(things[0].other.is_none());
    assert_eq!(things[1].self_, Some(1));
    let other = things[1].other.as_ref().expect("thing 2 points at thing 1");
    assert_eq!((other.my_id, other.self_), (Some(1), Some(5)));

    let outs = my_schema::outs().exec(&client).await.expect("out params");
    assert_eq!((outs.o_id, outs.id, outs.r#type.as_deref()), (Some(1), Some(2), Some("t")));

    // A constraint whose name starts with a digit.
    match my_schema::add_thing().id(3).qty(0).exec(&client).await {
        Err(my_schema::AddThingError::ThingConstraint(errors::ThingConstraint::_1Positive, _)) => {}
        other => panic!("expected the CHECK as a typed variant, got {other:?}"),
    }
"#,
        );
    });
}
