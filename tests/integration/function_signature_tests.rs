//! Function signatures the introspection used to get wrong: unnamed and
//! VARIADIC arguments, overloads, and SQL-standard (`BEGIN ATOMIC` /
//! `RETURN`) bodies. Also: annotation validation.

use super::*;
use crate::integration::compile_helpers::read_pretty;
use crate::integration::run_helpers::{generate_project, run_binary};

/// A `prefix__` group of a SQL function's result columns that comes from the
/// nullable side of an outer join is `Option<Struct>`, as it is for views
/// and queries.
#[test]
fn grouped_out_params_from_an_outer_join_are_optional() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema gp;
            create table gp.owner (id int primary key, name text not null);
            create table gp.pet (
                id int primary key,
                name text not null,
                owner_id int references gp.owner (id)
            );
            insert into gp.owner values (10, 'ann');
            insert into gp.pet values (1, 'rex', 10), (2, 'stray', null);

            create function gp.pets() returns table (id int, name text, owner__id int, owner__name text)
                language sql as $$
                    select p.id, p.name, o.id, o.name
                    from gp.pet p left join gp.owner o on o.id = p.owner_id
                    order by p.id
                $$;
            "#,
        )
        .expect("schema");

        let project = generate_project(conn_string, |b| b.schema("gp"));
        run_binary(
            &project,
            conn_string,
            "grouped_out",
            "",
            r#"
    let pets = gp::pets().exec(&client).await.expect("pets");
    assert_eq!(pets.len(), 2);
    let owner: &Option<gp::PetsRowOwner> = &pets[0].owner;
    let owner = owner.as_ref().expect("rex has an owner");
    assert_eq!(Option::<i32>::from(owner.id), Some(10));
    assert_eq!(Option::<String>::from(owner.name.clone()).as_deref(), Some("ann"));
    assert!(pets[1].owner.is_none());
"#,
        );
    });
}

/// Annotations that are unknown, removed, malformed or on an object where
/// they mean nothing fail generation, each named with where it is: also on a
/// helper outside the codegen schemas, where a stray tag would otherwise sit
/// unnoticed.
#[test]
fn bad_annotations_are_generation_errors() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema ann;
            create schema ann_helpers;

            create function ann_helpers.helper() returns int language sql as $$ select 1 $$;
            comment on function ann_helpers.helper is 'Internal. @pgrpc_omit';

            create function ann.removed() returns setof int language sql as $$ select 1 $$;
            comment on function ann.removed is '@pgrpc_return_opt';

            create function ann.in_param(p_id int) returns int language sql as $$ select p_id $$;
            comment on function ann.in_param is '@pgrpc_not_null(p_id)';

            create function ann.bare_on_set() returns setof int language sql as $$ select 1 $$;
            comment on function ann.bare_on_set is '@pgrpc_not_null';

            create type ann.pair as (id int, name text);
            comment on type ann.pair is '@pgrpc_not_null(id, nmae)';
            comment on column ann.pair.name is '@pgrpc_not_null(name)';
            create function ann.pair() returns ann.pair language sql as $$ select 1, 'a' $$;

            create domain ann.code as text;
            comment on domain ann.code is '@pgrpc_strict';
            create function ann.code() returns ann.code language sql as $$ select 'a' $$;

            create table ann.t (id int primary key, n int constraint n_positive check (n > 0));
            comment on constraint n_positive on ann.t is '@pgrpc_field(n)';
            "#,
        )
        .expect("schema");

        let out = tempfile::TempDir::new().expect("temp dir");
        let result = std::panic::catch_unwind(|| {
            pgrpc::PgrpcBuilder::new()
                .connection_string(conn_string)
                .schema("ann")
                .output_path(out.path())
                .build()
        });
        let err = result
            .expect("bad annotations must not panic")
            .expect_err("bad annotations must fail generation");
        let message = format!("{err:#}");
        for expected in [
            "function ann_helpers.helper: unknown annotation `@pgrpc_omit`",
            "function ann.removed: `@pgrpc_return_opt` has been removed",
            "function ann.in_param: `@pgrpc_not_null(p_id)`: `p_id` is not an OUT parameter",
            "function ann.bare_on_set: bare `@pgrpc_not_null`",
            "ann.pair: `nmae` is not one of its columns",
            "column ann.pair.name: a list of names",
            "domain ann.code: `@pgrpc_strict` has been removed",
            "constraint n_positive on ann.t: `@pgrpc_field` has no `@pgrpc_message`",
        ] {
            assert!(message.contains(expected), "missing {expected:?} in:\n{message}");
        }
    });
}

/// `@pgrpc_not_null(*)` makes every attribute of a composite type non-null,
/// `@pgrpc_nullable` (in the type's list or on the column) carves out the
/// exceptions, and `builders` in the config derives `bon::Builder`.
#[test]
fn not_null_all_with_nullable_exceptions_and_config_builders() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema nn;
            create type nn.item as (id int, name text, note text, extra text);
            comment on type nn.item is 'An item. @pgrpc_not_null(*) @pgrpc_nullable(note)';
            comment on column nn.item.extra is '@pgrpc_nullable';
            create function nn.item() returns nn.item language sql as $$ select 1, 'a', null, null $$;
            "#,
        )
        .expect("schema");

        let out = tempfile::TempDir::new().expect("temp dir");
        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("nn")
            .builder_type("nn.item")
            .output_path(out.path())
            .build()
            .expect("generation");
        let code = read_pretty(out.path().join("nn.rs"));
        assert!(code.contains("bon::Builder"), "{code}");
        assert!(code.contains("pub id: i32,"), "{code}");
        assert!(code.contains("pub name: String,"), "{code}");
        assert!(code.contains("pub note: Option<String>,"), "{code}");
        assert!(code.contains("pub extra: Option<String>,"), "{code}");
    });
}

/// What a `LANGUAGE sql` function's result is inferred NOT NULL from: the
/// column's own nullability, and, for a function that does not return a set,
/// proof that its last statement always returns a row (otherwise the call
/// yields NULL). A STRICT function that can be handed a NULL proves nothing.
#[test]
fn sql_function_result_nullability_needs_a_certain_row() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema rn;
            create table rn.t (id int primary key, name text not null);

            -- One row whatever the data, and count(*) is never NULL.
            create function rn.sure_scalar() returns int
                language sql as $$ select count(*)::int from rn.t $$;
            -- `name` is NOT NULL, but no row may match.
            create function rn.maybe_scalar(p_id int) returns text
                language sql as $$ select name from rn.t where id = p_id $$;
            -- Skips its body, returning NULL, when the default NULL is used.
            create function rn.strict_scalar(p_n int default null) returns int
                language sql strict as $$ select count(*)::int from rn.t $$;

            create function rn.sure_pair(out o_sure_a int, out o_sure_b int)
                language sql as $$ select count(*)::int, count(id)::int from rn.t $$;
            create function rn.maybe_pair(p_id int, out o_maybe_a int, out o_maybe_b text)
                language sql as $$ select id, name from rn.t where id = p_id $$;

            -- Rows of a set exist or they do not: nothing wraps them in NULL.
            create function rn.listing() returns table (set_a int, set_b text)
                language sql as $$ select id, name from rn.t $$;
            "#,
        )
        .expect("schema");

        let out = tempfile::TempDir::new().expect("temp dir");
        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("rn")
            .output_path(out.path())
            .build()
            .expect("generation");
        let code = read_pretty(out.path().join("rn.rs"));
        let squashed: String = code.split_whitespace().collect();

        assert!(squashed.contains("->Result<i32,SureScalarError>"), "{code}");
        assert!(squashed.contains("->Result<Option<String>,MaybeScalarError>"), "{code}");
        assert!(squashed.contains("->Result<Option<i32>,StrictScalarError>"), "{code}");

        assert!(squashed.contains("pubsure_a:i32,"), "{code}");
        assert!(squashed.contains("pubmaybe_a:Option<i32>,"), "{code}");
        assert!(squashed.contains("pubmaybe_b:Option<String>,"), "{code}");

        assert!(squashed.contains("pubset_a:i32,"), "{code}");
        assert!(squashed.contains("pubset_b:String,"), "{code}");
    });
}

/// Unnamed, partially named and VARIADIC arguments all reach the generated
/// function, and SQL-standard bodies are analyzed like dollar-quoted ones.
#[test]
fn unnamed_variadic_and_sql_standard_functions() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema sig;
            create table sig.item (id int primary key, label text not null unique);

            -- No argument names at all.
            create function sig.add_unnamed(int, int) returns int
                language sql as $$ select $1 + $2 $$;

            -- First argument named, second not; the unnamed one has a default.
            create function sig.partly_named(p_base int, int default 10) returns int
                language sql as $$ select p_base + $2 $$;

            -- Unnamed OUT parameters.
            create function sig.unnamed_out(p_n int, out int, out text)
                language sql as $$ select p_n * 2, p_n::text $$;

            create function sig.total(p_scale int, variadic p_nums int[]) returns int
                language sql as $$ select p_scale * coalesce(sum(n), 0)::int from unnest(p_nums) n $$;

            create function sig.total_or_default(p_scale int, variadic p_nums int[] default array[1, 2]) returns int
                language sql as $$ select p_scale * coalesce(sum(n), 0)::int from unnest(p_nums) n $$;

            -- Overloads: the one with the fewest arguments is kept.
            create function sig.over(p_a int) returns int language sql as $$ select p_a $$;
            create function sig.over(p_a int, p_b int) returns int language sql as $$ select p_a + p_b $$;
            create function sig.over(p_a text) returns text language sql as $$ select p_a $$;

            -- SQL-standard bodies have no dollar quoting.
            create function sig.insert_item(p_id int, p_label text) returns int
                language sql
                begin atomic
                    insert into sig.item (id, label) values (p_id, p_label) returning id;
                end;

            create function sig.plus_one(p_n int) returns int
                language sql
                return p_n + 1;

            -- A comment on something else that shares this function's oid
            -- must not attach to it; there is nothing to assert beyond
            -- generation succeeding with exactly one row per function.
            comment on function sig.plus_one is 'Adds one. @pgrpc_not_null';
            "#,
        )
        .expect("schema");

        let project = generate_project(conn_string, |b| b.schema("sig"));
        let sig_rs = read_pretty(project.path().join("src/generated/sig.rs"));

        // The SQL-standard body was found and analyzed: the insert's
        // constraints are in the function's error enum.
        assert!(
            sig_rs.contains("InsertItemError::ItemConstraint"),
            "BEGIN ATOMIC body should be analyzed:\n{sig_rs}"
        );
        assert_eq!(
            sig_rs.matches("pub async fn over(").count(),
            1,
            "exactly one overload is generated"
        );

        run_binary(
            &project,
            conn_string,
            "signatures",
            "",
            r#"
    assert_eq!(sig::add_unnamed().arg1(2).arg2(3).exec(&client).await.expect("unnamed"), Some(5));

    assert_eq!(sig::partly_named().base(1).arg2(5).exec(&client).await.expect("partly named"), Some(6));

    let out = sig::unnamed_out().n(4).exec(&client).await.expect("unnamed out");
    assert_eq!(Option::<i32>::from(out.column1), Some(8));
    assert_eq!(Option::<String>::from(out.column2).as_deref(), Some("4"));

    let nums = vec![1, 2, 3];
    assert_eq!(sig::total().scale(2).nums(&nums).exec(&client).await.expect("variadic"), Some(12));
    assert_eq!(
        sig::total_or_default().scale(2).nums(&nums).exec(&client).await.expect("variadic given"),
        Some(12)
    );
    assert_eq!(
        sig::total_or_default().scale(2).exec(&client).await.expect("variadic defaulted"),
        Some(6)
    );

    // Fewest arguments, then argument types in text order: over(integer).
    let kept: Option<i32> = sig::over().a(7).exec(&client).await.expect("overload");
    assert_eq!(kept, Some(7));

    assert_eq!(sig::insert_item().id(1).label("a").exec(&client).await.expect("insert"), Some(1));
    match sig::insert_item().id(2).label("a").exec(&client).await {
        Err(sig::InsertItemError::ItemConstraint(errors::ItemConstraint::ItemLabelKey, _)) => {}
        other => panic!("expected the unique violation as a typed variant, got {other:?}"),
    }

    let next: i32 = sig::plus_one().n(41).exec(&client).await.expect("return body");
    assert_eq!(next, 42);
"#,
        );
    });
}
