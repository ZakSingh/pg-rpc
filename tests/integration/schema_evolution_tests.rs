//! A client generated against schema N must keep working against schema N+1
//! when N+1 only adds things: an enum label, a composite attribute, a view
//! column, a result column, a defaulted parameter.
//!
//! The binary below is generated and compiled against schema N. It then
//! applies the additive migration itself, so it can call through two kinds of
//! connection:
//!
//! - a **warm** one, opened and used before the migration. This is what a web
//!   process holds during a deploy. tokio-postgres caches type definitions and
//!   deadpool caches prepared statements per connection, so it carries the
//!   old picture of every type.
//! - a **fresh** one, opened after the migration, which sees the new catalog.
//!
//! One case fails until the application acts: writing a composite through a
//! warm connection. The client encodes the attribute list it cached, and the
//! server rejects a value with too few attributes. `schema_cache::invalidate()`
//! with the `schema_cache::hook()` installed on the pool makes the connection
//! drop that cache at its next checkout.

use super::*;
use crate::integration::compile_helpers::{
    add_test_binary, compile_and_run_with_args, create_test_cargo_project_with, print_output,
    read_pretty,
};
use indoc::indoc;

const SCHEMA_N: &str = indoc! {"
    create type evo_mood as enum ('happy', 'sad');
    create type evo_contact as (phone text, email text);
    create domain evo_checked_contact as evo_contact check ((value).phone is not null);
    create type evo_envelope as (label text, contact evo_checked_contact);

    create table evo_person (
        id int primary key,
        name text not null,
        mood evo_mood not null,
        contact evo_contact
    );
    insert into evo_person values
        (1, 'ann', 'happy', row('111', 'ann@example.com')),
        (2, 'bob', 'sad', row('222', 'bob@example.com'));

    create view evo_person_view as
        select id, name, mood, contact from evo_person;

    create function api.evo_get_mood(p_id int) returns evo_mood
        language sql as $$ select mood from evo_person where id = p_id $$;

    create function api.evo_set_mood(p_id int, p_mood evo_mood) returns int
        language sql as $$ update evo_person set mood = p_mood where id = p_id returning id $$;

    create function api.evo_get_contact(p_id int) returns evo_contact
        language sql as $$ select contact from evo_person where id = p_id $$;

    create function api.evo_get_checked(p_id int) returns evo_checked_contact
        language sql as $$ select contact::evo_checked_contact from evo_person where id = p_id $$;

    create function api.evo_get_envelope(p_id int) returns evo_envelope
        language sql as $$
            select row(name, contact::evo_checked_contact)::evo_envelope
            from evo_person where id = p_id
        $$;

    create function api.evo_set_contact(p_id int, p_contact evo_contact) returns int
        language sql as $$ update evo_person set contact = p_contact where id = p_id returning id $$;

    create function api.evo_set_checked(p_id int, p_contact evo_checked_contact) returns int
        language sql as $$ update evo_person set contact = p_contact where id = p_id returning id $$;

    create function api.evo_count_contacts(p_contacts evo_contact[]) returns int
        language sql as $$ select cardinality(p_contacts) $$;

    create function api.evo_people(p_id int) returns setof evo_person_view
        language sql as $$ select * from evo_person_view where id = p_id $$;

    create function api.evo_people_table() returns table (id int, name text)
        language sql as $$ select id, name from evo_person order by id $$;

    create function api.evo_greet(p_name text) returns text
        language sql as $$ select 'hello ' || p_name $$;
"};

/// Query files with a star that is not last. After the migration the view
/// behind `v.*` has one more column, which lands in the middle of the row.
const QUERIES: &str = indoc! {"
    -- name: evo_star_many :many
    select v.*, p.name as person_name, 7 as seven
    from evo_person_view v join evo_person p using (id)
    where v.id = :id;

    -- name: evo_star_opt :opt
    select v.*, p.name as person_name, 7 as seven
    from evo_person_view v join evo_person p using (id)
    where v.id = :id;
"};

/// The additive migration, one statement per entry: a new enum label cannot
/// be used in the transaction that adds it.
const MIGRATION: &[&str] = &[
    "alter type evo_mood add value 'excited'",
    "alter type evo_contact add attribute fax text",
    "create or replace view evo_person_view as \
        select id, name, mood, contact, upper(name) as shout from evo_person",
    "drop function api.evo_people_table()",
    "create function api.evo_people_table() returns table (id int, name text, shout text) \
        language sql as $$ select id, name, upper(name) from evo_person order by id $$",
    "drop function api.evo_greet(text)",
    "create function api.evo_greet(p_name text, p_punct text default '!') returns text \
        language sql as $$ select 'hello ' || p_name || p_punct $$",
    "update evo_person set mood = 'excited' where id = 2",
    "update evo_person set contact = row('111', 'ann@example.com', 'ann-fax') where id = 1",
];

#[test]
fn old_client_survives_additive_schema_changes() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(client, SCHEMA_N).expect("Should create schema N");

        let queries_dir = tempfile::TempDir::new().expect("temp dir");
        let queries_path = queries_dir.path().join("queries.sql");
        std::fs::write(&queries_path, QUERIES).expect("write queries");

        let project = create_test_cargo_project_with(conn_string, |b| {
            b.schema("public").schema("api").queries_config(pgrpc::QueriesConfig {
                paths: vec![queries_path.to_string_lossy().to_string()],
            })
        });

        let migration = MIGRATION
            .iter()
            .map(|stmt| format!("    {:?},\n", stmt))
            .collect::<String>();

        let test_code = indoc! {r#"
            use deadpool_postgres::{Config, Pool, Runtime};
            use test_project::api::*;
            use test_project::generated::{queries, schema_cache};
            use test_project::public::{EvoCheckedContact, EvoContact, EvoMood};

            const MIGRATION: &[&str] = &[
            __MIGRATION__];

            /// A pool with the schema-cache hook installed, as an application
            /// would build it. The hook does nothing until `invalidate()`.
            fn pool(conn_string: &str) -> Pool {
                let mut config = Config::new();
                config.url = Some(conn_string.to_string());
                config
                    .builder(tokio_postgres::NoTls)
                    .expect("Failed to configure pool")
                    .runtime(Runtime::Tokio1)
                    .pre_recycle(schema_cache::hook())
                    .build()
                    .expect("Failed to create pool")
            }

            async fn backend_pid(client: &deadpool_postgres::Client) -> i32 {
                client.query_one("select pg_backend_pid()", &[]).await.expect("pid").get(0)
            }

            fn contact(phone: &str) -> EvoContact {
                EvoContact { phone: Some(phone.into()), email: Some("x@example.com".into()) }
            }

            /// Every call that must work the same before and after the
            /// migration, on any connection.
            async fn reads(client: &deadpool_postgres::Client, when: &str) {
                let mood = evo_get_mood().id(1).exec(client).await
                    .unwrap_or_else(|e| panic!("[{when}] read enum: {e:?}"));
                assert_eq!(mood, Some(EvoMood::Happy), "[{when}] read enum");

                let set = evo_set_mood().id(1).mood(EvoMood::Happy).exec(client).await
                    .unwrap_or_else(|e| panic!("[{when}] write enum: {e:?}"));
                assert_eq!(set, Some(1), "[{when}] write enum");

                let c = evo_get_contact().id(1).exec(client).await
                    .unwrap_or_else(|e| panic!("[{when}] read composite: {e:?}"))
                    .expect("contact");
                assert_eq!(c.phone.as_deref(), Some("111"), "[{when}] read composite");
                assert_eq!(c.email.as_deref(), Some("ann@example.com"), "[{when}] read composite");

                let c = evo_get_checked().id(1).exec(client).await
                    .unwrap_or_else(|e| panic!("[{when}] read domain over composite: {e:?}"))
                    .expect("checked contact");
                assert_eq!(c.phone, "111", "[{when}] read domain over composite");

                let e = evo_get_envelope().id(1).exec(client).await
                    .unwrap_or_else(|e| panic!("[{when}] read nested domain: {e:?}"))
                    .expect("envelope");
                assert_eq!(e.label.as_deref(), Some("ann"), "[{when}] read nested domain");
                assert_eq!(
                    e.contact.map(|c| c.phone).as_deref(),
                    Some("111"),
                    "[{when}] read nested domain"
                );

                let rows = evo_people().id(1).exec(client).await
                    .unwrap_or_else(|e| panic!("[{when}] read view rows: {e:?}"));
                assert_eq!(rows.len(), 1, "[{when}] read view rows");
                assert_eq!(rows[0].name, "ann", "[{when}] read view rows");
                assert_eq!(
                    rows[0].contact.as_ref().and_then(|c| c.phone.as_deref()),
                    Some("111"),
                    "[{when}] composite nested in a view row"
                );

                let table = evo_people_table().exec(client).await
                    .unwrap_or_else(|e| panic!("[{when}] returns table: {e:?}"));
                assert_eq!(table.len(), 2, "[{when}] returns table");
                assert_eq!(table[1].name, "bob", "[{when}] returns table");

                let hello = evo_greet().name("ann").exec(client).await
                    .unwrap_or_else(|e| panic!("[{when}] defaulted parameter: {e:?}"));
                assert!(
                    hello.as_deref().is_some_and(|h| h.starts_with("hello ann")),
                    "[{when}] defaulted parameter: {hello:?}"
                );
            }

            /// Writes of a composite: as a value, through a domain, and as an
            /// array element.
            async fn composite_writes(
                client: &deadpool_postgres::Client,
            ) -> Result<(), test_project::PgRpcError> {
                evo_set_contact().id(2).contact(&contact("999")).exec(client).await?;
                let checked = EvoCheckedContact { phone: "999".into(), email: None };
                evo_set_checked().id(2).contact(&checked).exec(client).await?;
                let payload = vec![contact("1"), contact("2")];
                let n = evo_count_contacts().contacts(&payload).exec(client).await?;
                assert_eq!(n, Some(2));
                Ok(())
            }

            /// Query files whose star is followed by more columns. Row
            /// structs find their columns by name, so the column the view
            /// gains in the middle of the row must not shift the rest.
            async fn star_queries(client: &deadpool_postgres::Client) -> Result<(), String> {
                let rows = queries::evo_star_many(client, 1).await.map_err(|e| format!("{e:?}"))?;
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].name, "ann");
                assert_eq!(rows[0].person_name, "ann");
                assert_eq!(rows[0].seven, 7);

                let row = queries::evo_star_opt(client, 1).await.map_err(|e| format!("{e:?}"))?
                    .expect("row");
                assert_eq!(row.person_name, "ann");
                assert_eq!(row.seven, 7);
                Ok(())
            }

            #[tokio::main]
            async fn main() {
                let conn_string = std::env::args().nth(1).expect("Connection string required");

                // Warm connection: fill its type and statement caches against schema N.
                let warm_pool = pool(&conn_string);
                let warm = warm_pool.get().await.expect("warm client");
                reads(&warm, "warm, before").await;
                composite_writes(&warm).await.expect("composite writes before the migration");
                star_queries(&warm).await.expect("star queries before the migration");
                let mood = evo_get_mood().id(2).exec(&warm).await.expect("read enum");
                assert_eq!(mood, Some(EvoMood::Sad));

                // Migrate through a separate connection.
                {
                    let admin_pool = pool(&conn_string);
                    let admin = admin_pool.get().await.expect("admin client");
                    for stmt in MIGRATION {
                        admin.batch_execute(stmt).await
                            .unwrap_or_else(|e| panic!("migration step failed: {stmt}: {e:?}"));
                    }
                }

                // The same warm connection, now against schema N+1.
                reads(&warm, "warm, after").await;
                println!("OK: warm reads");

                // A fresh connection against schema N+1.
                let fresh_pool = pool(&conn_string);
                let fresh = fresh_pool.get().await.expect("fresh client");
                reads(&fresh, "fresh, after").await;
                println!("OK: fresh reads");

                composite_writes(&fresh).await.expect("composite writes on a fresh connection");
                let fax_is_null: bool = fresh
                    .query_one("select (contact).fax is null from evo_person where id = 2", &[])
                    .await
                    .expect("check fax")
                    .get(0);
                assert!(fax_is_null, "an attribute the client does not know is written as NULL");
                println!("OK: fresh composite writes");

                star_queries(&fresh).await.expect("star queries on a fresh connection");
                println!("OK: fresh star queries");

                // A warm connection still holds the statement prepared with
                // the old expansion of the star.
                let err = star_queries(&warm).await
                    .expect_err("a cached statement with a star cannot survive the view change");
                println!("warm star query: {err}");

                // A label the client has never heard of fails at the value,
                // with an error rather than a panic, on either connection.
                for (when, client) in [("warm", &warm), ("fresh", &fresh)] {
                    let err = evo_get_mood().id(2).exec(client).await
                        .expect_err("an unknown enum label cannot be decoded");
                    println!("unknown label on {when} connection: {err:?}");
                    // The connection is still usable afterwards.
                    reads(client, "after unknown label").await;
                }
                println!("OK: unknown label");

                // Until told otherwise, a warm connection encodes a composite
                // with the attribute list it cached, and the server rejects it.
                let err = composite_writes(&warm).await
                    .expect_err("composite write on a warm connection is expected to fail");
                println!("warm composite write: {err:?}");
                reads(&warm, "after failed write").await;
                println!("OK: warm composite write fails cleanly");

                // Once the application reports the schema change, the same
                // connection drops its caches at its next checkout and the
                // write goes through.
                let pid = backend_pid(&warm).await;
                schema_cache::invalidate();
                drop(warm);
                let warm = warm_pool.get().await.expect("warm client again");
                assert_eq!(backend_pid(&warm).await, pid, "the pool hands back the same connection");
                composite_writes(&warm).await
                    .expect("composite writes on a warm connection after invalidate()");
                reads(&warm, "warm, after invalidate").await;
                println!("OK: warm composite write after invalidate");
                star_queries(&warm).await.expect("star queries on a warm connection after invalidate()");
                println!("OK: warm star queries after invalidate");

                println!("OK: schema evolution");
            }
        "#}
        .replace("__MIGRATION__", &migration);

        add_test_binary(project.path(), "schema_evolution", &test_code);
        let output = compile_and_run_with_args(project.path(), "schema_evolution", &[conn_string]);

        let stdout = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() {
            print_output(&output);
            panic!("schema evolution test failed (see compile/run output above)");
        }
        println!("{}", stdout);
        assert!(
            stdout.contains("OK: schema evolution"),
            "test binary did not reach success marker. stdout:\n{}",
            stdout
        );

        // How the generated code gets there.
        let public_rs = read_pretty(project.path().join("src/generated/public.rs"));
        assert!(
            public_rs.contains("allow_mismatch"),
            "generated enums should carry #[postgres(allow_mismatch)]"
        );
        let api_rs = read_pretty(project.path().join("src/generated/api.rs"));
        assert!(
            !api_rs.contains("select * from"),
            "row-returning calls should name their columns, not select *"
        );
        assert!(
            api_rs.contains(r#"select \"id\", \"name\", \"mood\", \"contact\" from api.evo_people"#),
            "evo_people should select the columns known at generation time. Got:\n{}",
            api_rs
        );
    });
}

/// Row structs find their columns by name, so a query whose result has two
/// columns with one name is rejected at generation time, with every such
/// query listed.
#[test]
fn duplicate_result_column_names_are_rejected() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            "create table dup_a (id int primary key, label text);
             create table dup_b (id int primary key, label text);",
        )
        .expect("Should create tables");

        let dir = tempfile::TempDir::new().expect("temp dir");
        let queries_path = dir.path().join("queries.sql");
        std::fs::write(
            &queries_path,
            indoc! {"
                -- name: dup_star :many
                select a.*, b.label from dup_a a join dup_b b using (id);

                -- name: dup_named :many
                select a.id, b.id from dup_a a join dup_b b using (id);

                -- name: dup_fine :many
                select a.*, b.label as b_label from dup_a a join dup_b b using (id);
            "},
        )
        .expect("write queries");

        let err = pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .output_path(dir.path().join("out"))
            .schema("public")
            .queries_config(pgrpc::QueriesConfig {
                paths: vec![queries_path.to_string_lossy().to_string()],
            })
            .build()
            .expect_err("duplicate column names should fail generation");

        let message = format!("{err:#}");
        assert!(message.contains("distinct names"), "{message}");
        assert!(message.contains("dup_star") && message.contains("label"), "{message}");
        assert!(message.contains("dup_named") && message.contains("id"), "{message}");
        assert!(!message.contains("dup_fine"), "{message}");
    });
}
