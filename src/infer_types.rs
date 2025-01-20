use std::collections::HashMap;
use std::str::FromStr;
use tokio_postgres::Client;

struct PgField {
    name: String,
    type_oid: OID,
    nullable: bool,
}

struct PgArg {
    name: String,
    type_oid: OID,
}

/// Postgres object ID
/// Uniquely identifies database objects
type OID = u32;

enum PgType {
    Array(OID),
    Composite { name: String, fields: Vec<PgField> },
    Enum { name: String, variants: Vec<String> },
    Int32,
    Bool,
    Text,
}

struct TypeRegistry {
    types: HashMap<OID, PgType>,
}

impl TypeRegistry {
    fn new() -> Self {
        Self {
            types: HashMap::new(),
        }
    }

    async fn fetch_functions(self, db: &Client) -> anyhow::Result<()> {
        let fn_rows = db
            .query(
                r#"
select distinct on (n.nspname, p.proname)
    n.nspname as schema_name,
    p.proname as function_name,
    pg_get_functiondef(p.oid) as function_definition,
    p.proargtypes as arg_oids,
   (
       select array_agg(t.typname order by ordinality)
       from unnest(p.proargtypes) with ordinality as args(oid, ordinality)
       join pg_type t on t.oid = args.oid
   ) as arg_types,
          (
           select array_agg(t.typcategory order by ordinality)
           from unnest(p.proargtypes) with ordinality as args(oid, ordinality)
           join pg_type t on t.oid = args.oid
       ) as arg_categories,
   case when p.proargnames is not null
        then (select p.proargnames[:array_length(p.proargtypes, 1)])
        end as arg_names
from pg_proc p
    join pg_namespace n on p.pronamespace = n.oid
    left join pg_type t on t.oid = any(p.proargtypes)
where p.prokind = 'f'
    and n.nspname not in ('pg_catalog', 'information_schema')
    and not exists (
        select 1
        from pg_depend d
        where d.objid = p.oid
        and d.deptype = 'e'  -- 'e' means it's from an extension
    )
order by n.nspname, p.proname;
        "#,
                &[],
            )
            .await?;

        for row in fn_rows {
            let arg_type_oids: Vec<OID> = row.get("arg_oids");
            let arg_names: Vec<String> = row.get("arg_names");
            let arg_types: Vec<String> = row.get("arg_types");

            let arguments: Vec<(OID, String, String)> = arg_type_oids
                .iter()
                .zip(arg_names.iter())
                .zip(arg_types.iter())
                .map(|((oid, name), type_name)| (oid.clone(), name.clone(), type_name.clone()))
                .collect();

            // get a vec of ALL OIDS needed by ALL FUNCTIONS
            // then do a single query to fetch ALL of them
            // repeat this process until every OID is in the cache

            let types = db
                .query(
                    r#"
with recursive type_tree as (
    -- base case: start with input oids
    select
        oid,
        typname,
        typtype
    from pg_type
    where oid = any($1)

    union all

    -- recursive case
    select
        t.oid,
        t.typname,
        t.typtype
    from pg_type t
    join type_tree tt on (
        -- array elements
        t.oid = (select typelem from pg_type where oid = tt.oid) or
        -- domain base types
        t.oid = (select typbasetype from pg_type where oid = tt.oid) or
        -- composite type fields
        (tt.typtype = 'c' and t.oid in (
            select atttypid
            from pg_attribute
            where attrelid = (select typrelid from pg_type where oid = tt.oid)
            and attnum > 0
        ))
    )
)
select distinct
    oid,
    typname,
    typtype
from type_tree;
              "#,
                    &[&arg_type_oids],
                )
                .await?;

            for t in types {
                let name: String = t.get("typname");
                dbg!(name);
            }

            // Insert each argument to the type registry
            //
            // for arg_oid in arg_type_oids {
            //     // self.types.insert(arg_oid)
            // }

            // There's basically a 'queue' of types to resolve... What happens if two are dependent on eachother?
            // It doesn't matter. They can always be created; they only contain oids.
            // Each Fn arg just keeps the TypeId
        }

        // OID -> whether it needs to be looked up.
        // Basically, plan is to generate structs/enums for every type object id encountered recursively
        // through function arguments/return type.

        // Then it's a matter of linking the argument OIDs to the respective structs...
        // Problem is that structs need names, not OIDs.

        // What do I name a type `int[]`? In Pg, it's named _int. I guess that's fine.

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use testcontainers_modules::postgres;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
    use tokio_postgres::{Client, Connection, NoTls, Socket};

    #[tokio::test]
    async fn test_infer_types() -> anyhow::Result<()> {
        let db = setup_test_db().await;

        db.client
            .simple_query(
                r#"
                create type a as (
                    int_field int,
                    text_field text
                );

                create type b as (
                    a_field a
                );

                create type test_enum as enum ('enum_variant_1', 'enum_variant_2');

                create function test_fn(arg_a a, num int, ray a[]) returns b as $$
                    begin
                    return row(arg_a)::b;
                    end;
                $$ language plpgsql;
                "#,
            )
            .await?;

        let r = TypeRegistry::new();
        r.fetch_functions(&db.client).await?;
        //
        // let a_oid: OID = db
        //     .client
        //     .query_one(r#"SELECT oid FROM pg_type WHERE typname = 'a';"#, &[])
        //     .await?
        //     .get(0);
        //
        // dbg!(a_oid);

        Ok(())
    }

    pub struct TestDb {
        pub client: Client,
        // Store these to keep them in scope
        #[allow(dead_code)]
        container: ContainerAsync<Postgres>,
        #[allow(dead_code)]
        connection_handle: tokio::task::JoinHandle<Result<(), tokio_postgres::error::Error>>,
    }

    async fn setup_test_db() -> TestDb {
        let container = Postgres::default()
            .with_tag("16-alpine")
            .start()
            .await
            .expect("container to start");

        let connection_string = &format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            container.get_host().await.expect("host to be present"),
            container
                .get_host_port_ipv4(5432)
                .await
                .expect("port to be present")
        );

        let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
            .expect("connection to be established");

        let connection_handle = tokio::spawn(connection);

        TestDb {
            client,
            container,
            connection_handle,
        }
    }
}
