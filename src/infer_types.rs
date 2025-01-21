use heck::{ToPascalCase, ToSnakeCase};
use quote::__private::TokenStream;
use quote::{quote, ToTokens};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use tokio_postgres::Client;

const FUNCTION_INTROSPECTION_QUERY: &'static str = r#"
select distinct on (n.nspname, p.proname)
    n.nspname as schema_name,
    p.proname as function_name,
    pg_get_functiondef(p.oid) as function_definition,
    p.proargtypes as arg_oids,
    p.prorettype as return_type,
    p.proretset as returns_set,
    case when p.proargnames is not null
        -- trim unused arg names
        then (select p.proargnames[:array_length(p.proargtypes, 1)])
        else array[]::text[]
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
"#;

const TYPES_INTROSPECTION_QUERY: &'static str = r#"
with recursive type_tree as (
    -- base case: start with input oids
    select
        t.oid,
        t.typname,
        t.typtype,
        n.nspname as schema_name,
        t.typelem as array_element_type,
        t.typbasetype as domain_base_type,
        case
            when t.typtype = 'd' then (
                select array_agg(
                    pg_get_constraintdef(c.oid)
                )
                from pg_constraint c
                where c.contypid = t.oid
            )
        end as domain_composite_constraints,
        case
            when t.typtype = 'e' then (
                select array_agg(enumlabel order by enumsortorder)
                from pg_enum
                where enumtypid = t.oid
            )
        end as enum_variants,
        case
            when t.typtype = 'c' then (
                select array_agg(attname order by attnum)
                from pg_attribute
                where attrelid = t.typrelid
                and attnum > 0
                and not attisdropped
            )
        end as composite_field_names,
        case
            when t.typtype = 'c' then (
                select array_agg(atttypid order by attnum)
                from pg_attribute
                where attrelid = t.typrelid
                and attnum > 0
                and not attisdropped
            )
        end as composite_field_types,
        case
            when t.typtype = 'c' then (
                select array_agg(not attnotnull order by attnum)
                from pg_attribute
                where attrelid = t.typrelid
                and attnum > 0
                and not attisdropped
            )
        end as composite_field_nullables
    from pg_type t
    join pg_namespace n on t.typnamespace = n.oid
    where t.oid = any($1)

    union all

    -- recursive case
    select
        t.oid,
        t.typname,
        t.typtype,
        n.nspname as schema_name,
        t.typelem as array_element_type,
        t.typbasetype as domain_base_type,
        case
            when t.typtype = 'd' then (
                select array_agg(
                    pg_get_constraintdef(c.oid)
                )
                from pg_constraint c
                where c.contypid = t.oid
            )
        end as domain_composite_constraints,
        case
            when t.typtype = 'e' then (
                select array_agg(enumlabel order by enumsortorder)
                from pg_enum
                where enumtypid = t.oid
            )
        end as enum_variants,
        case
            when t.typtype = 'c' then (
                select array_agg(attname order by attnum)
                from pg_attribute
                where attrelid = t.typrelid
                and attnum > 0
                and not attisdropped
            )
        end as composite_field_names,
        case
            when t.typtype = 'c' then (
                select array_agg(atttypid order by attnum)
                from pg_attribute
                where attrelid = t.typrelid
                and attnum > 0
                and not attisdropped
            )
        end as composite_field_types,
        case
            when t.typtype = 'c' then (
                select array_agg(not attnotnull order by attnum)
                from pg_attribute
                where attrelid = t.typrelid
                and attnum > 0
                and not attisdropped
            )
        end as composite_field_nullables
    from pg_type t
    join pg_namespace n on t.typnamespace = n.oid
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
    typtype,
    schema_name,
    array_element_type,
    domain_base_type,
    domain_composite_constraints,
    enum_variants,
    composite_field_names,
    composite_field_types,
    composite_field_nullables
from type_tree;
"#;

#[derive(Debug)]
struct PgField {
    name: String,
    type_oid: OID,
    nullable: bool,
}

/// Postgres object ID
/// Uniquely identifies database objects
type OID = u32;

#[derive(Debug)]
enum PgType {
    Array {
        schema: String,
        element_type_oid: OID,
    },
    Composite {
        schema: String,
        name: String,
        fields: Vec<PgField>,
    },
    Enum {
        schema: String,
        name: String,
        variants: Vec<String>,
    },
    Domain {
        schema: String,
        name: String,
        type_oid: OID,
        constraints: Vec<String>,
    },
    Custom {
        schema: String,
        name: String,
    },
    Int32,
    Int64,
    Bool,
    Text,
}

#[derive(Debug)]
struct PgFn {
    schema: String,
    name: String,
    args: Vec<PgArg>,
    return_type_oid: OID,
    returns_set: bool,
    definition: String,
}

#[derive(Debug)]
struct PgArg {
    name: String,
    type_oid: OID,
}

async fn main(db: &Client) -> anyhow::Result<()> {
    let (fns, type_oids) = fetch_functions(db).await?;
    let types = fetch_types(db, &type_oids).await?;
    let type_def_code = codegen_types(&types)?;
    // let fn_code = codegen_fns(&fns, &types)?;

    let syntax_tree = syn::parse2::<syn::File>(type_def_code)?;
    let formatted = prettyplease::unparse(&syntax_tree);
    println!("{}", formatted);

    Ok(())
}

async fn fetch_functions(db: &Client) -> anyhow::Result<(Vec<PgFn>, Vec<OID>)> {
    let fn_rows = db.query(FUNCTION_INTROSPECTION_QUERY, &[]).await?;

    let mut type_oids: HashSet<OID> = HashSet::new();
    let mut pg_fns: Vec<PgFn> = Vec::new();

    for row in fn_rows {
        let arg_type_oids: Vec<OID> = row.get("arg_oids");
        let arg_names: Vec<String> = row.get("arg_names");
        let return_type_oid: OID = row.get("return_type");

        let args = arg_type_oids
            .iter()
            .zip(arg_names)
            .map(|(oid, name)| PgArg {
                name: name.clone(),
                type_oid: *oid,
            })
            .collect();

        type_oids.extend(arg_type_oids);
        type_oids.insert(return_type_oid);

        pg_fns.push(PgFn {
            schema: row.get("schema_name"),
            name: row.get("function_name"),
            definition: row.get("function_definition"),
            returns_set: row.get("returns_set"),
            args,
            return_type_oid,
        });
    }

    Ok((pg_fns, Vec::from_iter(type_oids)))
}

async fn fetch_types(db: &Client, type_oids: &[OID]) -> anyhow::Result<HashMap<OID, PgType>> {
    let types = db
        .query(TYPES_INTROSPECTION_QUERY, &[&Vec::from_iter(type_oids)])
        .await?;

    let mut type_registry = HashMap::new();

    for t in types {
        let name: &str = t.get("typname");
        let schema: String = t.get("schema_name");

        let pg_type = match t.get::<_, i8>("typtype") as u8 as char {
            'b' => match name {
                "int4" => PgType::Int32,
                "int8" => PgType::Int64,
                "text" => PgType::Text,
                "bool" => PgType::Bool,
                a if a.starts_with("_") && t.get::<&str, u32>("array_element_type") != 0 => {
                    PgType::Array {
                        schema,
                        element_type_oid: t.get("array_element_type"),
                    }
                }
                x => unimplemented!("base type not implemented {}", x),
            },
            'c' => PgType::Composite {
                schema,
                name: name.to_string(),
                fields: t
                    .get::<&str, Vec<&str>>("composite_field_names")
                    .iter()
                    .zip(t.get::<&str, Vec<u32>>("composite_field_types"))
                    .zip(t.get::<&str, Vec<bool>>("composite_field_nullables"))
                    .map(|((name, ty), nullable)| PgField {
                        name: name.to_string(),
                        type_oid: ty,
                        nullable,
                    })
                    .collect(),
            },
            'd' => PgType::Domain {
                schema,
                name: name.to_string(),
                type_oid: t.get("domain_base_type"),
                constraints: t.get("domain_composite_constraints"),
            },
            'e' => PgType::Enum {
                schema,
                name: name.to_string(),
                variants: t.get("enum_variants"),
            },
            x => unimplemented!("ttype not implemented {}", x),
        };

        type_registry.insert(t.get::<&str, u32>("oid"), pg_type);
    }

    Ok(type_registry)
}

enum CaseType {
    Snake,
    Pascal,
}

/// Converts an arbitrary postgres identifier to a valid Rust identifier with a given
/// case. Will ensure any invalid strings are turned into raw identifiers.
fn clean_identifier(name: &str, case_type: CaseType) -> TokenStream {
    let name = match case_type {
        CaseType::Snake => name.to_snake_case(),
        CaseType::Pascal => name.to_pascal_case(),
    };

    match syn::parse_str::<syn::Ident>(&name) {
        Ok(ident) => ident.into_token_stream(),
        Err(_) => ("r#".to_owned() + &name).parse().unwrap(),
    }
}

fn to_ident(t: &PgType, types: &HashMap<OID, PgType>) -> TokenStream {
    match t {
        PgType::Domain { name, .. } => clean_identifier(name, CaseType::Pascal),
        PgType::Composite { name, .. } => clean_identifier(name, CaseType::Pascal),
        PgType::Enum { name, .. } => clean_identifier(name, CaseType::Pascal),
        PgType::Array {
            element_type_oid, ..
        } => {
            let inner = to_ident(types.get(element_type_oid).unwrap(), types);
            quote! { Vec<#inner> }
        }
        PgType::Int32 => "i32".parse().unwrap(),
        PgType::Bool => "bool".parse().unwrap(),
        PgType::Text => "String".parse().unwrap(),
        x => unimplemented!("unknown ident {:?}", x),
    }
}

trait ToRust {
    fn to_rust(&self, types: &HashMap<OID, PgType>) -> TokenStream;
}

impl ToRust for PgField {
    fn to_rust(&self, types: &HashMap<OID, PgType>) -> TokenStream {
        let ident = to_ident(types.get(&self.type_oid).unwrap(), types);
        let field_name = clean_identifier(&self.name, CaseType::Snake);

        if self.nullable {
            quote! {
                #field_name: Option<#ident>
            }
        } else {
            quote! {
                #field_name: #ident
            }
        }
    }
}

impl ToRust for PgType {
    fn to_rust(&self, types: &HashMap<OID, PgType>) -> TokenStream {
        match self {
            PgType::Composite {
                schema,
                name,
                fields,
            } => {
                let name = clean_identifier(name, CaseType::Pascal);
                let field_tokens: Vec<TokenStream> =
                    fields.into_iter().map(|f| f.to_rust(types)).collect();

                quote! {
                    struct #name {
                        #(#field_tokens),*
                    }
                }
            }
            PgType::Enum {
                schema,
                name,
                variants,
            } => {
                let name = clean_identifier(name, CaseType::Pascal);
                let variants: Vec<TokenStream> = variants
                    .into_iter()
                    .map(|v| clean_identifier(&v, CaseType::Pascal))
                    .collect();

                quote! {
                    enum #name {
                        #(#variants),*
                    }
                }
            }
            PgType::Domain {
                schema,
                name,
                type_oid,
                constraints,
            } => {
                let name = clean_identifier(name, CaseType::Pascal);
                let inner = to_ident(types.get(type_oid).unwrap(), types);

                quote! {
                    struct #name(#inner);
                }
            }

            // for base types, we intentionally emit nothing (since they already exist in Rust).
            x => quote! {},
        }
    }
}

fn codegen_types(types: &HashMap<OID, PgType>) -> anyhow::Result<TokenStream> {
    let mut type_streams: Vec<TokenStream> = Vec::new();

    for t in types.values() {
        let tokens = t.to_rust(types);
        if !tokens.is_empty() {
            type_streams.push(tokens);
        }
    }

    let r = quote! {
        mod types {
            #(#type_streams)*
        }
    };

    Ok(r)
}

fn codegen_fns(fns: &[PgFn], types: &HashMap<OID, PgType>) -> anyhow::Result<String> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
    use tokio_postgres::{Client, NoTls};

    #[tokio::test]
    async fn test_infer_types() -> anyhow::Result<()> {
        let db = setup_test_db().await;

        db.client
            .simple_query(
                r#"
                create type a as (
                    int_field int,
                    text_field text,
                    type int[]
                );

                create type b as (
                    a_field a
                );

                create table tab (
                    id serial primary key
                );

                create domain b_with_non_null_a as b
                    check (
                        (value).a_field is not null
                    );

                create type test_enum as enum ('enum_variant_1', 'enum_variant_2');

                create function test_fn(arg_a a, num int, ray a[], e test_enum, dom b_with_non_null_a) returns setof tab as $$
                    begin
                    return query select * from tab;
                    end;
                $$ language plpgsql;

                create schema api;

                 create function api_fn(arg_e test_enum) returns test_enum as $$
                    begin
                    return arg_e;
                    end;
                $$ language plpgsql;

                "#,
            )
            .await?;

        main(&db.client).await?;

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

// 1. arrange generated types into schema mods
// 2. generate function signatures
// 3. constraint analysis (for domains)
// 4. exception tracking

// instead of using lots of features, just allow overrides using a toml file
// then the 'features' just merge in default values into the config objection (e.g. numeric -> rust_decimal)
// config specifies both the mapping AND any imports necessary

// I don't really need a separate mod for types and functions. It should just be per schema.
