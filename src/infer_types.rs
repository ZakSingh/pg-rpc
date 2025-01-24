use crate::infer_types::CaseType::Snake;
use crate::parse_domain::non_null_cols_from_checks;
use heck::{ToPascalCase, ToSnakeCase};
use itertools::Itertools;
use pg_query::protobuf::node::Node;
use postgres_types::{FromSql, ToSql};
use quote::__private::TokenStream;
use quote::{quote, ToTokens};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use syn::parse::Parser;
use tokio_postgres::{Client, GenericClient, Row};

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
        end as arg_names,
    array(
            select n > (array_length(p.proargtypes, 1) - p.pronargdefaults)
            from generate_series(1, array_length(p.proargtypes, 1)) as n
    ) as has_defaults
from pg_proc p
         join pg_namespace n on p.pronamespace = n.oid
         left join pg_type t on t.oid = any(p.proargtypes)
where p.prokind = 'f'
  and n.nspname not in ('pg_catalog', 'information_schema')
  and not exists (
    select 1
    from pg_depend d
    where d.objid = p.oid
      and d.deptype = 'e'
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
            case when exists (
                select 1
                from pg_class c
                where c.oid = t.typrelid
                and c.relkind = 'v'
            ) then (
                with view_columns as (
                    select
                        a.attname,
                        a.attnum,
                        coalesce(derived.is_nullable, 'YES') as is_nullable
                    from pg_attribute a
                    join pg_class cl on cl.oid = a.attrelid
                    join pg_namespace ns on ns.oid = cl.relnamespace
                    left join (
                        select
                            vcu.column_name,
                            vcu.view_name,
                            vcu.view_schema,
                            cols.is_nullable
                        from information_schema.view_column_usage vcu
                        join information_schema.columns cols on
                            cols.column_name = vcu.column_name
                            and cols.table_name = vcu.table_name
                            and cols.table_schema = vcu.table_schema
                    ) derived on
                        derived.column_name = a.attname
                        and derived.view_name = cl.relname
                        and derived.view_schema = ns.nspname
                    where a.attrelid = t.typrelid
                    and a.attnum > 0
                    and not a.attisdropped
                )
                select array_agg(
                    case when is_nullable = 'YES' then true else false end
                    order by attnum
                )
                from view_columns
            ) else (
                select array_agg(not attnotnull order by attnum)
                from pg_attribute
                where attrelid = t.typrelid
                and attnum > 0
                and not attisdropped
            ) end
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
            case when exists (
                select 1
                from pg_class c
                where c.oid = t.typrelid
                and c.relkind = 'v'
            ) then (
                with view_columns as (
                    select
                        a.attname,
                        a.attnum,
                        coalesce(derived.is_nullable, 'YES') as is_nullable
                    from pg_attribute a
                    join pg_class cl on cl.oid = a.attrelid
                    join pg_namespace ns on ns.oid = cl.relnamespace
                    left join (
                        select
                            vcu.column_name,
                            vcu.view_name,
                            vcu.view_schema,
                            cols.is_nullable
                        from information_schema.view_column_usage vcu
                        join information_schema.columns cols on
                            cols.column_name = vcu.column_name
                            and cols.table_name = vcu.table_name
                            and cols.table_schema = vcu.table_schema
                    ) derived on
                        derived.column_name = a.attname
                        and derived.view_name = cl.relname
                        and derived.view_schema = ns.nspname
                    where a.attrelid = t.typrelid
                    and a.attnum > 0
                    and not a.attisdropped
                )
                select array_agg(
                    case when is_nullable = 'YES' then true else false end
                    order by attnum
                )
                from view_columns
            ) else (
                select array_agg(not attnotnull order by attnum)
                from pg_attribute
                where attrelid = t.typrelid
                and attnum > 0
                and not attisdropped
            ) end
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

type SchemaName = String;

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

impl PgType {
    pub fn schema(&self) -> SchemaName {
        match self {
            PgType::Composite { schema, .. } => schema,
            PgType::Enum { schema, .. } => schema,
            PgType::Array { schema, .. } => schema,
            PgType::Domain { schema, .. } => schema,
            PgType::Custom { schema, .. } => schema,
            _ => "pg_catalog",
        }
        .to_owned()
    }

    fn to_rust_ident(&self, types: &HashMap<OID, PgType>) -> TokenStream {
        match self {
            // For user-defined types, qualify with schema name
            PgType::Domain { schema, name, .. } => {
                let schema_mod = clean_identifier(schema, CaseType::Snake);
                let type_name = clean_identifier(name, CaseType::Pascal);
                quote! { super::#schema_mod::#type_name }
            }
            PgType::Composite { schema, name, .. } => {
                let schema_mod = clean_identifier(schema, CaseType::Snake);
                let type_name = clean_identifier(name, CaseType::Pascal);
                quote! { super::#schema_mod::#type_name }
            }
            PgType::Enum { schema, name, .. } => {
                let schema_mod = clean_identifier(schema, CaseType::Snake);
                let type_name = clean_identifier(name, CaseType::Pascal);
                quote! { super::#schema_mod::#type_name }
            }
            PgType::Array {
                element_type_oid, ..
            } => {
                let inner = types.get(element_type_oid).unwrap().to_rust_ident(types);
                quote! { Vec<#inner> }
            }
            // Built-in types don't need schema qualification
            PgType::Int32 => quote! { i32 },
            PgType::Int64 => quote! { i64 },
            PgType::Bool => quote! { bool },
            PgType::Text => quote! { String },
            x => unimplemented!("unknown type {:?}", x),
        }
    }
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

#[derive(Debug, Clone)]
struct PgArg {
    name: String,
    type_oid: OID,
    has_default: bool,
}

async fn main(db: &Client) -> anyhow::Result<()> {
    let (fns, type_oids) = fetch_functions(db).await?;
    let types = fetch_types(db, &type_oids).await?;
    let type_def_code = codegen_types(&types);
    let fn_code = codegen_fns(&fns, &types);

    let out: String = type_def_code
        .into_iter()
        .chain(fn_code) // Combine both maps into one iterator
        .into_grouping_map()
        .fold(TokenStream::new(), |acc, _, ts| quote! { #acc #ts })
        .iter()
        .map(|(schema, tokens)| {
            let s = clean_identifier(schema.as_str(), CaseType::Snake);
            prettyplease::unparse(
                &syn::parse2::<syn::File>(quote! {
                    mod #s {
                        #tokens
                    }
                })
                .expect("generated code to parse"),
            )
        })
        .collect();

    println!("{}", out);

    Ok(())
}

async fn fetch_functions(
    db: &Client,
) -> anyhow::Result<(HashMap<SchemaName, Vec<PgFn>>, Vec<OID>)> {
    let fn_rows = db.query(FUNCTION_INTROSPECTION_QUERY, &[]).await?;

    let mut type_oids: HashSet<OID> = HashSet::new();
    let mut pg_fns: HashMap<SchemaName, Vec<PgFn>> = HashMap::new();

    for row in fn_rows {
        let schema: SchemaName = row.get("schema_name");
        let arg_type_oids: Vec<OID> = row.get("arg_oids");
        let arg_names: Vec<String> = row.get("arg_names");
        let return_type_oid: OID = row.get("return_type");
        let arg_defaults: Vec<bool> = row.get("has_defaults");

        let args = arg_type_oids
            .iter()
            .zip(arg_names)
            .zip(arg_defaults)
            .map(|((oid, name), has_default)| PgArg {
                name: name.clone(),
                type_oid: *oid,
                has_default,
            })
            .collect();

        type_oids.extend(arg_type_oids);
        type_oids.insert(return_type_oid);

        let f = PgFn {
            schema: schema.clone(),
            name: row.get("function_name"),
            definition: row.get("function_definition"),
            returns_set: row.get("returns_set"),
            args,
            return_type_oid,
        };

        if let Some(val) = pg_fns.get_mut(&schema) {
            val.push(f);
        } else {
            pg_fns.insert(schema, vec![f]);
        }
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
            'p' => {
                unimplemented!();
            }
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
    let prefix = if starts_with_number(&name) { "_" } else { "" };
    let name = prefix.to_string()
        + &match case_type {
            CaseType::Snake => name.to_snake_case(),
            CaseType::Pascal => {
                // Preserve _ prefixes (heck removes them for some reason)
                if name.chars().next() == Some('_') {
                    "_".to_string().as_str().to_owned() + name.to_pascal_case().as_str()
                } else {
                    name.to_pascal_case()
                }
            }
        };

    match syn::parse_str::<syn::Ident>(&name) {
        Ok(ident) => ident.into_token_stream(),
        Err(_) => ("r#".to_owned() + &name).parse().unwrap(),
    }
}

fn starts_with_number(s: &str) -> bool {
    s.chars().next().map(|c| c.is_numeric()).unwrap_or(false)
}

trait ToRust {
    fn to_rust(&self, types: &HashMap<OID, PgType>) -> TokenStream;
}

impl ToRust for PgField {
    fn to_rust(&self, types: &HashMap<OID, PgType>) -> TokenStream {
        let ident = types.get(&self.type_oid).unwrap().to_rust_ident(types);
        let field_name = clean_identifier(&self.name, CaseType::Snake);
        let pg_name = &self.name;

        if self.nullable {
            quote! {
                #[postgres(name = #pg_name)]
                pub #field_name: Option<#ident>
            }
        } else {
            quote! {
                #[postgres(name = #pg_name)]
                pub #field_name: #ident
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
                let rs_name = clean_identifier(name, CaseType::Pascal);
                let field_tokens: Vec<TokenStream> =
                    fields.into_iter().map(|f| f.to_rust(types)).collect();

                let field_mappings: Vec<_> = fields
                    .into_iter()
                    .map(|f| {
                        let sql_name = &f.name;
                        let rs_name = clean_identifier(&f.name, CaseType::Snake);

                        quote! { #rs_name: row.try_get(#sql_name)? }
                    })
                    .collect();

                quote! {
                    #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
                    #[postgres(name = #name)]
                    pub struct #rs_name {
                        #(#field_tokens),*
                    }

                    impl TryFrom<tokio_postgres::Row> for #rs_name {
                        type Error = tokio_postgres::Error;

                        fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
                            Ok(Self {
                                #(#field_mappings),*
                            })
                        }
                    }
                }
            }
            PgType::Domain {
                schema,
                name,
                type_oid,
                constraints,
            } => {
                let rs_name = clean_identifier(name, CaseType::Pascal);

                match types.get(type_oid).unwrap() {
                    PgType::Composite { fields, .. } => {
                        // If the domain wraps a composite type, we want to create a new composite type
                        // with the non-null constraints enforced by the domain rather than creating a new wrapper type.
                        // TODO: Refactor for DRY
                        let c: Vec<&str> = constraints.into_iter().map(|s| s.as_str()).collect();
                        let non_null_cols = non_null_cols_from_checks(&c).unwrap();

                        let field_tokens: Vec<TokenStream> = fields
                            .into_iter()
                            .map(|mut f| {
                                PgField {
                                    nullable: f.nullable && !non_null_cols.contains(&f.name),
                                    name: f.name.clone(),
                                    type_oid: f.type_oid,
                                }
                                .to_rust(types)
                            })
                            .collect();

                        let field_mappings: Vec<_> = fields
                            .into_iter()
                            .map(|f| {
                                let sql_name = &f.name;
                                let rs_name = clean_identifier(&f.name, CaseType::Snake);

                                quote! { #rs_name: row.try_get(#sql_name)? }
                            })
                            .collect();

                        // TODO: Do I need custom ToSql and FromSql implementations that re-add the wrapper?
                        quote! {
                            #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
                            #[postgres(name = #name)]
                            pub struct #rs_name {
                                #(#field_tokens),*
                            }

                            impl TryFrom<tokio_postgres::Row> for #rs_name {
                                type Error = tokio_postgres::Error;

                                fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
                                    Ok(Self {
                                        #(#field_mappings),*
                                    })
                                }
                            }
                        }
                    }
                    pg_type => {
                        // For any type besides composite types, domains are just a wrapper.
                        let inner = pg_type.to_rust_ident(types);
                        quote! {
                            #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
                            #[postgres(name = #name)]
                            pub struct #rs_name(#inner);
                        }
                    }
                }
            }
            PgType::Enum {
                schema,
                name,
                variants,
            } => {
                let clean_name = clean_identifier(name, CaseType::Pascal);
                let variants: Vec<TokenStream> = variants
                    .into_iter()
                    .map(|v| {
                        let clean_v = clean_identifier(&v, CaseType::Pascal);

                        quote! {
                            #[postgres(name = #v)]
                            #clean_v
                        }
                    })
                    .collect();

                quote! {
                    #[derive(Debug, Clone, Copy, postgres_types::FromSql, postgres_types::ToSql)]
                    #[postgres(name = #name)]
                    pub enum #clean_name {
                        #(#variants),*
                    }
                }
            }
            // Skip base types like i32/i64 as they are already-defined primitives
            PgType::Int32 | PgType::Int64 | PgType::Text | PgType::Bool => quote! {},
            // No need to create type aliases for arrays. Instead they'll be used as Vec<Inner>
            PgType::Array { .. } => quote! {},
            x => unimplemented!("Unhandled PgType {:?}", x),
        }
    }
}

fn codegen_types(types: &HashMap<OID, PgType>) -> HashMap<SchemaName, TokenStream> {
    types
        .values()
        .map(|t| (t.schema(), t.to_rust(types)))
        .filter(|(_, tokens)| !tokens.is_empty())
        .into_group_map()
        .into_iter()
        .map(|(schema, token_streams)| {
            (
                schema,
                quote! {
                    #(#token_streams)*
                },
            )
        })
        .collect()
}

impl PgArg {
    pub fn rs_name(&self) -> TokenStream {
        clean_identifier(&self.name, CaseType::Snake)
    }
}

impl ToRust for PgArg {
    fn to_rust(&self, types: &HashMap<OID, PgType>) -> TokenStream {
        let name = clean_identifier(&self.name, CaseType::Snake);
        let ty = match types.get(&self.type_oid).unwrap() {
            PgType::Int32 => quote! { i32 },
            PgType::Int64 => quote! { i64 },
            PgType::Bool => quote! { bool },
            PgType::Text => quote! { &str },
            t @ PgType::Enum { .. } => {
                // Enums are passed by copy
                let id = t.to_rust_ident(types);
                quote! { #id }
            }
            t => {
                let id = t.to_rust_ident(types);
                quote! { &#id }
            }
        };

        if self.has_default {
            quote! {
                #name: Option<#ty>
            }
        } else {
            quote! {
                #name: #ty
            }
        }
    }
}

impl ToRust for PgFn {
    fn to_rust(&self, types: &HashMap<OID, PgType>) -> TokenStream {
        let args: Vec<TokenStream> = self.args.iter().map(|a| a.to_rust(types)).collect();

        // TODO: replace with custom error type later
        let error_type: TokenStream = "tokio_postgres::Error".parse().unwrap();

        let mapping = {
            let (opt_args, req_args): (Vec<PgArg>, Vec<PgArg>) =
                self.args.iter().cloned().partition(|n| n.has_default);

            // Generate required query parameters positionally ($1, $2, ..., $n)
            let required_query_params = (1..=self.args.len() - opt_args.len())
                .map(|i| format!("${}", i))
                .collect::<Vec<_>>()
                .join(", ");

            let req_arg_names: Vec<TokenStream> = req_args.iter().map(|a| a.rs_name()).collect();
            let opt_arg_names: Vec<TokenStream> = opt_args.iter().map(|a| a.rs_name()).collect();
            let opt_arg_sql_names: Vec<&str> = opt_args.iter().map(|a| a.name.as_str()).collect();

            let query_string = if self.returns_set {
                format!("select * from {}.{}", self.schema, self.name)
            } else {
                format!("select {}.{}", self.schema, self.name)
            };

            let query = if self.returns_set {
                quote! {
                    client
                        .query(&query, &params)
                        .await
                        .and_then(|rows| {
                            rows.into_iter().map(TryInto::try_into).collect()
                        })
                }
            } else {
                quote! {
                    client
                        .query_one(&query, &params)
                        .await
                        .and_then(|r| r.try_get(0))
                }
            };

            quote! {
                let mut params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![#(&#req_arg_names),*];
                let mut query = concat!(#query_string, "(", #required_query_params).to_string();

                #(
                    if let Some(ref value) = #opt_arg_names {
                        params.push(value);
                        query.push_str(concat!(", ", #opt_arg_sql_names, ":= $"));
                        query.push_str(&params.len().to_string());
                    }
                )*

                query.push_str(")");

                #query
            }
        };

        let rs_fn_name = self.rs_name();
        let return_type = {
            let inner_ty = types
                .get(&self.return_type_oid)
                .unwrap()
                .to_rust_ident(types);

            if self.returns_set {
                quote! { Vec<#inner_ty> }
            } else {
                inner_ty
            }
        };

        quote! {
            pub async fn #rs_fn_name(client: &tokio_postgres::Client, #(#args),*) -> Result<#return_type, #error_type> {
                #mapping
            }
        }
    }
}

impl PgFn {
    pub fn rs_name(&self) -> TokenStream {
        clean_identifier(&self.name, CaseType::Snake)
    }
}

fn codegen_fns(
    fns: &HashMap<SchemaName, Vec<PgFn>>,
    types: &HashMap<OID, PgType>,
) -> HashMap<SchemaName, TokenStream> {
    fns.iter()
        .map(|(schema, fns)| {
            let fn_rust: Vec<TokenStream> = fns.iter().map(|f| f.to_rust(types)).collect();
            (
                schema.clone(),
                quote! {
                    #(#fn_rust)*
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::setup_test_db;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
    use tokio_postgres::{Client, NoTls};

    #[tokio::test]
    async fn hello() {
        let db = setup_test_db().await;

        db.client
            .simple_query(
                r#"
                create type role as enum ('admin', 'user');

                create table account (
                    account_id int primary key generated always as identity,
                    name text not null,
                    age int,
                    role role not null
                );

                create table post (
                    post_id int primary key generated always as identity,
                    author_id int not null references account(account_id),
                    content text
                );

                create schema api;

                create function api.optional_argument(required int, opt_1 int , opt_2 bool)
                returns int as $$
                begin
                   return 1;
                end;
                $$ language plpgsql;
                "#,
            )
            .await
            .unwrap();

        main(&db.client).await.unwrap();
        //
        // let res = api::create_account(&db.client, "Zak", 25, public::Role::Admin)
        //     .await
        //     .unwrap();
        //
        // let x = api::get_account_with_posts(&db.client, res.account_id)
        //     .await
        //     .unwrap();

        // dbg!(x);
    }
}

// TODO:
// 1. Table return types
// 3. Fetch functions and fetch types should be refactored to use a fromsql From<Row> implementation.
// 4. Nullability/cardinality inference for return type?
// 5. Exception tracking / error enum generation
// 6. Default null parameters
// 7. Collect comments on functions, arguments, return type, composite types, etc.

// If it's a `returns setof composite_type` it expands each to a row
// If it's `returns table(name text, age int, email text)`, it expands to rows
// If it's `returns record` it's the same thing

// so it's only in that case that we need From<Row> and the try_from
// I only need this for composite rows, right? What about domains?
