use crate::annotations;
use crate::codegen::ToRust;
use crate::codegen::{SchemaName, OID};
use crate::config::Config;
use crate::ident::{sql_to_rs_ident, sql_to_rs_string, CaseType};
use crate::parse_domain::non_null_cols_from_checks;
use itertools::izip;
use postgres::Row;
use postgres_types::FromSql;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

/// Parse @pgrpc_not_null(col1, col2, ...) annotations from a comment
pub fn parse_bulk_not_null_columns(comment: &Option<String>) -> BTreeSet<String> {
    comment
        .as_ref()
        .map(|c| annotations::parse_not_null(c))
        .unwrap_or_default()
}

#[derive(Debug, FromSql)]
struct DomainConstraint {
    name: String,
    definition: String,
}

#[derive(Debug)]
pub enum PgType {
    Array {
        schema: String,
        element_type_oid: OID,
    },
    Composite {
        schema: String,
        name: String,
        fields: Vec<PgField>,
        comment: Option<String>,
        relkind: Option<String>,
        view_definition: Option<String>,
    },
    Enum {
        schema: String,
        name: String,
        variants: Vec<String>,
        comment: Option<String>,
    },
    Domain {
        schema: String,
        name: String,
        type_oid: OID,
        constraints: Vec<DomainConstraint>,
        comment: Option<String>,
    },
    Custom {
        schema: String,
        name: String,
    },
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Numeric,
    Bool,
    Text,
    Timestamptz,
    Date,
    INet,
    Void,
    Bytea,
    Json,
    Record,
    Geography,
    Geometry,
    TsVector,
    TsQuery,
    Vector,
}

#[derive(Debug)]
pub struct PgField {
    pub name: String,
    pub type_oid: OID,
    pub nullable: bool,
    pub comment: Option<String>,
}

impl PgType {
    pub fn schema(&self) -> SchemaName {
        match self {
            PgType::Composite { schema, .. } => schema,
            PgType::Enum { schema, .. } => schema,
            PgType::Array { schema, .. } => schema,
            PgType::Domain { schema, .. } => schema,
            PgType::Custom { schema, .. } => schema,
            PgType::Record => "pg_catalog",
            _ => "pg_catalog",
        }
        .to_owned()
    }

    pub fn name(&self) -> &str {
        match self {
            PgType::Composite { name, .. } => name,
            PgType::Enum { name, .. } => name,
            PgType::Domain { name, .. } => name,
            PgType::Custom { name, .. } => name,
            PgType::Array { .. } => "array",
            PgType::Bool => "bool",
            PgType::Bytea => "bytea",
            PgType::Int16 => "int16",
            PgType::Int32 => "int32",
            PgType::Int64 => "int64",
            PgType::Float32 => "float32",
            PgType::Float64 => "float64",
            PgType::Numeric => "numeric",
            PgType::Text => "text",
            PgType::Json => "json",
            PgType::Timestamptz => "timestamptz",
            PgType::Date => "date",
            PgType::INet => "inet",
            PgType::Record => "record",
            PgType::Void => "void",
            PgType::Geography => "geography",
            PgType::Geometry => "geometry",
            PgType::TsVector => "tsvector",
            PgType::TsQuery => "tsquery",
            PgType::Vector => "vector",
        }
    }

    pub fn to_rust_ident(&self, types: &BTreeMap<OID, PgType>) -> TokenStream {
        match self {
            // For user-defined types, qualify with schema name
            PgType::Domain { schema, name, .. } => {
                let schema_mod = sql_to_rs_ident(schema, CaseType::Snake);
                let type_name = sql_to_rs_ident(name, CaseType::Pascal);
                quote! { super::#schema_mod::#type_name }
            }
            PgType::Composite { schema, name, .. } => {
                let schema_mod = sql_to_rs_ident(schema, CaseType::Snake);
                let type_name = sql_to_rs_ident(name, CaseType::Pascal);
                quote! { super::#schema_mod::#type_name }
            }
            PgType::Enum { schema, name, .. } => {
                let schema_mod = sql_to_rs_ident(schema, CaseType::Snake);
                let type_name = sql_to_rs_ident(name, CaseType::Pascal);
                quote! { super::#schema_mod::#type_name }
            }
            PgType::Array {
                element_type_oid, ..
            } => {
                let inner = types.get(element_type_oid).unwrap().to_rust_ident(types);
                quote! { Vec<#inner> }
            }
            // Built-in types don't need schema qualification
            PgType::Int16 => quote! { i16 },
            PgType::Int32 => quote! { i32 },
            PgType::Int64 => quote! { i64 },
            PgType::Float32 => quote! { f32 },
            PgType::Float64 => quote! { f64 },
            PgType::Numeric => quote! { rust_decimal::Decimal },
            PgType::Bool => quote! { bool },
            PgType::Text => quote! { String },
            PgType::Timestamptz => quote! { time::OffsetDateTime },
            PgType::Date => quote! { time::Date },
            PgType::INet => quote! { std::net::IpAddr },
            PgType::Bytea => quote! { Vec<u8> },
            PgType::Json => quote! { serde_json::Value },
            PgType::Void => quote! { () },
            PgType::Record => quote! { tokio_postgres::Row },
            PgType::Geography => quote! { postgis_butmaintained::ewkb::Geometry },
            PgType::Geometry => quote! { postgis_butmaintained::ewkb::Geometry },
            PgType::TsVector => quote! { tsvector::TsVector },
            PgType::TsQuery => quote! { tsvector::TsQuery },
            PgType::Vector => quote! { pgvector::Vector },
            x => unimplemented!("unknown type {:?}", x),
        }
    }

    /// Get the Rust type identifier and collect referenced schemas
    pub fn to_rust_ident_with_schemas(
        &self,
        types: &BTreeMap<OID, PgType>,
    ) -> (TokenStream, BTreeSet<String>) {
        let mut schemas = BTreeSet::new();

        let tokens = match self {
            // For user-defined types, use schema-qualified name without super::
            PgType::Domain { schema, name, .. } => {
                let schema_mod = sql_to_rs_ident(schema, CaseType::Snake);
                let type_name = sql_to_rs_ident(name, CaseType::Pascal);
                schemas.insert(schema_mod.to_string());
                quote! { #schema_mod::#type_name }
            }
            PgType::Composite { schema, name, .. } => {
                let schema_mod = sql_to_rs_ident(schema, CaseType::Snake);
                let type_name = sql_to_rs_ident(name, CaseType::Pascal);
                schemas.insert(schema_mod.to_string());
                quote! { #schema_mod::#type_name }
            }
            PgType::Enum { schema, name, .. } => {
                let schema_mod = sql_to_rs_ident(schema, CaseType::Snake);
                let type_name = sql_to_rs_ident(name, CaseType::Pascal);
                schemas.insert(schema_mod.to_string());
                quote! { #schema_mod::#type_name }
            }
            PgType::Array {
                element_type_oid, ..
            } => {
                let (inner, inner_schemas) = types
                    .get(element_type_oid)
                    .unwrap()
                    .to_rust_ident_with_schemas(types);
                schemas.extend(inner_schemas);
                quote! { Vec<#inner> }
            }
            // Built-in types don't need schema qualification
            PgType::Int16 => quote! { i16 },
            PgType::Int32 => quote! { i32 },
            PgType::Int64 => quote! { i64 },
            PgType::Float32 => quote! { f32 },
            PgType::Float64 => quote! { f64 },
            PgType::Numeric => quote! { rust_decimal::Decimal },
            PgType::Bool => quote! { bool },
            PgType::Text => quote! { String },
            PgType::Timestamptz => quote! { time::OffsetDateTime },
            PgType::Date => quote! { time::Date },
            PgType::INet => quote! { std::net::IpAddr },
            PgType::Bytea => quote! { Vec<u8> },
            PgType::Json => quote! { serde_json::Value },
            PgType::Void => quote! { () },
            PgType::Record => quote! { tokio_postgres::Row },
            PgType::Geography => quote! { postgis_butmaintained::ewkb::Geometry },
            PgType::Geometry => quote! { postgis_butmaintained::ewkb::Geometry },
            PgType::TsVector => quote! { tsvector::TsVector },
            PgType::TsQuery => quote! { tsvector::TsQuery },
            PgType::Vector => quote! { pgvector::Vector },
            x => unimplemented!("unknown type {:?}", x),
        };

        (tokens, schemas)
    }
}

impl TryFrom<Row> for PgType {
    type Error = postgres::Error;

    fn try_from(t: Row) -> Result<Self, Self::Error> {
        let name: String = t.get("typname");
        let comment: Option<String> = t.get("type_comment");
        let schema: String = t.get("schema_name");

        let pg_type = match t.get::<_, i8>("typtype") as u8 as char {
            'b' => match name.as_ref() {
                "int2" => PgType::Int16,
                "int4" => PgType::Int32,
                "int8" => PgType::Int64,
                "oid" => PgType::Int32, // OID maps to u32/i32
                "numeric" => PgType::Numeric,
                "text" | "citext" => PgType::Text,
                "char" | "bpchar" => PgType::Text, // char and bpchar (blank-padded char) map to String
                "varchar" => PgType::Text,
                "bool" => PgType::Bool,
                "timestamptz" => PgType::Timestamptz,
                "timestamp" => PgType::Timestamptz, // timestamp without timezone also maps to timestamptz
                "time" => PgType::Text,             // time types map to String
                "timetz" => PgType::Text,
                "uuid" => PgType::Text, // UUID as string for now
                "float4" | "real" => PgType::Float32,
                "float8" | "double precision" => PgType::Float64,
                _ if t.get::<&str, u32>("array_element_type") != 0 => PgType::Array {
                    schema,
                    element_type_oid: t.get("array_element_type"),
                },
                "date" => PgType::Date,
                "inet" => PgType::INet,
                "bytea" => PgType::Bytea,
                "ltree" => PgType::Text,
                "json" | "jsonb" => PgType::Json,
                "geography" => PgType::Geography,
                "geometry" => PgType::Geometry,
                "tsvector" => PgType::TsVector,
                "tsquery" => PgType::TsQuery,
                "vector" => PgType::Vector,
                x => unimplemented!("base type not implemented {}", x),
            },
            'c' => {
                // Get view information if this is a view
                // PostgreSQL's "char" type is returned as i8
                let relkind: Option<char> = t
                    .try_get::<_, Option<i8>>("relkind")
                    .ok()
                    .flatten()
                    .map(|v| v as u8 as char);
                let view_definition: Option<String> = t.try_get("view_definition").ok().flatten();
                let _is_view = relkind == Some('v') || relkind == Some('m');

                // Check if composite_field_names is NULL (happens when composite type has no fields)
                let field_names_opt: Option<Vec<&str>> = t.try_get("composite_field_names").ok();

                if field_names_opt.is_none()
                    || field_names_opt
                        .as_ref()
                        .map(|v| v.is_empty())
                        .unwrap_or(true)
                {
                    // Handle composite types with no fields
                    let type_oid: u32 = t.get("oid");
                    eprintln!(
                        "WARNING: Composite type '{}.{}' (OID: {}) has no fields defined.",
                        schema, name, type_oid
                    );
                    if schema == "tasks" {
                        eprintln!("  This appears to be a task type. Make sure the corresponding payload type is defined in the database.");
                        eprintln!(
                            "  Example: CREATE TYPE tasks.{} AS (field1 type1, field2 type2, ...);",
                            name
                        );
                    }
                    PgType::Composite {
                        schema,
                        name,
                        comment,
                        fields: vec![],
                        relkind: relkind.map(|c| c.to_string()),
                        view_definition,
                    }
                } else {
                    // Parse type-level bulk not null annotations
                    let bulk_not_null_columns = parse_bulk_not_null_columns(&comment);
                    
                    PgType::Composite {
                        schema,
                        name,
                        comment,
                        fields: izip!(
                            field_names_opt.unwrap(),
                            t.get::<&str, Vec<u32>>("composite_field_types"),
                            t.get::<&str, Vec<bool>>("composite_field_nullables"),
                            t.get::<&str, Vec<Option<String>>>("composite_field_comments")
                        )
                        .map(|(name, ty, nullable, comment)| {
                            let field_name = name.to_string();
                            let is_bulk_not_null = bulk_not_null_columns.contains(&field_name);
                            let is_column_not_null = comment
                                .as_ref()
                                .is_some_and(|c| annotations::has_not_null(c));

                            PgField {
                                name: field_name,
                                type_oid: ty,
                                nullable: nullable && !is_column_not_null && !is_bulk_not_null,
                                comment: comment.clone(),
                            }
                        })
                        .collect(),
                        relkind: relkind.map(|c| c.to_string()),
                        view_definition,
                    }
                }
            }
            'd' => {
                let constraints = izip!(
                    t.try_get::<_, Vec<String>>("domain_constraint_names")
                        .unwrap_or(Vec::default()),
                    t.try_get::<_, Vec<String>>("domain_composite_constraints")
                        .unwrap_or(Vec::default())
                )
                .map(|(name, definition)| DomainConstraint { name, definition })
                .collect();

                PgType::Domain {
                    schema,
                    name,
                    comment,
                    type_oid: t.get("domain_base_type"),
                    constraints,
                }
            }
            'e' => PgType::Enum {
                schema,
                name,
                comment,
                variants: t.get("enum_variants"),
            },
            'p' => match name.as_ref() {
                "void" => PgType::Void,
                "trigger" => PgType::Void, // Trigger functions return pseudo-type, treat as void
                "record" => PgType::Record,
                p => {
                    eprintln!("ERROR: Unhandled pseudo type encountered:");
                    eprintln!("  Type name: {}", name);
                    eprintln!("  Schema: {}", schema);
                    eprintln!("  Pseudo type: {}", p);
                    eprintln!("  Type category: p (pseudo)");
                    if let Ok(oid) = t.try_get::<_, u32>("oid") {
                        eprintln!("  OID: {}", oid);
                    }
                    unimplemented!("pseudo type not implemented: {}", p)
                }
            },
            x => unimplemented!("ttype not implemented {}", x),
        };

        Ok(pg_type)
    }
}

/// Determines additional derives and custom implementations for domain types based on the inner type
fn determine_domain_traits(
    inner_type: &PgType,
    domain_name: &TokenStream,
) -> (Vec<TokenStream>, Vec<TokenStream>) {
    let mut derives = Vec::new();
    let mut impls = Vec::new();

    match inner_type {
        // Copy types - add Copy and ordering traits
        PgType::Int16 | PgType::Int32 | PgType::Int64 | PgType::Bool => {
            derives.push(quote! { PartialEq });
            derives.push(quote! { Eq });
            derives.push(quote! { Hash });
            derives.push(quote! { Copy });
            derives.push(quote! { PartialOrd });
            derives.push(quote! { Ord });
        }

        // Float types - Copy but only partial equality/ordering (no Eq/Ord/Hash due to NaN)
        PgType::Float32 | PgType::Float64 => {
            derives.push(quote! { PartialEq });
            derives.push(quote! { Copy });
            derives.push(quote! { PartialOrd });
        }

        // String types - add ordering traits and custom Display impl
        PgType::Text => {
            derives.push(quote! { PartialEq });
            derives.push(quote! { Eq });
            derives.push(quote! { Hash });
            derives.push(quote! { PartialOrd });
            derives.push(quote! { Ord });

            // Custom Display implementation that delegates to inner String
            impls.push(quote! {
                impl std::fmt::Display for #domain_name {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        self.0.fmt(f)
                    }
                }
            });
        }

        // Other orderable types - add ordering traits but not Copy
        PgType::Numeric | PgType::Timestamptz | PgType::Date => {
            derives.push(quote! { PartialEq });
            derives.push(quote! { Eq });
            derives.push(quote! { Hash });
            derives.push(quote! { PartialOrd });
            derives.push(quote! { Ord });
        }

        // For all other types, just the basic traits
        _ => {
            derives.push(quote! { PartialEq });
            derives.push(quote! { Eq });
            derives.push(quote! { Hash });
        }
    }

    (derives, impls)
}

impl ToRust for PgType {
    fn to_rust(&self, types: &BTreeMap<OID, PgType>, config: &Config) -> TokenStream {
        match self {
            PgType::Composite {
                name,
                schema,
                comment,
                fields,
                ..
            } => {
                let rs_name = sql_to_rs_ident(name, CaseType::Pascal);

                let field_tokens: Vec<TokenStream> = fields
                    .into_iter()
                    .map(|f| f.to_rust(types, config))
                    .collect();

                let field_extractions: Vec<_> = fields
                    .iter()
                    .map(|f| {
                        let sql_name = &f.name;
                        let rs_name = sql_to_rs_ident(&f.name, CaseType::Snake);
                        let rs_name_str = f.name.clone();
                        let var_name = format_ident!("_{}", rs_name_str);

                        quote! {
                            let #var_name = row.try_get(#sql_name)?;
                        }
                    })
                    .collect();

                let field_assignments: Vec<_> = fields
                    .into_iter()
                    .map(|f| {
                        let rs_name = sql_to_rs_ident(&f.name, CaseType::Snake);
                        let rs_name_str = f.name.clone();
                        let var_name = format_ident!("_{}", rs_name_str);
                        quote! { #rs_name: #var_name }
                    })
                    .collect();

                let try_from_impl = quote! {
                    impl TryFrom<tokio_postgres::Row> for #rs_name {
                        type Error = tokio_postgres::Error;

                        fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
                            #(#field_extractions)*

                            Ok(Self {
                                #(#field_assignments),*
                            })
                        }
                    }
                };

                let comment_macro = if comment.is_some() {
                    quote! { #[doc=#comment] }
                } else {
                    quote! {}
                };

                let deserialize_derive = if config.should_disable_deserialize(schema, name) {
                    quote! {}
                } else {
                    quote! { serde::Deserialize, }
                };

                let builder_derive = if comment.as_ref().map_or(false, |c| annotations::has_builder(c)) {
                    quote! { bon::Builder, }
                } else {
                    quote! {}
                };

                quote! {
                    #comment_macro
                    #[derive(Clone, Debug, #builder_derive serde::Serialize, #deserialize_derive postgres_types::FromSql, postgres_types::ToSql)]
                    #[postgres(name = #name)]
                    pub struct #rs_name {
                        #(#field_tokens),*
                    }

                    #try_from_impl
                }
            }
            PgType::Domain {
                name,
                schema,
                comment,
                type_oid,
                constraints,
                ..
            } => {
                let rs_name = sql_to_rs_ident(name, CaseType::Pascal);

                match types.get(type_oid).unwrap() {
                    PgType::Composite {
                        fields,
                        name: pg_inner_name,
                        schema: _inner_schema,
                        ..
                    } => {
                        let rs_dom_name =
                            format_ident!("Dom{}", sql_to_rs_string(name, CaseType::Pascal));
                        let rs_dom_inner_name =
                            format_ident!("Inner{}", sql_to_rs_string(name, CaseType::Pascal));

                        // If the domain wraps a composite type, we want to create a new composite type
                        // with the non-null constraints enforced by the domain rather than creating a new wrapper type.
                        // TODO: Refactor for DRY
                        let c: Vec<&str> = constraints
                            .into_iter()
                            .map(|s| s.definition.as_str())
                            .collect();
                        let non_null_cols = non_null_cols_from_checks(&c).unwrap();

                        // TODO: strip out 'postgres()
                        let field_tokens: Vec<TokenStream> = fields
                            .into_iter()
                            .map(|f| {
                                PgField {
                                    nullable: f.nullable && !non_null_cols.contains(&f.name),
                                    name: f.name.clone(),
                                    comment: f.comment.clone(),
                                    type_oid: f.type_oid,
                                }
                                .to_rust_inner(types, false)
                            })
                            .collect();

                        let field_extractions: Vec<_> = fields
                            .iter()
                            .map(|f| {
                                let sql_name = &f.name;
                                let rs_name = sql_to_rs_ident(&f.name, CaseType::Snake);
                                let rs_name_str = f.name.clone();
                                let var_name = format_ident!("_{}", rs_name_str);

                                quote! {
                                    let #var_name = row.try_get(#sql_name)?;
                                }
                            })
                            .collect();

                        let field_assignments: Vec<_> = fields
                            .into_iter()
                            .map(|f| {
                                let rs_name = sql_to_rs_ident(&f.name, CaseType::Snake);
                                let rs_name_str = f.name.clone();
                                let var_name = format_ident!("_{}", rs_name_str);
                                quote! { #rs_name: #var_name }
                            })
                            .collect();

                        let comment_macro = if comment.is_some() {
                            quote! { #[doc=#comment] }
                        } else {
                            quote! {}
                        };

                        let deserialize_derive = if config.should_disable_deserialize(schema, name) {
                            quote! {}
                        } else {
                            quote! { serde::Deserialize, }
                        };

                        let builder_derive = if comment.as_ref().map_or(false, |c| annotations::has_builder(c)) {
                            quote! { bon::Builder, }
                        } else {
                            quote! {}
                        };

                        quote! {
                            #comment_macro
                            #[derive(Clone, Debug, #builder_derive serde::Serialize, #deserialize_derive)]
                            pub struct #rs_name {
                                #(#field_tokens),*
                            }

                            /// Internal; used for serialization/deserialization only
                            #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql, serde::Deserialize, serde::Serialize)]
                            #[postgres(name = #pg_inner_name)]
                            struct #rs_dom_inner_name {
                                #(#field_tokens),*
                            }

                            /// Internal; used for serialization/deserialization only
                            #[derive(Debug, serde::Deserialize, serde::Serialize)]
                            struct #rs_dom_name(#rs_dom_inner_name);


                            /// Manual FromSql implementation for domain wrapper that handles domain unwrapping
                            impl<'a> postgres_types::FromSql<'a> for #rs_dom_name {
                                fn from_sql(ty: &postgres_types::Type, buf: &'a [u8]) -> std::result::Result<#rs_dom_name, Box<dyn std::error::Error + std::marker::Sync + std::marker::Send>> {
                                    // Key fix: unwrap domain types as per PR #1189
                                    let actual_type = match ty.kind() {
                                        postgres_types::Kind::Domain(inner) => inner,
                                        _ => ty
                                    };
                                    <#rs_dom_inner_name as postgres_types::FromSql>::from_sql(actual_type, buf).map(#rs_dom_name)
                                }

                                fn accepts(ty: &postgres_types::Type) -> bool {
                                    ty.name() == #name || ty.name() == #pg_inner_name
                                }
                            }

                            /// Manual ToSql implementation for domain wrapper
                            impl postgres_types::ToSql for #rs_dom_name {
                                fn to_sql(&self, ty: &postgres_types::Type, out: &mut postgres_types::private::BytesMut) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                                    // For domains, we need to get the inner type
                                    let inner_ty = match ty.kind() {
                                        postgres_types::Kind::Domain(inner) => inner,
                                        _ => return Err("Expected domain type".into()),
                                    };
                                    self.0.to_sql(inner_ty, out)
                                }

                                fn accepts(ty: &postgres_types::Type) -> bool {
                                    ty.name() == #name || ty.name() == #pg_inner_name
                                }

                                postgres_types::to_sql_checked!();
                            }

                            /// Dispatch FromSql implementation to internal domain wrapper struct
                            impl<'a> postgres_types::FromSql<'a> for #rs_name {
                                fn from_sql(_type: &postgres_types::Type, buf: &'a [u8]) -> std::result::Result<#rs_name, Box<dyn std::error::Error + std::marker::Sync + std::marker::Send>> {
                                    <#rs_dom_name as postgres_types::FromSql>::from_sql(_type, buf).map(|dom| unsafe { std::mem::transmute::<#rs_dom_inner_name, #rs_name>(dom.0) })
                                }

                                fn accepts(type_: &postgres_types::Type) -> bool {
                                    <#rs_dom_name as postgres_types::FromSql>::accepts(type_)
                                }
                            }

                            impl TryFrom<tokio_postgres::Row> for #rs_name {
                                type Error = tokio_postgres::Error;

                                fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
                                    #(#field_extractions)*

                                    Ok(Self {
                                        #(#field_assignments),*
                                    })
                                }
                            }

                            impl postgres_types::ToSql for #rs_name {
                                fn to_sql(
                                    &self,
                                    ty: &postgres_types::Type,
                                    out: &mut postgres_types::private::BytesMut,
                                ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                                    let inner = unsafe { std::mem::transmute::<#rs_name, #rs_dom_inner_name>(self.clone()) };

                                    let inner_ty = match ty.kind() {
                                        postgres_types::Kind::Domain(inner) => inner,
                                        _ => return Err("Expected domain type".into()),
                                    };

                                    inner.to_sql(inner_ty, out)
                                }

                                fn accepts(ty: &postgres_types::Type) -> bool {
                                    // Match domain by name or underlying composite type name
                                    ty.name() == #name || ty.name() == #pg_inner_name
                                }
                                postgres_types::to_sql_checked!();
                            }
                        }
                    }
                    pg_type => {
                        // For any type besides composite types, domains are just a wrapper.
                        let inner = pg_type.to_rust_ident(types);
                        let (additional_derives, custom_impls) =
                            determine_domain_traits(pg_type, &rs_name);

                        let deserialize_derive = if config.should_disable_deserialize(schema, name) {
                            quote! {}
                        } else {
                            quote! { serde::Deserialize, }
                        };

                        let base_derives = quote! { Debug, Clone, derive_more::Deref, serde::Serialize, #deserialize_derive postgres_types::ToSql, postgres_types::FromSql };
                        let all_derives = if additional_derives.is_empty() {
                            base_derives
                        } else {
                            quote! { #base_derives, #(#additional_derives),* }
                        };

                        quote! {
                            #[derive(#all_derives)]
                            #[postgres(name = #name)]
                            pub struct #rs_name(pub #inner);

                            #(#custom_impls)*
                        }
                    }
                }
            }
            PgType::Enum {
                name,
                schema,
                comment,
                variants,
                ..
            } => {
                let rs_enum_name = sql_to_rs_ident(name, CaseType::Pascal);
                let rs_variants: Vec<TokenStream> = variants
                    .into_iter()
                    .map(|sql_variant| {
                        let rs_variant = sql_to_rs_ident(&sql_variant, CaseType::Pascal);

                        quote! {
                            #[postgres(name = #sql_variant)]
                            #[serde(rename = #sql_variant)]
                            #rs_variant
                        }
                    })
                    .collect();

                let comment_macro = if comment.is_some() {
                    quote! { #[doc=#comment] }
                } else {
                    quote! {}
                };

                let deserialize_derive = if config.should_disable_deserialize(schema, name) {
                    quote! {}
                } else {
                    quote! { serde::Deserialize, }
                };

                quote! {
                    #comment_macro
                    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, postgres_types::FromSql, postgres_types::ToSql, #deserialize_derive serde::Serialize)]
                    #[postgres(name = #name)]
                    pub enum #rs_enum_name {
                        #(#rs_variants),*
                    }
                }
            }
            // Skip base types like i32/i64 as they are already-defined primitives
            PgType::Int32
            | PgType::Int64
            | PgType::Int16
            | PgType::Float32
            | PgType::Float64
            | PgType::Text
            | PgType::Bool
            | PgType::Timestamptz
            | PgType::INet
            | PgType::Date
            | PgType::Bytea
            | PgType::Numeric
            | PgType::Json
            | PgType::Void
            | PgType::Record
            | PgType::Geography
            | PgType::Geometry
            | PgType::TsVector
            | PgType::TsQuery
            | PgType::Vector => {
                quote! {}
            }
            // No need to create type aliases for arrays. Instead they'll be used as Vec<Inner>
            PgType::Array { .. } => quote! {},
            x => unimplemented!("Unhandled PgType {:?}", x),
        }
    }
}

impl ToRust for PgField {
    fn to_rust(&self, types: &BTreeMap<OID, PgType>, _config: &Config) -> TokenStream {
        self.to_rust_inner(types, true)
    }
}

/// Generate serde annotation for datetime types that need special serialization
fn generate_datetime_serde_attr(pg_type: &PgType, nullable: bool) -> Option<TokenStream> {
    match pg_type {
        PgType::Timestamptz => {
            if nullable {
                Some(quote! { #[serde(with = "time::serde::rfc3339::option")] })
            } else {
                Some(quote! { #[serde(with = "time::serde::rfc3339")] })
            }
        }
        PgType::Date => {
            if nullable {
                Some(quote! { #[serde(with = "date_serde::option")] })
            } else {
                Some(quote! { #[serde(with = "date_serde")] })
            }
        }
        _ => None,
    }
}

impl PgField {
    pub fn to_rust_inner(
        &self,
        types: &BTreeMap<OID, PgType>,
        include_pg_derive: bool,
    ) -> TokenStream {
        let ident = types.get(&self.type_oid).unwrap().to_rust_ident(types);
        let field_name = sql_to_rs_ident(&self.name, CaseType::Snake);
        let pg_name = &self.name;

        let comment_macro = match self.comment.as_ref() {
            Some(comment) => quote! { #[doc=#comment] },
            None => quote! {},
        };

        let pg_macro = if include_pg_derive {
            quote! { #[postgres(name = #pg_name)] }
        } else {
            quote! {}
        };
        let option_macro = if self.nullable {
            quote! { Option<#ident> }
        } else {
            quote! {#ident}
        };

        // Generate datetime serde annotation if needed
        let pg_type = types.get(&self.type_oid).unwrap();
        let datetime_serde_attr = generate_datetime_serde_attr(pg_type, self.nullable);

        // Only add serde rename if the Rust name differs from the PostgreSQL name
        let rs_name_str = sql_to_rs_string(&self.name, CaseType::Snake);
        let needs_rename = rs_name_str != self.name;

        let serde_attr = match (datetime_serde_attr, self.nullable, needs_rename) {
            (Some(attr), true, true) => quote! {
                #[serde(rename = #pg_name, default)]
                #attr
            },
            (Some(attr), true, false) => quote! {
                #[serde(default)]
                #attr
            },
            (Some(attr), false, true) => quote! {
                #[serde(rename = #pg_name)]
                #attr
            },
            (Some(attr), false, false) => quote! {
                #attr
            },
            (None, true, true) => quote! {
                #[serde(rename = #pg_name, default)]
            },
            (None, true, false) => quote! {
                #[serde(default)]
            },
            (None, false, true) => quote! {
                #[serde(rename = #pg_name)]
            },
            (None, false, false) => quote! {},
        };

        quote! {
            #comment_macro
            #pg_macro
            #serde_attr
            pub #field_name: #option_macro
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_domain_smart_derives() {
        use std::collections::HashMap;

        let mut types = BTreeMap::new();

        // Test int32 domain (should get Copy + ordering traits)
        types.insert(1, PgType::Int32);
        let int_domain = PgType::Domain {
            schema: "public".to_string(),
            name: "user_id".to_string(),
            type_oid: 1,
            constraints: vec![],
            comment: None,
        };

        let generated = int_domain.to_rust(&types, &Config::default());
        let code_str = generated.to_string();

        // Should have Copy trait for int types
        assert!(code_str.contains("Copy"));
        assert!(code_str.contains("PartialEq"));
        assert!(code_str.contains("Eq"));
        assert!(code_str.contains("Hash"));
        assert!(code_str.contains("PartialOrd"));
        assert!(code_str.contains("Ord"));

        // Test text domain (should get Display impl + ordering traits)
        types.insert(2, PgType::Text);
        let text_domain = PgType::Domain {
            schema: "public".to_string(),
            name: "user_name".to_string(),
            type_oid: 2,
            constraints: vec![],
            comment: None,
        };

        let generated = text_domain.to_rust(&types, &Config::default());
        let code_str = generated.to_string();

        // Should NOT have Copy trait for String types
        assert!(!code_str.contains("Copy"));
        // Should have Display impl
        assert!(code_str.contains("impl std :: fmt :: Display for UserName"));
        assert!(code_str.contains("self . 0 . fmt (f)"));
        assert!(code_str.contains("PartialEq"));
        assert!(code_str.contains("Eq"));
        assert!(code_str.contains("Hash"));
        assert!(code_str.contains("PartialOrd"));
        assert!(code_str.contains("Ord"));

        // Test numeric domain (should get ordering but not Copy)
        types.insert(3, PgType::Numeric);
        let numeric_domain = PgType::Domain {
            schema: "public".to_string(),
            name: "price".to_string(),
            type_oid: 3,
            constraints: vec![],
            comment: None,
        };

        let generated = numeric_domain.to_rust(&types, &Config::default());
        let code_str = generated.to_string();

        // Should NOT have Copy trait for Decimal types
        assert!(!code_str.contains("Copy"));
        // Should NOT have Display impl (only for text)
        assert!(!code_str.contains("impl std :: fmt :: Display for Price"));
        // Should have ordering traits
        assert!(code_str.contains("PartialEq"));
        assert!(code_str.contains("Eq"));
        assert!(code_str.contains("Hash"));
        assert!(code_str.contains("PartialOrd"));
        assert!(code_str.contains("Ord"));
    }
}
