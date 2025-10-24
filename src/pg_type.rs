use crate::codegen::ToRust;
use crate::codegen::{SchemaName, OID};
use crate::config::Config;
use crate::ident::{sql_to_rs_ident, sql_to_rs_string, CaseType};
use crate::parse_domain::non_null_cols_from_checks;
// Temporarily inline flatten logic until module import is resolved
// use crate::flatten::{analyze_flatten_dependencies, FlattenedField};
use itertools::izip;
use postgres::Row;
use postgres_types::FromSql;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

/// Parse @pgrpc_not_null(col1, col2, ...) annotations from a comment
pub fn parse_bulk_not_null_columns(comment: &Option<String>) -> HashSet<String> {
    let mut columns = HashSet::new();
    if let Some(comment_text) = comment {
        // Match @pgrpc_not_null(col1, col2, col3) pattern
        let re = Regex::new(r"@pgrpc_not_null\(([^)]+)\)").unwrap();
        for cap in re.captures_iter(comment_text) {
            // Split the column list by comma and trim whitespace
            let cols_str = &cap[1];
            for col in cols_str.split(',') {
                let col = col.trim();
                if !col.is_empty() {
                    columns.insert(col.to_string());
                }
            }
        }
    }
    columns
}

// Temporarily inline flatten types until module import is resolved
#[derive(Debug, Clone)]
pub struct FlattenedField {
    pub name: String,
    pub original_path: Vec<String>,
    pub type_oid: OID,
    pub nullable: bool,
    pub comment: Option<String>,
}

#[derive(Debug)]
pub struct FlattenAnalysis {
    pub flattened_fields: Vec<FlattenedField>,
    pub _dependencies: HashSet<OID>,
}

#[derive(Debug)]
pub enum FlattenError {
    CyclicDependency(Vec<String>),
    InvalidFlattenTarget(String),
    TypeNotFound(OID),
}

/// Recursive flatten analysis for composite types
pub fn analyze_flatten_dependencies(
    composite_type: &PgType,
    types: &HashMap<OID, PgType>,
) -> Result<FlattenAnalysis, FlattenError> {
    let mut visited = HashSet::new();
    let mut path = Vec::new();
    let mut dependencies = HashSet::new();

    let flattened_fields = flatten_composite_recursive(
        composite_type,
        types,
        &mut visited,
        &mut path,
        &mut dependencies,
    )?;

    Ok(FlattenAnalysis {
        flattened_fields,
        _dependencies: dependencies,
    })
}

fn flatten_composite_recursive(
    composite_type: &PgType,
    types: &HashMap<OID, PgType>,
    visited: &mut HashSet<String>,
    path: &mut Vec<String>,
    dependencies: &mut HashSet<OID>,
) -> Result<Vec<FlattenedField>, FlattenError> {
    let type_name = match composite_type {
        PgType::Composite { name, .. } => name.clone(),
        _ => {
            return Err(FlattenError::InvalidFlattenTarget(
                "Only composite types can be flattened".to_string(),
            ))
        }
    };

    // Cycle detection
    if visited.contains(&type_name) {
        return Err(FlattenError::CyclicDependency(path.clone()));
    }

    visited.insert(type_name.clone());
    path.push(type_name.clone());

    let mut result = Vec::new();

    if let PgType::Composite { fields, .. } = composite_type {
        for field in fields {
            if field.flatten {
                // This field should be flattened
                let field_type = types
                    .get(&field.type_oid)
                    .ok_or(FlattenError::TypeNotFound(field.type_oid))?;

                dependencies.insert(field.type_oid);

                // Recursively flatten this field
                let sub_fields =
                    flatten_composite_recursive(field_type, types, visited, path, dependencies)?;

                // Add sub-fields with updated paths and names
                for sub_field in sub_fields {
                    let mut new_path = vec![field.name.clone()];
                    new_path.extend(sub_field.original_path);

                    // Generate field name with conflict resolution
                    let field_name = generate_field_name(&field.name, &sub_field.name);

                    result.push(FlattenedField {
                        name: field_name,
                        original_path: new_path,
                        type_oid: sub_field.type_oid,
                        nullable: field.nullable || sub_field.nullable,
                        comment: sub_field.comment,
                    });
                }
            } else {
                // This field should not be flattened, include as-is
                result.push(FlattenedField {
                    name: field.name.clone(),
                    original_path: vec![field.name.clone()],
                    type_oid: field.type_oid,
                    nullable: field.nullable,
                    comment: field.comment.clone(),
                });
            }
        }
    }

    path.pop();
    visited.remove(&type_name);

    Ok(result)
}

/// Generate a field name, handling potential conflicts
fn generate_field_name(parent_name: &str, field_name: &str) -> String {
    // For now, simple concatenation with underscore
    // TODO: Implement more sophisticated conflict resolution
    if parent_name.is_empty() {
        field_name.to_string()
    } else {
        format!("{}_{}", parent_name, field_name)
    }
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
}

#[derive(Debug)]
pub struct PgField {
    pub name: String,
    pub type_oid: OID,
    pub nullable: bool,
    pub comment: Option<String>,
    pub flatten: bool,
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
        }
    }

    pub fn to_rust_ident(&self, types: &HashMap<OID, PgType>) -> TokenStream {
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
            x => unimplemented!("unknown type {:?}", x),
        }
    }

    /// Get the Rust type identifier and collect referenced schemas
    pub fn to_rust_ident_with_schemas(
        &self,
        types: &HashMap<OID, PgType>,
    ) -> (TokenStream, HashSet<String>) {
        let mut schemas = HashSet::new();

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
                "float4" | "real" => PgType::Numeric, // float types map to numeric
                "float8" | "double precision" => PgType::Numeric,
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
                "tsvector" => PgType::Text,
                "tsquery" => PgType::Text,
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
                                .is_some_and(|c| c.contains("@pgrpc_not_null"));
                            
                            PgField {
                                name: field_name,
                                type_oid: ty,
                                nullable: nullable && !is_column_not_null && !is_bulk_not_null,
                                comment: comment.clone(),
                                flatten: comment
                                    .as_ref()
                                    .is_some_and(|c| c.contains("@pgrpc_flatten")),
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

    // Always add these useful traits for all domain types
    derives.push(quote! { PartialEq });
    derives.push(quote! { Eq });
    derives.push(quote! { Hash });

    match inner_type {
        // Copy types - add Copy and ordering traits
        PgType::Int16 | PgType::Int32 | PgType::Int64 | PgType::Bool => {
            derives.push(quote! { Copy });
            derives.push(quote! { PartialOrd });
            derives.push(quote! { Ord });
        }

        // String types - add ordering traits and custom Display impl
        PgType::Text => {
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
            derives.push(quote! { PartialOrd });
            derives.push(quote! { Ord });
        }

        // For all other types, just the basic traits (PartialEq, Eq, Hash) are sufficient
        _ => {}
    }

    (derives, impls)
}

impl ToRust for PgType {
    fn to_rust(&self, types: &HashMap<OID, PgType>, config: &Config) -> TokenStream {
        match self {
            PgType::Composite {
                name,
                schema,
                comment,
                fields,
                ..
            } => {
                let rs_name = sql_to_rs_ident(name, CaseType::Pascal);

                // Check if any fields need flattening
                let has_flattened_fields = fields.iter().any(|f| f.flatten);

                let (field_tokens, try_from_impl) = if has_flattened_fields {
                    // Use flatten analysis for complex field structure
                    match analyze_flatten_dependencies(self, types) {
                        Ok(analysis) => {
                            let field_tokens: Vec<TokenStream> = analysis
                                .flattened_fields
                                .iter()
                                .map(|f| generate_flattened_field_token(f, types))
                                .collect();

                            // Generate custom TryFrom implementation for flattened types
                            let try_from_impl = generate_flattened_try_from_impl(
                                &rs_name, &analysis, fields, types,
                            );

                            (field_tokens, try_from_impl)
                        }
                        Err(_) => {
                            // Fallback to regular field generation if flattening fails
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

                            (field_tokens, try_from_impl)
                        }
                    }
                } else {
                    // Regular field generation for non-flattened types
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

                    (field_tokens, try_from_impl)
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

                quote! {
                    #comment_macro
                    #[derive(Clone, Debug, bon::Builder, serde::Serialize, #deserialize_derive postgres_types::FromSql, postgres_types::ToSql)]
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
                                    flatten: f.flatten,
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

                        quote! {
                            #comment_macro
                            #[derive(Clone, Debug, bon::Builder, serde::Serialize, #deserialize_derive)]
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
            | PgType::Geometry => {
                quote! {}
            }
            // No need to create type aliases for arrays. Instead they'll be used as Vec<Inner>
            PgType::Array { .. } => quote! {},
            x => unimplemented!("Unhandled PgType {:?}", x),
        }
    }
}

impl ToRust for PgField {
    fn to_rust(&self, types: &HashMap<OID, PgType>, _config: &Config) -> TokenStream {
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
        types: &HashMap<OID, PgType>,
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
        let serde_attr = match datetime_serde_attr {
            Some(attr) => quote! {
                #[serde(rename = #pg_name)]
                #attr
            },
            None => quote! {
                #[serde(rename = #pg_name)]
            },
        };

        quote! {
            #comment_macro
            #pg_macro
            #serde_attr
            pub #field_name: #option_macro
        }
    }
}

/// Generate a Rust field token for a flattened field
fn generate_flattened_field_token(
    flattened_field: &FlattenedField,
    types: &HashMap<OID, PgType>,
) -> TokenStream {
    let field_name = sql_to_rs_ident(&flattened_field.name, CaseType::Snake);
    let type_ident = types
        .get(&flattened_field.type_oid)
        .unwrap()
        .to_rust_ident(types);

    let comment_macro = match flattened_field.comment.as_ref() {
        Some(comment) => quote! { #[doc=#comment] },
        None => quote! {},
    };

    // Use original field path for PostgreSQL name mapping - for serde JSON compatibility
    let pg_name = flattened_field.original_path.join(".");

    let option_macro = if flattened_field.nullable {
        quote! { Option<#type_ident> }
    } else {
        quote! { #type_ident }
    };

    // Generate datetime serde annotation if needed
    let pg_type = types.get(&flattened_field.type_oid).unwrap();
    let datetime_serde_attr = generate_datetime_serde_attr(pg_type, flattened_field.nullable);
    let serde_attr = match datetime_serde_attr {
        Some(attr) => quote! {
            #[serde(rename = #pg_name)]
            #attr
        },
        None => quote! {
            #[serde(rename = #pg_name)]
        },
    };

    quote! {
        #comment_macro
        #serde_attr
        pub #field_name: #option_macro
    }
}

/// Generate custom TryFrom implementation for flattened composite types
fn generate_flattened_try_from_impl(
    struct_name: &TokenStream,
    analysis: &FlattenAnalysis,
    original_fields: &[PgField],
    types: &HashMap<OID, PgType>,
) -> TokenStream {
    // Generate field extraction logic for each flattened field
    let field_extractions: Vec<TokenStream> = analysis
        .flattened_fields
        .iter()
        .map(|flattened_field| {
            let rs_field_name = sql_to_rs_ident(&flattened_field.name, CaseType::Snake);

            // Generate the extraction path from the original composite type
            let extraction_code =
                generate_field_extraction_code(flattened_field, original_fields, types);

            quote! {
                let #rs_field_name = #extraction_code;
            }
        })
        .collect();

    // Generate the struct construction
    let field_assignments: Vec<TokenStream> = analysis
        .flattened_fields
        .iter()
        .map(|flattened_field| {
            let rs_field_name = sql_to_rs_ident(&flattened_field.name, CaseType::Snake);
            quote! { #rs_field_name }
        })
        .collect();

    quote! {
        impl TryFrom<tokio_postgres::Row> for #struct_name {
            type Error = tokio_postgres::Error;

            fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
                #(#field_extractions)*

                Ok(Self {
                    #(#field_assignments),*
                })
            }
        }
    }
}

/// Generate field extraction code for a flattened field from the original composite type
fn generate_field_extraction_code(
    flattened_field: &FlattenedField,
    original_fields: &[PgField],
    types: &HashMap<OID, PgType>,
) -> TokenStream {
    if flattened_field.original_path.len() == 1 {
        // Simple field - direct access from row
        let field_name = &flattened_field.original_path[0];
        let rs_field_name = sql_to_rs_ident(&flattened_field.name, CaseType::Snake);
        quote! {
            row.try_get(#field_name)?
        }
    } else {
        // Nested field - need to extract from composite type
        let root_field_name = &flattened_field.original_path[0];
        let sub_field_name = &flattened_field.original_path[1];

        // Find the root field to get its type
        if let Some(root_field) = original_fields.iter().find(|f| f.name == *root_field_name) {
            if let Some(root_type) = types.get(&root_field.type_oid) {
                // Get the Rust type name for the composite type
                // When referencing types in the same module, use simple name
                let composite_type_name = match root_type {
                    PgType::Composite { name, .. } => sql_to_rs_ident(&name, CaseType::Pascal),
                    _ => root_type.to_rust_ident(types),
                };
                let sub_field_rust_name = sql_to_rs_ident(sub_field_name, CaseType::Snake);

                // Now that composite types have FromSql, we can extract them properly
                // Generate nested field access through the composite type
                let rs_field_name = sql_to_rs_ident(&flattened_field.name, CaseType::Snake);
                quote! {
                    row.try_get::<_, Option<#composite_type_name>>(#root_field_name)?
                        .and_then(|c| c.#sub_field_rust_name.clone())
                }
            } else {
                // Fallback if type not found
                if flattened_field.nullable {
                    quote! { None }
                } else {
                    quote! { Default::default() }
                }
            }
        } else {
            // Fallback if field not found
            if flattened_field.nullable {
                quote! { None }
            } else {
                quote! { Default::default() }
            }
        }
    }
}

/// Generate PostgreSQL composite field expansion expression from field path
/// Examples:
/// - ["name"] -> "name"
/// - ["addr", "street"] -> "(addr).street"
/// - ["addr", "contact", "phone"] -> "((addr).contact).phone"
fn generate_composite_field_expression(path: &[String]) -> String {
    if path.len() == 1 {
        // Simple field access
        path[0].clone()
    } else if path.len() == 2 {
        // Single-level composite field access: (composite_field).sub_field
        format!("({}).{}", path[0], path[1])
    } else {
        // Multi-level composite field access: ((composite_field).sub_composite).final_field
        let mut expression = format!("({})", path[0]);
        for field in &path[1..path.len() - 1] {
            expression = format!("({}.{})", expression, field);
        }
        format!("{}.{}", expression, path.last().unwrap())
    }
}

/// Generate the SELECT clause for a composite type with flattened fields
/// This creates the SQL that expands composite types using PostgreSQL's (column).* syntax
pub fn generate_select_clause_for_flattened_type(
    analysis: &FlattenAnalysis,
    table_alias: Option<&str>,
) -> String {
    let table_prefix = table_alias
        .map(|alias| format!("{}.", alias))
        .unwrap_or_default();

    let field_expressions: Vec<String> = analysis
        .flattened_fields
        .iter()
        .map(|field| {
            let sql_expr = generate_composite_field_expression(&field.original_path);
            // Add table alias if provided
            if let Some(alias) = table_alias {
                format!("{}{} AS {}", table_prefix, sql_expr, field.name)
            } else {
                format!("{} AS {}", sql_expr, field.name)
            }
        })
        .collect();

    field_expressions.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_field(name: &str, type_oid: OID, flatten: bool) -> PgField {
        PgField {
            name: name.to_string(),
            type_oid,
            nullable: true,
            comment: None,
            flatten,
        }
    }

    fn create_test_composite(name: &str, fields: Vec<PgField>) -> PgType {
        PgType::Composite {
            schema: "public".to_string(),
            name: name.to_string(),
            fields,
            comment: None,
            relkind: None,
            view_definition: None,
        }
    }

    #[test]
    fn test_flatten_analysis_simple() {
        let mut types = HashMap::new();

        // Create address type
        let address_type = create_test_composite(
            "address",
            vec![
                create_test_field("street", 1, false),
                create_test_field("city", 2, false),
            ],
        );
        types.insert(100, address_type);

        // Create person type with flattened address
        let person_type = create_test_composite(
            "person",
            vec![
                create_test_field("name", 3, false),
                create_test_field("addr", 100, true), // Flatten this
            ],
        );

        let analysis = analyze_flatten_dependencies(&person_type, &types).unwrap();

        assert_eq!(analysis.flattened_fields.len(), 3);
        assert_eq!(analysis.flattened_fields[0].name, "name");
        assert_eq!(analysis.flattened_fields[1].name, "addr_street");
        assert_eq!(analysis.flattened_fields[2].name, "addr_city");

        // Check original paths
        assert_eq!(analysis.flattened_fields[0].original_path, vec!["name"]);
        assert_eq!(
            analysis.flattened_fields[1].original_path,
            vec!["addr", "street"]
        );
        assert_eq!(
            analysis.flattened_fields[2].original_path,
            vec!["addr", "city"]
        );
    }

    #[test]
    fn test_flatten_analysis_no_flatten() {
        let mut types = HashMap::new();

        // Create simple person type without any flattening
        let person_type = create_test_composite(
            "person",
            vec![
                create_test_field("name", 1, false),
                create_test_field("age", 2, false),
            ],
        );

        let analysis = analyze_flatten_dependencies(&person_type, &types).unwrap();

        assert_eq!(analysis.flattened_fields.len(), 2);
        assert_eq!(analysis.flattened_fields[0].name, "name");
        assert_eq!(analysis.flattened_fields[1].name, "age");

        // All fields should have simple original paths
        assert_eq!(analysis.flattened_fields[0].original_path, vec!["name"]);
        assert_eq!(analysis.flattened_fields[1].original_path, vec!["age"]);
    }

    #[test]
    fn test_flatten_analysis_error_cases() {
        let types = HashMap::new();

        // Try to flatten a field that references a non-existent type
        let person_type = create_test_composite(
            "person",
            vec![
                create_test_field("name", 1, false),
                create_test_field("addr", 999, true), // Type 999 doesn't exist
            ],
        );

        let result = analyze_flatten_dependencies(&person_type, &types);
        assert!(result.is_err());

        // Try to flatten a non-composite type
        let mut types = HashMap::new();
        types.insert(100, PgType::Text); // Text is not a composite type

        let person_type = create_test_composite(
            "person",
            vec![
                create_test_field("name", 1, false),
                create_test_field("description", 100, true), // Try to flatten text
            ],
        );

        let result = analyze_flatten_dependencies(&person_type, &types);
        assert!(result.is_err());
    }

    #[test]
    fn test_composite_field_expression_generation() {
        // Test simple field access
        assert_eq!(
            generate_composite_field_expression(&["name".to_string()]),
            "name"
        );

        // Test single-level composite field access
        assert_eq!(
            generate_composite_field_expression(&["addr".to_string(), "street".to_string()]),
            "(addr).street"
        );

        // Test multi-level composite field access
        assert_eq!(
            generate_composite_field_expression(&[
                "addr".to_string(),
                "contact".to_string(),
                "phone".to_string()
            ]),
            "((addr).contact).phone"
        );

        // Test deeper nesting
        assert_eq!(
            generate_composite_field_expression(&[
                "person".to_string(),
                "addr".to_string(),
                "contact".to_string(),
                "emergency".to_string(),
                "phone".to_string()
            ]),
            "((((person).addr).contact).emergency).phone"
        );
    }

    #[test]
    fn test_select_clause_generation() {
        let mut types = HashMap::new();

        // Create address type
        let address_type = create_test_composite(
            "address",
            vec![
                create_test_field("street", 1, false),
                create_test_field("city", 2, false),
            ],
        );
        types.insert(100, address_type);

        // Create person type with flattened address
        let person_type = create_test_composite(
            "person",
            vec![
                create_test_field("name", 3, false),
                create_test_field("addr", 100, true), // Flatten this
            ],
        );

        let analysis = analyze_flatten_dependencies(&person_type, &types).unwrap();

        // Test without table alias
        let select_clause = generate_select_clause_for_flattened_type(&analysis, None);
        assert_eq!(
            select_clause,
            "name AS name, (addr).street AS addr_street, (addr).city AS addr_city"
        );

        // Test with table alias
        let select_clause_with_alias =
            generate_select_clause_for_flattened_type(&analysis, Some("p"));
        assert_eq!(
            select_clause_with_alias,
            "p.name AS name, p.(addr).street AS addr_street, p.(addr).city AS addr_city"
        );
    }

    #[test]
    fn test_nested_select_clause_generation() {
        let mut types = HashMap::new();

        // Create contact_info type
        let contact_type = create_test_composite(
            "contact_info",
            vec![
                create_test_field("phone", 1, false),
                create_test_field("email", 2, false),
            ],
        );
        types.insert(200, contact_type);

        // Create address type with flattened contact
        let address_type = create_test_composite(
            "address",
            vec![
                create_test_field("street", 3, false),
                create_test_field("city", 4, false),
                create_test_field("contact", 200, true), // Flatten this
            ],
        );
        types.insert(100, address_type);

        // Create person type with flattened address (which contains flattened contact)
        let person_type = create_test_composite(
            "person",
            vec![
                create_test_field("name", 5, false),
                create_test_field("addr", 100, true), // Flatten this
            ],
        );

        let analysis = analyze_flatten_dependencies(&person_type, &types).unwrap();

        let select_clause = generate_select_clause_for_flattened_type(&analysis, Some("t"));

        // Should generate:
        // t.name AS name,
        // t.(addr).street AS addr_street,
        // t.(addr).city AS addr_city,
        // t.((addr).contact).phone AS addr_contact_phone,
        // t.((addr).contact).email AS addr_contact_email
        assert!(select_clause.contains("t.name AS name"));
        assert!(select_clause.contains("t.(addr).street AS addr_street"));
        assert!(select_clause.contains("t.(addr).city AS addr_city"));
        assert!(select_clause.contains("t.((addr).contact).phone AS addr_contact_phone"));
        assert!(select_clause.contains("t.((addr).contact).email AS addr_contact_email"));
    }

    #[test]
    fn test_flattened_try_from_generation() {
        let mut types = HashMap::new();

        // Create address type
        let address_type = create_test_composite(
            "address",
            vec![
                create_test_field("street", 1, false),
                create_test_field("city", 1, false),
            ],
        );
        types.insert(100, address_type);

        // Add text type
        types.insert(1, PgType::Text);

        // Create person type with flattened address
        let person_type = create_test_composite(
            "person",
            vec![
                create_test_field("name", 1, false),
                create_test_field("addr", 100, true), // Flatten this
            ],
        );

        let config = Config {
            connection_string: None,
            output_path: None,
            schemas: vec![],
            types: HashMap::new(),
            exceptions: HashMap::new(),
            task_queue: None,
            errors: None,
            infer_view_nullability: true,
            disable_deserialize: Vec::new(),
            queries: None,
        };

        // Generate Rust code
        let generated = person_type.to_rust(&types, &config);
        let generated_str = generated.to_string();

        println!("Generated TryFrom code:\n{}", generated_str);

        // Verify that the generated code contains expected elements
        assert!(generated_str.contains("pub struct Person"));
        assert!(generated_str.contains("name : Option < String >"));
        assert!(generated_str.contains("addr_street : Option < String >"));
        assert!(generated_str.contains("addr_city : Option < String >"));
        assert!(generated_str.contains("impl TryFrom < tokio_postgres :: Row > for Person"));
        assert!(generated_str.contains("try_get (\"name\")"));
        assert!(generated_str.contains("try_get :: < _ , Option < Address >> (\"addr\")"));

        // Verify the flattened field extraction logic
        assert!(generated_str.contains("let addr_street"));
        assert!(generated_str.contains("let addr_city"));
        assert!(generated_str.contains(". and_then (| c | c . street"));
        assert!(generated_str.contains(". and_then (| c | c . city"));
    }

    #[test]
    fn test_domain_smart_derives() {
        use std::collections::HashMap;

        let mut types = HashMap::new();

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
