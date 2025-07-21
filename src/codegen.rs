use crate::config::Config;
use crate::fn_index::FunctionIndex;
use crate::pg_fn::PgFn;
use crate::pg_type::PgType;
use crate::ty_index::TypeIndex;
use crate::rel_index::RelIndex;
use crate::unified_error;
use crate::exceptions::PgException;
/// Postgres object ID
/// Uniquely identifies database objects
pub type OID = u32;

/// Postgres schema identifier
pub type SchemaName = String;

/// Postgres function identifier
pub type FunctionName = String;
use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::quote;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;

pub trait ToRust {
    fn to_rust(&self, types: &HashMap<OID, PgType>, config: &Config) -> TokenStream;
}

/// Generate code split by schema
pub fn codegen_split(
    fn_index: &FunctionIndex,
    ty_index: &TypeIndex,
    rel_index: &RelIndex,
    config: &Config,
) -> anyhow::Result<HashMap<SchemaName, String>> {
    let warning_ignores = "#![allow(dead_code)]\n#![allow(unused_variables)]\n#![allow(unused_imports)]\n#![allow(unused_mut)]\n\n";
    
    // Generate unified error types
    let (error_types_code, all_exceptions) = generate_unified_errors(fn_index, rel_index, config);
    
    // Generate types and collect referenced schemas
    let (type_def_code, type_schema_refs) = codegen_types_with_refs(&ty_index, config);
    let (fn_code, fn_schema_refs) = codegen_fns_with_refs(&fn_index, &ty_index, config, &all_exceptions);

    // Merge schema references
    let mut all_schema_refs: HashMap<SchemaName, HashSet<SchemaName>> = HashMap::new();
    for (schema, refs) in type_schema_refs {
        all_schema_refs.entry(schema).or_default().extend(refs);
    }
    for (schema, refs) in fn_schema_refs {
        all_schema_refs.entry(schema).or_default().extend(refs);
    }

    // Add error types to a common module
    let mut schema_code: HashMap<SchemaName, String> = type_def_code
        .into_iter()
        .chain(fn_code)
        .into_grouping_map()
        .fold(TokenStream::new(), |acc, _, ts| quote! { #acc #ts })
        .iter()
        .map(|(schema, tokens)| {
            // Get referenced schemas for this schema
            let referenced_schemas = all_schema_refs.get(schema).cloned().unwrap_or_default();
            
            // Generate use statements for referenced schemas
            let schema_imports: Vec<TokenStream> = referenced_schemas
                .into_iter()
                .filter(|s| s != schema) // Don't import self
                .map(|s| {
                    let schema_ident = quote::format_ident!("{}", s);
                    quote! { use super::#schema_ident; }
                })
                .collect();
            
            let code = prettyplease::unparse(
                &syn::parse2::<syn::File>(quote! {
                    #(#schema_imports)*
                    
                    use postgres_types::private::BytesMut;
                    use postgres_types::{IsNull, ToSql, Type};
                    use rust_decimal::Decimal;
                    use bon::builder;
                    
                    #tokens
                })
                .expect("generated code to parse"),
            );
            (schema.clone(), warning_ignores.to_string() + &code)
        })
        .collect();
    

    // Add the error types as a separate module
    schema_code.insert(
        "errors".to_string(),
        warning_ignores.to_string() + &prettyplease::unparse(&syn::parse2(error_types_code).expect("error types to parse"))
    );

    Ok(schema_code)
}


fn codegen_types(type_index: &TypeIndex, config: &Config) -> HashMap<SchemaName, TokenStream> {
    type_index
        .deref()
        .values()
        .map(|t| (t.schema(), t.to_rust(type_index, config)))
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

/// Generate type definitions and collect referenced schemas
fn codegen_types_with_refs(type_index: &TypeIndex, config: &Config) -> (HashMap<SchemaName, TokenStream>, HashMap<SchemaName, HashSet<SchemaName>>) {
    let mut schema_refs: HashMap<SchemaName, HashSet<SchemaName>> = HashMap::new();
    
    let type_code: HashMap<SchemaName, Vec<(TokenStream, HashSet<String>)>> = type_index
        .deref()
        .values()
        .map(|t| {
            let schema = t.schema();
            let tokens = t.to_rust(type_index, config);
            let refs = collect_type_refs(t, type_index);
            (schema, (tokens, refs))
        })
        .filter(|(_, (tokens, _))| !tokens.is_empty())
        .into_group_map();
    
    let result: HashMap<SchemaName, TokenStream> = type_code
        .into_iter()
        .map(|(schema, items)| {
            let mut all_refs = HashSet::new();
            let token_streams: Vec<TokenStream> = items
                .into_iter()
                .map(|(tokens, refs)| {
                    all_refs.extend(refs);
                    tokens
                })
                .collect();
            
            schema_refs.insert(schema.clone(), all_refs);
            
            (
                schema,
                quote! {
                    #(#token_streams)*
                },
            )
        })
        .collect();
    
    (result, schema_refs)
}

/// Collect all schema references for a type
fn collect_type_refs(pg_type: &PgType, type_index: &TypeIndex) -> HashSet<String> {
    let mut refs = HashSet::new();
    
    match pg_type {
        PgType::Composite { fields, .. } => {
            for field in fields {
                if let Some(field_type) = type_index.get(&field.type_oid) {
                    if let Some(schema) = get_type_schema(field_type) {
                        refs.insert(schema);
                    }
                    refs.extend(collect_type_refs(field_type, type_index));
                }
            }
        }
        PgType::Domain { type_oid, .. } => {
            if let Some(base_type) = type_index.get(type_oid) {
                if let Some(schema) = get_type_schema(base_type) {
                    refs.insert(schema);
                }
                refs.extend(collect_type_refs(base_type, type_index));
            }
        }
        PgType::Array { element_type_oid, .. } => {
            if let Some(elem_type) = type_index.get(element_type_oid) {
                if let Some(schema) = get_type_schema(elem_type) {
                    refs.insert(schema);
                }
                refs.extend(collect_type_refs(elem_type, type_index));
            }
        }
        _ => {}
    }
    
    refs
}

/// Get the schema of a type if it's a user-defined type
fn get_type_schema(pg_type: &PgType) -> Option<String> {
    match pg_type {
        PgType::Domain { schema, .. } |
        PgType::Composite { schema, .. } |
        PgType::Enum { schema, .. } => Some(schema.clone()),
        _ => None,
    }
}

/// Generate unified error types for all functions
fn generate_unified_errors(
    fn_index: &FunctionIndex,
    rel_index: &RelIndex,
    config: &Config,
) -> (TokenStream, HashSet<PgException>) {
    // Collect all constraints from tables
    let table_constraints = unified_error::collect_table_constraints(rel_index);
    
    // Collect all custom exceptions from functions
    let mut all_exceptions = HashSet::new();
    for pg_fn in fn_index.deref().values() {
        all_exceptions.extend(pg_fn.exceptions.iter().cloned());
    }
    
    // Generate constraint enums
    let constraint_enums = unified_error::generate_constraint_enums(&table_constraints);
    
    // Generate the unified error type
    let error_enum = unified_error::generate_unified_error(&table_constraints, &all_exceptions, config);
    
    let error_types_code = quote! {
        #(#constraint_enums)*
        
        #error_enum
    };
    
    (error_types_code, all_exceptions)
}

/// Returns map of schema to token stream containing all fn definitions for that schema
fn codegen_fns(
    fns: &FunctionIndex,
    types: &TypeIndex,
    config: &Config,
    _all_exceptions: &HashSet<PgException>,
) -> HashMap<SchemaName, TokenStream> {
    let schemas: HashMap<&str, Vec<&PgFn>> = fns.deref().values().into_group_map_by(|f| f.schema.as_str());

    schemas
        .into_iter()
        .map(|(schema, fns)| {
            let fns_rs: Vec<TokenStream> =
                fns.into_iter().map(|f| f.to_rust(&types, config)).collect();
            (schema.to_string(), quote! { #(#fns_rs)* })
        })
        .collect()
}

/// Generate function definitions and collect referenced schemas
fn codegen_fns_with_refs(
    fns: &FunctionIndex,
    types: &TypeIndex,
    config: &Config,
    _all_exceptions: &HashSet<PgException>,
) -> (HashMap<SchemaName, TokenStream>, HashMap<SchemaName, HashSet<SchemaName>>) {
    let mut schema_refs: HashMap<SchemaName, HashSet<SchemaName>> = HashMap::new();
    
    let schemas: HashMap<&str, Vec<&PgFn>> = fns.deref().values().into_group_map_by(|f| f.schema.as_str());

    let result: HashMap<SchemaName, TokenStream> = schemas
        .into_iter()
        .map(|(schema, fns)| {
            let mut all_refs = HashSet::new();
            
            // Collect references from all functions in this schema
            for pg_fn in &fns {
                all_refs.extend(collect_fn_refs(pg_fn, types));
            }
            
            // All functions reference the errors module
            all_refs.insert("errors".to_string());
            
            let fns_rs: Vec<TokenStream> =
                fns.into_iter().map(|f| f.to_rust(&types, config)).collect();
            
            schema_refs.insert(schema.to_string(), all_refs);
            
            (schema.to_string(), quote! { #(#fns_rs)* })
        })
        .collect();
    
    (result, schema_refs)
}

/// Collect all schema references for a function
fn collect_fn_refs(pg_fn: &PgFn, types: &TypeIndex) -> HashSet<String> {
    let mut refs = HashSet::new();
    
    // Collect from return type
    if let Some(ret_type) = types.get(&pg_fn.return_type_oid) {
        if let Some(schema) = get_type_schema(ret_type) {
            refs.insert(schema);
        }
        refs.extend(collect_type_refs(ret_type, types));
    }
    
    // Collect from arguments
    for arg in &pg_fn.args {
        if let Some(arg_type) = types.get(&arg.type_oid) {
            if let Some(schema) = get_type_schema(arg_type) {
                refs.insert(schema);
            }
            refs.extend(collect_type_refs(arg_type, types));
        }
    }
    
    refs
}

