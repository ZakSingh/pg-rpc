use crate::config::Config;
use crate::fn_index::FunctionIndex;
use crate::pg_fn::PgFn;
use crate::pg_type::PgType;
use crate::ty_index::TypeIndex;
use crate::rel_index::RelIndex;
use crate::unified_error;
use crate::exceptions::PgException;
use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::quote;
use std::collections::{HashMap, HashSet};

/// Postgres object ID
/// Uniquely identifies database objects
pub type OID = u32;

/// Postgres schema identifier
pub type SchemaName = String;

/// Postgres function identifier
pub type FunctionName = String;

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
    
    // First, we need to process the code to fix cross-schema references
    let type_def_code = codegen_types(&ty_index, config);
    let fn_code = codegen_fns(&fn_index, &ty_index, config, &all_exceptions);

    // Add error types to a common module
    let mut schema_code: HashMap<SchemaName, String> = type_def_code
        .into_iter()
        .chain(fn_code)
        .into_grouping_map()
        .fold(TokenStream::new(), |acc, _, ts| quote! { #acc #ts })
        .iter()
        .map(|(schema, tokens)| {
            // Convert tokens to string first to do replacements
            let tokens_str = tokens.to_string();
            
            // Replace cross-schema references from super:: to crate::
            // This is a bit hacky but works for now
            let fixed_tokens_str = tokens_str
                .replace("super ::", "crate::")
                .replace("super::", "crate::");
            
            // Parse back to tokens
            let fixed_tokens: TokenStream = fixed_tokens_str.parse().unwrap_or_else(|_| tokens.clone());
            
            let code = prettyplease::unparse(
                &syn::parse2::<syn::File>(quote! {
                    use postgres_types::private::BytesMut;
                    use postgres_types::{IsNull, ToSql, Type};
                    
                    #fixed_tokens
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
    for pg_fn in fn_index.values() {
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
    let schemas: HashMap<&str, Vec<&PgFn>> = fns.values().into_group_map_by(|f| f.schema.as_str());

    schemas
        .into_iter()
        .map(|(schema, fns)| {
            let fns_rs: Vec<TokenStream> =
                fns.into_iter().map(|f| f.to_rust(&types, config)).collect();
            (schema.to_string(), quote! { #(#fns_rs)* })
        })
        .collect()
}
