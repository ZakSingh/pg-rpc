use crate::config::Config;
use crate::exceptions::PgException;
use crate::fn_index::FunctionIndex;
use crate::pg_fn::{param_needs_reference, PgFn};
use crate::pg_type::PgType;
use crate::rel_index::RelIndex;
use crate::ty_index::TypeIndex;
use crate::unified_error;
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

    // Collect table constraints for function error generation
    let table_constraints = unified_error::collect_table_constraints(rel_index);

    // Generate types and collect referenced schemas
    let (type_def_code, type_schema_refs) = codegen_types_with_refs(&ty_index, config);
    let (fn_code, fn_schema_refs) = codegen_fns_with_refs(
        &fn_index,
        &ty_index,
        &rel_index,
        &table_constraints,
        config,
        &all_exceptions,
    );

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
                    use postgis_butmaintained::ewkb;

                    /// Custom serde module for time::Date using YYYY-MM-DD format
                    pub mod date_serde {
                        use serde::{self, Deserialize, Deserializer, Serializer};
                        use time::Date;

                        const FORMAT: &[time::format_description::FormatItem] = time::macros::format_description!("[year]-[month]-[day]");

                        pub fn serialize<S>(date: &Date, serializer: S) -> Result<S::Ok, S::Error>
                        where
                            S: Serializer,
                        {
                            let s = date.format(&FORMAT).map_err(serde::ser::Error::custom)?;
                            serializer.serialize_str(&s)
                        }

                        pub fn deserialize<'de, D>(deserializer: D) -> Result<Date, D::Error>
                        where
                            D: Deserializer<'de>,
                        {
                            let s = String::deserialize(deserializer)?;
                            Date::parse(&s, &FORMAT).map_err(serde::de::Error::custom)
                        }

                        pub mod option {
                            use serde::{Deserialize, Deserializer, Serializer};
                            use time::Date;

                            const FORMAT: &[time::format_description::FormatItem] = time::macros::format_description!("[year]-[month]-[day]");

                            pub fn serialize<S>(date: &Option<Date>, serializer: S) -> Result<S::Ok, S::Error>
                            where
                                S: Serializer,
                            {
                                match date {
                                    Some(d) => {
                                        let s = d.format(&FORMAT).map_err(serde::ser::Error::custom)?;
                                        serializer.serialize_some(&s)
                                    }
                                    None => serializer.serialize_none(),
                                }
                            }

                            pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Date>, D::Error>
                            where
                                D: Deserializer<'de>,
                            {
                                let opt = Option::<String>::deserialize(deserializer)?;
                                opt.map(|s| Date::parse(&s, &FORMAT).map_err(serde::de::Error::custom)).transpose()
                            }
                        }
                    }

                    #tokens
                })
                .expect("generated code to parse"),
            );
            (schema.clone(), warning_ignores.to_string() + &code)
        })
        .collect();

    // Add the error types as a separate module
    let error_module_code = prettyplease::unparse(
        &syn::parse2::<syn::File>(error_types_code).expect("error module to parse"),
    );
    schema_code.insert(
        "errors".to_string(),
        warning_ignores.to_string() + &error_module_code,
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
fn codegen_types_with_refs(
    type_index: &TypeIndex,
    config: &Config,
) -> (
    HashMap<SchemaName, TokenStream>,
    HashMap<SchemaName, HashSet<SchemaName>>,
) {
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
        PgType::Array {
            element_type_oid, ..
        } => {
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
        PgType::Domain { schema, .. }
        | PgType::Composite { schema, .. }
        | PgType::Enum { schema, .. } => Some(schema.clone()),
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
    let error_enum =
        unified_error::generate_unified_error(&table_constraints, &all_exceptions, config);

    let error_types_code = quote! {
        /// Trait for types that can provide access to an underlying DbError
        pub trait AsDbError {
            /// Returns a reference to the underlying DbError if this error contains one
            fn as_db_error(&self) -> Option<&tokio_postgres::error::DbError>;
        }

        /// Extension trait for error types that provides helper methods for PostgreSQL errors
        pub trait PgRpcErrorExt: AsDbError {
            /// Returns the column name from the underlying PostgreSQL error, if available
            fn column(&self) -> Option<&str> {
                self.as_db_error().and_then(|e| e.column())
            }

            /// Returns the constraint name from the underlying PostgreSQL error, if available
            fn constraint(&self) -> Option<&str> {
                self.as_db_error().and_then(|e| e.constraint())
            }

            /// Returns the table name from the underlying PostgreSQL error, if available
            fn table(&self) -> Option<&str> {
                self.as_db_error().and_then(|e| e.table())
            }

            /// Returns the error message from the underlying PostgreSQL error, if available
            fn message(&self) -> Option<&str> {
                self.as_db_error().map(|e| e.message())
            }

            /// Returns the SQL state code from the underlying PostgreSQL error, if available
            fn code(&self) -> Option<&tokio_postgres::error::SqlState> {
                self.as_db_error().map(|e| e.code())
            }

            /// Returns the schema name from the underlying PostgreSQL error, if available
            fn schema(&self) -> Option<&str> {
                self.as_db_error().and_then(|e| e.schema())
            }

            /// Returns the data type name from the underlying PostgreSQL error, if available
            fn datatype(&self) -> Option<&str> {
                self.as_db_error().and_then(|e| e.datatype())
            }

            /// Returns the detail from the underlying PostgreSQL error, if available
            fn detail(&self) -> Option<&str> {
                self.as_db_error().and_then(|e| e.detail())
            }

            /// Returns the hint from the underlying PostgreSQL error, if available
            fn hint(&self) -> Option<&str> {
                self.as_db_error().and_then(|e| e.hint())
            }

            /// Returns the position from the underlying PostgreSQL error, if available
            fn position(&self) -> Option<&tokio_postgres::error::ErrorPosition> {
                self.as_db_error().and_then(|e| e.position())
            }

            /// Returns the where clause from the underlying PostgreSQL error, if available
            fn where_(&self) -> Option<&str> {
                self.as_db_error().and_then(|e| e.where_())
            }

            /// Returns the routine from the underlying PostgreSQL error, if available
            fn routine(&self) -> Option<&str> {
                self.as_db_error().and_then(|e| e.routine())
            }
        }

        /// Blanket implementation of PgRpcErrorExt for all types that implement AsDbError
        impl<T: AsDbError> PgRpcErrorExt for T {}

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
    let schemas: HashMap<&str, Vec<&PgFn>> = fns
        .deref()
        .values()
        .into_group_map_by(|f| f.schema.as_str());

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
    rel_index: &RelIndex,
    table_constraints: &HashMap<crate::pg_id::PgId, Vec<crate::pg_constraint::Constraint>>,
    config: &Config,
    _all_exceptions: &HashSet<PgException>,
) -> (
    HashMap<SchemaName, TokenStream>,
    HashMap<SchemaName, HashSet<SchemaName>>,
) {
    let mut schema_refs: HashMap<SchemaName, HashSet<SchemaName>> = HashMap::new();

    let schemas: HashMap<&str, Vec<&PgFn>> = fns
        .deref()
        .values()
        .into_group_map_by(|f| f.schema.as_str());

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

            let fns_rs: Vec<TokenStream> = fns
                .into_iter()
                .map(|f| {
                    // Generate error enum for this function
                    let error_enum = f.generate_error_enum(rel_index, table_constraints, config);
                    let fn_impl = f.to_rust(&types, config);
                    quote! {
                        #error_enum

                        #fn_impl
                    }
                })
                .collect();

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

/// Generate code for SQL query files
pub fn codegen_queries(
    query_index: &crate::query_index::QueryIndex,
    type_index: &TypeIndex,
    rel_index: &RelIndex,
    config: &Config,
) -> TokenStream {
    use crate::query_introspector::IntrospectedQuery;

    // Collect table constraints for error generation
    let table_constraints = unified_error::collect_table_constraints(rel_index);

    let queries: Vec<&IntrospectedQuery> = query_index.values().collect();

    let query_code: Vec<TokenStream> = queries
        .iter()
        .map(|query| generate_query_code(query, type_index, &table_constraints, config))
        .collect();

    quote! {
        #(#query_code)*
    }
}

/// Generate code for a single query
fn generate_query_code(
    query: &crate::query_introspector::IntrospectedQuery,
    type_index: &TypeIndex,
    table_constraints: &std::collections::HashMap<crate::pg_id::PgId, Vec<crate::pg_constraint::Constraint>>,
    config: &Config,
) -> TokenStream {
    use crate::ident::{sql_to_rs_ident, CaseType};

    let fn_name = sql_to_rs_ident(&query.name, CaseType::Snake);
    let sql = &query.sql;

    // Generate error enum for this query
    let error_enum = generate_query_error_enum(query, table_constraints, config);
    let error_enum_name = quote::format_ident!("{}Error", sql_to_rs_ident(&query.name, CaseType::Pascal).to_string());

    // Generate parameter list
    let params: Vec<TokenStream> = query
        .params
        .iter()
        .map(|param| {
            let param_name = sql_to_rs_ident(&param.name, CaseType::Snake);
            let param_type = get_param_type(param.type_oid, type_index, param.nullable);
            quote! { #param_name: #param_type }
        })
        .collect();

    // Generate return type and query execution code
    let (return_type, query_execution, row_struct) = match &query.query_type {
        crate::sql_parser::QueryType::One => {
            if let Some(columns) = &query.return_columns {
                let struct_name_str = format!("{}Row", sql_to_rs_ident(&query.name, CaseType::Pascal).to_string());
                let struct_name = quote::format_ident!("{}", struct_name_str);
                let row_struct = generate_row_struct(&struct_name, columns, type_index);

                let return_type = quote! { Result<#struct_name, #error_enum_name> };
                let execution = quote! {
                    client.query_opt(query, &params).await
                        .and_then(|opt_row| {
                            let row = opt_row.expect("query returned no rows");
                            row.try_into()
                        })
                        .map_err(#error_enum_name::from)
                };

                (return_type, execution, row_struct)
            } else {
                // Should not happen for :one queries
                let return_type = quote! { Result<(), #error_enum_name> };
                let execution = quote! {
                    client.execute(query, &params).await?;
                    Ok(())
                };
                (return_type, execution, quote! {})
            }
        }
        crate::sql_parser::QueryType::Opt => {
            if let Some(columns) = &query.return_columns {
                let struct_name_str = format!("{}Row", sql_to_rs_ident(&query.name, CaseType::Pascal).to_string());
                let struct_name = quote::format_ident!("{}", struct_name_str);
                let row_struct = generate_row_struct(&struct_name, columns, type_index);

                let return_type = quote! { Result<Option<#struct_name>, #error_enum_name> };
                let execution = quote! {
                    client.query_opt(query, &params).await
                        .and_then(|row| match row {
                            Some(row) => Ok(Some(row.try_into()?)),
                            None => Ok(None),
                        })
                        .map_err(#error_enum_name::from)
                };

                (return_type, execution, row_struct)
            } else {
                let return_type = quote! { Result<Option<()>, #error_enum_name> };
                let execution = quote! {
                    client.execute(query, &params).await?;
                    Ok(Some(()))
                };
                (return_type, execution, quote! {})
            }
        }
        crate::sql_parser::QueryType::Many => {
            if let Some(columns) = &query.return_columns {
                let struct_name_str = format!("{}Row", sql_to_rs_ident(&query.name, CaseType::Pascal).to_string());
                let struct_name = quote::format_ident!("{}", struct_name_str);
                let row_struct = generate_row_struct(&struct_name, columns, type_index);

                let return_type = quote! { Result<Vec<#struct_name>, #error_enum_name> };
                let execution = quote! {
                    client.query(query, &params).await
                        .and_then(|rows| rows.into_iter().map(|row| row.try_into()).collect())
                        .map_err(#error_enum_name::from)
                };

                (return_type, execution, row_struct)
            } else {
                // Should not happen for :many queries
                let return_type = quote! { Result<Vec<()>, #error_enum_name> };
                let execution = quote! {
                    let rows = client.query(query, &params).await?;
                    Ok(vec![(); rows.len()])
                };
                (return_type, execution, quote! {})
            }
        }
        crate::sql_parser::QueryType::Exec => {
            let return_type = quote! { Result<(), #error_enum_name> };
            let execution = quote! {
                client.execute(query, &params).await?;
                Ok(())
            };
            (return_type, execution, quote! {})
        }
        crate::sql_parser::QueryType::ExecRows => {
            let return_type = quote! { Result<u64, #error_enum_name> };
            let execution = quote! {
                let rows_affected = client.execute(query, &params).await?;
                Ok(rows_affected)
            };
            (return_type, execution, quote! {})
        }
    };

    // Build parameter references for query call
    let param_refs: Vec<TokenStream> = query
        .params
        .iter()
        .map(|param| {
            let param_name = sql_to_rs_ident(&param.name, CaseType::Snake);

            if param.nullable {
                // For nullable parameters, Option<T> implements ToSql, so always use reference
                // This handles both Option<&T> and Option<T>
                quote! { &#param_name }
            } else {
                // For non-nullable, check if we need to add a reference
                if param_needs_reference(param.type_oid, type_index) {
                    quote! { &#param_name }
                } else {
                    quote! { #param_name }
                }
            }
        })
        .collect();

    // Generate doc comment with SQL
    let doc_comment = format!("Query: {}\n\nSQL:\n```sql\n{}\n```", query.name, sql);

    quote! {
        #row_struct

        #error_enum

        #[doc = #doc_comment]
        pub async fn #fn_name(
            client: &impl deadpool_postgres::GenericClient,
            #(#params),*
        ) -> #return_type {
            let query = #sql;
            let params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![#(#param_refs),*];

            #query_execution
        }
    }
}

/// Generate error enum for a single query
fn generate_query_error_enum(
    query: &crate::query_introspector::IntrospectedQuery,
    table_constraints: &std::collections::HashMap<crate::pg_id::PgId, Vec<crate::pg_constraint::Constraint>>,
    config: &Config,
) -> TokenStream {
    use crate::exceptions::PgException;
    use crate::ident::{sql_to_rs_ident, CaseType};
    use crate::pg_constraint::Constraint;
    use heck::ToPascalCase;

    let error_enum_name = quote::format_ident!("{}Error", sql_to_rs_ident(&query.name, CaseType::Pascal).to_string());
    let mut error_variants = Vec::new();
    let mut unique_arms = Vec::new();
    let mut check_arms = Vec::new();
    let mut fk_arms = Vec::new();
    let mut not_null_arms = Vec::new();
    let mut custom_arms = Vec::new();
    let mut to_pgrpc_arms = Vec::new();
    let mut as_db_error_arms = Vec::new();
    let mut handled_exceptions = HashSet::new();

    // Process table dependencies that were collected during introspection
    for (table_id, constraints) in &query.table_dependencies {
        let table_name_str = table_id.name();
        let table_name_pascal = table_name_str.to_pascal_case();
        let variant_name = quote::format_ident!("{}Constraint", table_name_pascal);
        let constraint_enum = quote::format_ident!("{}Constraint", table_name_pascal);

        // Add error variant for this table's constraints
        error_variants.push(quote! {
            #[error("{} table constraint violation", #table_name_str)]
            #variant_name(super::errors::#constraint_enum, #[source] tokio_postgres::error::DbError)
        });

        // Add conversion arm to PgRpcError
        to_pgrpc_arms.push(quote! {
            #error_enum_name::#variant_name(constraint, db_error) => {
                super::errors::PgRpcError::#variant_name(constraint, db_error)
            }
        });

        // Add AsDbError arm
        as_db_error_arms.push(quote! {
            #error_enum_name::#variant_name(_, db_error) => Some(db_error)
        });

        // Generate match arms for From<tokio_postgres::Error>
        for constraint in constraints {
            match constraint {
                Constraint::PrimaryKey(pk) => {
                    let constraint_name = pk.name.as_str();
                    unique_arms.push(quote! {
                        Some(#constraint_name) => #error_enum_name::#variant_name(
                            super::errors::#constraint_enum::PrimaryKey,
                            db_error.to_owned()
                        )
                    });
                }
                Constraint::Unique(u) => {
                    let constraint_name = u.name.as_str();
                    let u_variant_name_str = u.name.to_pascal_case();
                    let u_variant_name = quote::format_ident!("{}", u_variant_name_str);
                    unique_arms.push(quote! {
                        Some(#constraint_name) => #error_enum_name::#variant_name(
                            super::errors::#constraint_enum::#u_variant_name,
                            db_error.to_owned()
                        )
                    });
                }
                Constraint::Check(c) => {
                    let constraint_name = c.name.as_str();
                    let c_variant_name_str = c.name.to_pascal_case();
                    let c_variant_name = quote::format_ident!("{}", c_variant_name_str);
                    check_arms.push(quote! {
                        Some(#constraint_name) => #error_enum_name::#variant_name(
                            super::errors::#constraint_enum::#c_variant_name,
                            db_error.to_owned()
                        )
                    });
                }
                Constraint::ForeignKey(fk) => {
                    let constraint_name = fk.name.as_str();
                    let fk_variant_name_str = fk.name.to_pascal_case();
                    let fk_variant_name = quote::format_ident!("{}", fk_variant_name_str);
                    fk_arms.push(quote! {
                        Some(#constraint_name) => #error_enum_name::#variant_name(
                            super::errors::#constraint_enum::#fk_variant_name,
                            db_error.to_owned()
                        )
                    });
                }
                Constraint::NotNull(nn) => {
                    let column_name = nn.column.as_str();
                    let col_variant_str = nn.column.to_pascal_case();
                    let nn_variant_name = quote::format_ident!("{}NotNull", col_variant_str);
                    not_null_arms.push(quote! {
                        (Some(#table_name_str), Some(#column_name)) => #error_enum_name::#variant_name(
                            super::errors::#constraint_enum::#nn_variant_name,
                            db_error.to_owned()
                        )
                    });
                }
                _ => {}
            }
        }
    }

    // Process exceptions (including from @pgrpc_throws annotations)
    for exception in &query.exceptions {
        if let PgException::Explicit(sql_state) = exception {
            let code = sql_state.code();
            let ex_variant_name = if let Some(description) = config.exceptions.get(code) {
                sql_to_rs_ident(description, CaseType::Pascal)
            } else {
                exception.rs_name(config)
            };

            if handled_exceptions.insert(ex_variant_name.to_string()) {
                if let Some(description) = config.exceptions.get(code) {
                    error_variants.push(quote! {
                        #[error(#description)]
                        #ex_variant_name(#[source] tokio_postgres::error::DbError)
                    });
                } else {
                    error_variants.push(quote! {
                        #[error("SQL state {}", #code)]
                        #ex_variant_name(#[source] tokio_postgres::error::DbError)
                    });
                }

                to_pgrpc_arms.push(quote! {
                    #error_enum_name::#ex_variant_name(db_error) => {
                        super::errors::PgRpcError::#ex_variant_name(db_error)
                    }
                });

                as_db_error_arms.push(quote! {
                    #error_enum_name::#ex_variant_name(db_error) => Some(db_error)
                });

                custom_arms.push(quote! {
                    code if code == &tokio_postgres::error::SqlState::from_code(#code) => {
                        #error_enum_name::#ex_variant_name(db_error.to_owned())
                    }
                });
            }
        }
    }

    // Add catch-all Other variant
    error_variants.push(quote! {
        #[error(transparent)]
        Other(super::errors::PgRpcError)
    });

    // Build match arms for From<tokio_postgres::Error>
    let mut all_from_arms = Vec::new();

    if !unique_arms.is_empty() {
        all_from_arms.push(quote! {
            &tokio_postgres::error::SqlState::UNIQUE_VIOLATION => {
                match db_error.constraint() {
                    #(#unique_arms,)*
                    _ => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                }
            },
        });
    }

    if !check_arms.is_empty() {
        all_from_arms.push(quote! {
            &tokio_postgres::error::SqlState::CHECK_VIOLATION => {
                match db_error.constraint() {
                    #(#check_arms,)*
                    _ => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                }
            },
        });
    }

    if !fk_arms.is_empty() {
        all_from_arms.push(quote! {
            &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION => {
                match db_error.constraint() {
                    #(#fk_arms,)*
                    _ => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                }
            },
        });
    }

    if !not_null_arms.is_empty() {
        all_from_arms.push(quote! {
            &tokio_postgres::error::SqlState::NOT_NULL_VIOLATION => {
                match (db_error.table(), db_error.column()) {
                    #(#not_null_arms,)*
                    _ => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                }
            },
        });
    }

    all_from_arms.extend(custom_arms);

    let from_postgres_arms: TokenStream = all_from_arms.into_iter().collect();

    // Build the complete error enum
    quote! {
        #[derive(Debug, thiserror::Error)]
        pub enum #error_enum_name {
            #(#error_variants),*
        }

        impl From<tokio_postgres::Error> for #error_enum_name {
            fn from(e: tokio_postgres::Error) -> Self {
                match e.as_db_error() {
                    Some(db_error) => match db_error.code() {
                        #from_postgres_arms
                        _ => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                    },
                    None => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                }
            }
        }

        impl From<#error_enum_name> for super::errors::PgRpcError {
            fn from(err: #error_enum_name) -> Self {
                match err {
                    #error_enum_name::Other(e) => e,
                    #(#to_pgrpc_arms),*
                }
            }
        }

        impl From<super::errors::PgRpcError> for #error_enum_name {
            fn from(err: super::errors::PgRpcError) -> Self {
                #error_enum_name::Other(err)
            }
        }

        impl super::errors::AsDbError for #error_enum_name {
            fn as_db_error(&self) -> Option<&tokio_postgres::error::DbError> {
                match self {
                    #(#as_db_error_arms,)*
                    #error_enum_name::Other(e) => e.as_db_error(),
                }
            }
        }
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

/// Generate a row struct for query results
fn generate_row_struct(
    struct_name: &proc_macro2::Ident,
    columns: &[crate::query_introspector::QueryColumn],
    type_index: &TypeIndex,
) -> TokenStream {
    use crate::ident::{sql_to_rs_ident, CaseType};

    let fields: Vec<TokenStream> = columns
        .iter()
        .map(|col| {
            let field_name = quote::format_ident!("{}", sql_to_rs_ident(&col.name, CaseType::Snake).to_string());
            let field_type = get_column_type(col, type_index);
            let column_name = &col.name;

            // Generate datetime serde attribute if needed
            let datetime_serde_attr = if let Some(pg_type) = type_index.get(&col.type_oid) {
                generate_datetime_serde_attr(pg_type, col.nullable)
            } else {
                None
            };

            // Generate serde attributes
            let serde_attr = match datetime_serde_attr {
                Some(attr) => quote! {
                    #[serde(rename = #column_name)]
                    #attr
                },
                None => quote! {
                    #[serde(rename = #column_name)]
                },
            };

            quote! {
                #serde_attr
                pub #field_name: #field_type
            }
        })
        .collect();

    let field_indices: Vec<usize> = (0..columns.len()).collect();
    let field_names: Vec<proc_macro2::Ident> = columns
        .iter()
        .map(|col| quote::format_ident!("{}", sql_to_rs_ident(&col.name, CaseType::Snake).to_string()))
        .collect();

    quote! {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        pub struct #struct_name {
            #(#fields),*
        }

        impl TryFrom<tokio_postgres::Row> for #struct_name {
            type Error = tokio_postgres::Error;

            fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
                Ok(Self {
                    #(
                        #field_names: row.try_get(#field_indices)?,
                    )*
                })
            }
        }
    }
}

/// Get Rust type for a parameter
fn get_param_type(type_oid: OID, type_index: &TypeIndex, nullable: bool) -> TokenStream {
    let base_type = if let Some(pg_type) = type_index.get(&type_oid) {
        match pg_type {
            PgType::Int16 => quote! { i16 },
            PgType::Int32 => quote! { i32 },
            PgType::Int64 => quote! { i64 },
            PgType::Bool => quote! { bool },
            PgType::Text => quote! { &str },
            PgType::Enum { .. } => {
                let type_ident = pg_type.to_rust_ident(type_index);
                quote! { #type_ident }
            }
            _ => {
                let type_ident = pg_type.to_rust_ident(type_index);
                quote! { &#type_ident }
            }
        }
    } else {
        // Fallback for unknown types
        quote! { &dyn postgres_types::ToSql }
    };

    if nullable {
        quote! { Option<#base_type> }
    } else {
        base_type
    }
}

/// Get Rust type for a column
fn get_column_type(
    column: &crate::query_introspector::QueryColumn,
    type_index: &TypeIndex,
) -> TokenStream {
    if let Some(pg_type) = type_index.get(&column.type_oid) {
        let base_type = match pg_type {
            PgType::Int16 => quote! { i16 },
            PgType::Int32 => quote! { i32 },
            PgType::Int64 => quote! { i64 },
            PgType::Bool => quote! { bool },
            PgType::Text => quote! { String },
            _ => {
                let type_ident = pg_type.to_rust_ident(type_index);
                quote! { #type_ident }
            }
        };

        if column.nullable {
            quote! { Option<#base_type> }
        } else {
            base_type
        }
    } else {
        // Fallback for unknown types
        if column.nullable {
            quote! { Option<serde_json::Value> }
        } else {
            quote! { serde_json::Value }
        }
    }
}
