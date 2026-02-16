use crate::annotations;
use crate::codegen::ToRust;
use crate::codegen::{FunctionName, SchemaName, OID};
use crate::config;
use crate::exceptions::{get_comment_exceptions, get_exceptions_with_triggers, PgException};
use crate::fn_index::FunctionId;
use crate::ident::{
    sql_to_rs_ident,
    CaseType::{self, Pascal},
};
use crate::pg_constraint::Constraint;
use crate::pg_id::PgId;
use crate::pg_type::PgType;
use crate::rel_index::RelIndex;
use crate::trigger_index::TriggerIndex;
use config::Config;
use heck::ToPascalCase;
use itertools::{izip, Itertools};
use log::warn;
use pg_query::protobuf::node::Node;
use pg_query::{parse_plpgsql, ParseResult};
use postgres::Row;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet};

// Re-export constraint analysis types for backward compatibility
pub use crate::constraint_analysis::{
    extract_queries, get_rel_deps, populate_trigger_exceptions, Cmd, ConflictTarget,
};

const VOID_TYPE_OID: u32 = 2278;
const RECORD_TYPE_OID: u32 = 2249;

#[derive(Debug)]
pub struct PgFn {
    pub oid: OID,
    pub schema: SchemaName,
    pub name: FunctionName,
    pub args: Vec<PgArg>,
    pub out_args: Vec<PgArg>,
    pub return_type_oid: OID,
    pub returns_set: bool,
    pub is_procedure: bool,
    pub has_table_params: bool,
    pub definition: String,
    pub comment: Option<String>,
    pub exceptions: Vec<PgException>,
}

#[derive(Debug, Clone)]
pub struct PgArg {
    pub name: String,
    pub type_oid: OID,
    pub has_default: bool,
    pub nullable: bool,
}

/// Parse @pgrpc_not_null(param_name) annotations from function comments
fn parse_out_param_not_null_annotations(comment: &Option<String>) -> BTreeSet<String> {
    comment
        .as_ref()
        .map(|c| annotations::parse_not_null(c))
        .unwrap_or_default()
}

/// Returns whether a parameter with this type needs an additional `&` when passed to the params vector.
///
/// For parameters declared as reference types (like `&str`), we need to add another `&` to create
/// the correct type for the ToSql trait. For example, a `&str` parameter becomes `&&str` in the
/// params vector because `ToSql` is implemented for `&str`, not `str`.
pub fn param_needs_reference(type_oid: OID, types: &BTreeMap<OID, PgType>) -> bool {
    match types.get(&type_oid) {
        Some(PgType::Int16) | Some(PgType::Int32) | Some(PgType::Int64)
        | Some(PgType::Bool) => true, // Primitive types need refs
        Some(PgType::Text) => true, // Function param is &str, need &&str for ToSql vector
        Some(PgType::Enum { .. }) => true, // Enums are passed by value, need reference
        Some(_) => true,            // Domain types, composite types need references for ToSql
        None => true,               // Unknown types (like std::net::IpAddr) need references
    }
}

impl PgArg {
    pub fn rs_name(&self) -> TokenStream {
        // Strip 'p_' prefix if present for cleaner Rust parameter names
        let clean_name = if self.name.starts_with("p_") {
            &self.name[2..]
        } else {
            &self.name
        };
        sql_to_rs_ident(clean_name, CaseType::Snake)
    }

    /// Returns the Rust name for this argument when used as an OUT parameter field
    pub fn rs_out_name(&self) -> TokenStream {
        // Strip 'o_' prefix if present for cleaner Rust field names
        let clean_name = if self.name.starts_with("o_") {
            &self.name[2..]
        } else {
            &self.name
        };
        sql_to_rs_ident(clean_name, CaseType::Snake)
    }

    /// Returns whether this argument needs an additional `&` when passed to the params vector
    pub fn needs_reference(&self, types: &BTreeMap<OID, PgType>) -> bool {
        param_needs_reference(self.type_oid, types)
    }

    /// Returns the appropriate reference for this argument in the params vector
    pub fn param_reference(&self, types: &BTreeMap<OID, PgType>) -> TokenStream {
        let name = self.rs_name();
        if self.needs_reference(types) {
            quote! { &#name }
        } else {
            quote! { #name }
        }
    }
}

impl PgFn {
    pub fn id(&self) -> FunctionId {
        FunctionId {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }

    pub fn rs_name(&self) -> TokenStream {
        sql_to_rs_ident(&self.name, CaseType::Snake)
    }

    pub fn return_table_struct_name(&self) -> TokenStream {
        sql_to_rs_ident(&format!("{}_row", &self.name), CaseType::Pascal)
    }
    
    pub fn out_params_struct_name(&self) -> TokenStream {
        sql_to_rs_ident(&format!("{}_out", &self.name), CaseType::Pascal)
    }

    pub fn error_enum_name(&self) -> TokenStream {
        sql_to_rs_ident(&format!("{}_error", &self.name), CaseType::Pascal)
    }

    /// Generate match arms for From<tokio_postgres::Error> implementation
    fn generate_from_postgres_match_arms(
        &self,
        used_constraints: &BTreeMap<PgId, Vec<Constraint>>,
        config: &Config,
    ) -> TokenStream {
        let error_enum_name = self.error_enum_name();
        let mut unique_arms = Vec::new();
        let mut check_arms = Vec::new();
        let mut fk_arms = Vec::new();
        let mut not_null_arms = Vec::new();
        let mut custom_arms = Vec::new();

        // Process constraints by table for this function
        for (table_id, constraints) in used_constraints {
            let table_name_str = table_id.name();
            let table_name_pascal = table_name_str.to_pascal_case();
            let error_variant = format_ident!("{}Constraint", table_name_pascal);
            let constraint_enum = format_ident!("{}Constraint", table_name_pascal);

            for constraint in constraints {
                match constraint {
                    Constraint::PrimaryKey(pk) => {
                        // Constraint names are unique within schema, so simple name match works
                        let constraint_name = pk.name.as_str();
                        unique_arms.push(quote! {
                            Some(#constraint_name) => #error_enum_name::#error_variant(
                                super::errors::#constraint_enum::PrimaryKey,
                                db_error.to_owned()
                            )
                        });
                    }
                    Constraint::Unique(u) => {
                        let constraint_name = u.name.as_str();
                        let variant_name_str = u.name.to_pascal_case();
                        let variant_name = format_ident!("{}", variant_name_str);
                        unique_arms.push(quote! {
                            Some(#constraint_name) => #error_enum_name::#error_variant(
                                super::errors::#constraint_enum::#variant_name,
                                db_error.to_owned()
                            )
                        });
                    }
                    Constraint::Check(c) => {
                        let constraint_name = c.name.as_str();
                        let variant_name_str = c.name.to_pascal_case();
                        let variant_name = format_ident!("{}", variant_name_str);
                        check_arms.push(quote! {
                            Some(#constraint_name) => #error_enum_name::#error_variant(
                                super::errors::#constraint_enum::#variant_name,
                                db_error.to_owned()
                            )
                        });
                    }
                    Constraint::ForeignKey(fk) => {
                        let constraint_name = fk.name.as_str();
                        let variant_name_str = fk.name.to_pascal_case();
                        let variant_name = format_ident!("{}", variant_name_str);
                        fk_arms.push(quote! {
                            Some(#constraint_name) => #error_enum_name::#error_variant(
                                super::errors::#constraint_enum::#variant_name,
                                db_error.to_owned()
                            )
                        });
                    }
                    Constraint::NotNull(nn) => {
                        // NOT NULL violations are reported by table and column, not constraint name
                        let column_name = nn.column.as_str();
                        let col_variant_str = nn.column.to_pascal_case();
                        let variant_name = format_ident!("{}NotNull", col_variant_str);
                        not_null_arms.push(quote! {
                            (Some(#table_name_str), Some(#column_name)) => #error_enum_name::#error_variant(
                                super::errors::#constraint_enum::#variant_name,
                                db_error.to_owned()
                            )
                        });
                    }
                    _ => {} // Skip other constraint types
                }
            }
        }

        // Process custom exceptions from this function
        let mut has_custom_errors = false;
        for exception in &self.exceptions {
            match exception {
                PgException::Explicit(sql_state) => {
                    let code = sql_state.code();
                    let variant_name = if let Some(description) = config.exceptions.get(code) {
                        sql_to_rs_ident(description, Pascal)
                    } else {
                        exception.rs_name(config)
                    };

                    custom_arms.push(quote! {
                        code if code == &tokio_postgres::error::SqlState::from_code(#code) => {
                            #error_enum_name::#variant_name(db_error.to_owned())
                        }
                    });
                }
                PgException::CustomError(_) => {
                    has_custom_errors = true;
                }
                _ => {}
            }
        }

        // Add handling for custom errors with JSON hint if we have any
        if has_custom_errors {
            // Collect all custom error names for this function
            let custom_error_arms: Vec<TokenStream> = self
                .exceptions
                .iter()
                .filter_map(|e| {
                    if let PgException::CustomError(error_name) = e {
                        let variant_name = sql_to_rs_ident(error_name, Pascal);
                        let error_name_str = error_name.as_str();
                        Some(quote! {
                            Some(#error_name_str) => {
                                if let Ok(payload) = serde_json::from_value(json.clone()) {
                                    #error_enum_name::#variant_name(payload)
                                } else {
                                    #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                                }
                            }
                        })
                    } else {
                        None
                    }
                })
                .collect();

            // Check for JSON hint instead of specific SQLSTATE
            custom_arms.push(quote! {
                _code if db_error.hint() == Some("application/json") => {
                    // Parse JSON message for custom error
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(db_error.message()) {
                        match json.get("type").and_then(|t| t.as_str()) {
                            #(#custom_error_arms,)*
                            _ => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                        }
                    } else {
                        #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                    }
                }
            });
        }

        // Build the complete match expression
        let mut all_arms = Vec::new();

        if !unique_arms.is_empty() {
            all_arms.push(quote! {
                &tokio_postgres::error::SqlState::UNIQUE_VIOLATION => {
                    match db_error.constraint() {
                        #(#unique_arms,)*
                        _ => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                    }
                },
            });
        }

        if !check_arms.is_empty() {
            all_arms.push(quote! {
                &tokio_postgres::error::SqlState::CHECK_VIOLATION => {
                    match db_error.constraint() {
                        #(#check_arms,)*
                        _ => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                    }
                },
            });
        }

        if !fk_arms.is_empty() {
            all_arms.push(quote! {
                &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION => {
                    match db_error.constraint() {
                        #(#fk_arms,)*
                        _ => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                    }
                },
            });
        }

        if !not_null_arms.is_empty() {
            all_arms.push(quote! {
                &tokio_postgres::error::SqlState::NOT_NULL_VIOLATION => {
                    match (db_error.table(), db_error.column()) {
                        #(#not_null_arms,)*
                        _ => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                    }
                },
            });
        }

        all_arms.extend(custom_arms);

        // Combine all arms into a single TokenStream
        all_arms.into_iter().collect()
    }

    /// Generate the function-specific error enum
    pub fn generate_error_enum(
        &self,
        rel_index: &crate::rel_index::RelIndex,
        table_constraints: &BTreeMap<PgId, Vec<Constraint>>,
        config: &Config,
    ) -> TokenStream {
        let error_enum_name = self.error_enum_name();
        let mut error_variants = Vec::new();
        let mut used_constraints: BTreeMap<PgId, Vec<Constraint>> = BTreeMap::new();

        // Analyze both PL/pgSQL and SQL functions
        let rel_deps = if self.definition.contains("LANGUAGE plpgsql")
            || self.definition.contains("language plpgsql")
        {
            let parsed = parse_plpgsql(&self.definition).unwrap_or_default();
            let queries = extract_queries(&parsed);
            get_rel_deps(&queries, rel_index)
        } else if self.definition.contains("LANGUAGE sql")
            || self.definition.contains("language sql")
            || self.definition.contains("LANGUAGE SQL")
        {
            // For SQL functions, extract the body and parse it
            let body = self.body();
            match pg_query::parse(body) {
                Ok(parsed) => {
                    let queries = vec![parsed];
                    get_rel_deps(&queries, rel_index)
                }
                Err(e) => {
                    warn!(
                        "Failed to parse SQL function body for error analysis: {}",
                        e
                    );
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };

        // Collect constraints from tables this function touches
        for dep in &rel_deps {
            // Find the PgId for this OID
            let pg_id = rel_index
                .iter()
                .find(|(oid, _)| **oid == dep.rel_oid)
                .map(|(_, rel)| &rel.id);

            if let Some(id) = pg_id {
                if let Some(constraints) = table_constraints.get(id) {
                    let relevant_constraints: Vec<Constraint> = constraints
                        .iter()
                        .filter(|c| {
                            // Filter constraints based on the operation type
                            match &dep.cmd {
                                Cmd::Update { cols } => c.contains_columns(cols),
                                Cmd::Insert { cols, .. } => {
                                    c.contains_columns(cols) && !matches!(c, Constraint::Default(_))
                                }
                                Cmd::Delete => matches!(c, Constraint::ForeignKey(_)),
                                _ => false,
                            }
                        })
                        .cloned()
                        .collect();

                    if !relevant_constraints.is_empty() {
                        used_constraints.insert(id.clone(), relevant_constraints);
                    }
                }
            }
        }

        // Generate constraint error variants
        for (table_id, _) in &used_constraints {
            let table_name_str = table_id.name();
            let table_name_pascal = table_name_str.to_pascal_case();
            let variant_name = format_ident!("{}Constraint", table_name_pascal);
            let constraint_enum = format_ident!("{}Constraint", table_name_pascal);

            error_variants.push(quote! {
                #[error("{} table constraint violation", #table_name_str)]
                #variant_name(super::errors::#constraint_enum, #[source] tokio_postgres::error::DbError)
            });
        }

        // Add variants for explicit exceptions raised by this function
        let mut handled_exceptions = BTreeSet::new();
        for exception in &self.exceptions {
            match exception {
                PgException::Explicit(sql_state) => {
                    let code = sql_state.code();
                    if let Some(description) = config.exceptions.get(code) {
                        let variant_name = sql_to_rs_ident(description, Pascal);
                        if handled_exceptions.insert(variant_name.to_string()) {
                            error_variants.push(quote! {
                                #[error(#description)]
                                #variant_name(#[source] tokio_postgres::error::DbError)
                            });
                        }
                    } else {
                        let variant_name = exception.rs_name(config);
                        if handled_exceptions.insert(variant_name.to_string()) {
                            error_variants.push(quote! {
                                #[error("SQL state {}", #code)]
                                #variant_name(#[source] tokio_postgres::error::DbError)
                            });
                        }
                    }
                }
                PgException::CustomError(error_name) => {
                    let variant_name = sql_to_rs_ident(error_name, Pascal);
                    if handled_exceptions.insert(variant_name.to_string()) {
                        let struct_name = variant_name.clone();
                        error_variants.push(quote! {
                            #[error("Custom error: {}", stringify!(#error_name))]
                            #variant_name(super::errors::#struct_name)
                        });
                    }
                }
                _ => {} // Handle other exception types if needed
            }
        }

        // Add catch-all variant for undetected errors
        error_variants.push(quote! {
            #[error(transparent)]
            Other(super::errors::PgRpcError)
        });

        // Generate match arms for From implementations
        let mut to_pgrpc_arms = Vec::new();
        let mut constraint_variant_names = Vec::new();

        // Generate arms for constraint variants
        for (table_id, _) in &used_constraints {
            let table_name_pascal = table_id.name().to_pascal_case();
            let variant_name = format_ident!("{}Constraint", table_name_pascal);
            constraint_variant_names.push(variant_name.clone());

            to_pgrpc_arms.push(quote! {
                #error_enum_name::#variant_name(constraint, db_error) => {
                    super::errors::PgRpcError::#variant_name(constraint, db_error)
                }
            });
        }

        // Generate arms for exception variants
        for exception in &self.exceptions {
            match exception {
                PgException::Explicit(sql_state) => {
                    let variant_name =
                        if let Some(description) = config.exceptions.get(sql_state.code()) {
                            sql_to_rs_ident(description, Pascal)
                        } else {
                            exception.rs_name(config)
                        };

                    to_pgrpc_arms.push(quote! {
                        #error_enum_name::#variant_name(db_error) => {
                            super::errors::PgRpcError::#variant_name(db_error)
                        }
                    });
                }
                PgException::CustomError(error_name) => {
                    let variant_name = sql_to_rs_ident(error_name, Pascal);
                    to_pgrpc_arms.push(quote! {
                        #error_enum_name::#variant_name(payload) => {
                            super::errors::PgRpcError::CustomError(super::errors::CustomError::#variant_name(payload))
                        }
                    });
                }
                _ => {}
            }
        }

        // Generate From<tokio_postgres::Error> match arms
        let from_postgres_arms = self.generate_from_postgres_match_arms(&used_constraints, config);

        // Generate AsDbError implementation arms
        let mut as_db_error_arms = Vec::new();

        // Arms for constraint variants
        for (table_id, _) in &used_constraints {
            let table_name_pascal = table_id.name().to_pascal_case();
            let variant_name = format_ident!("{}Constraint", table_name_pascal);
            as_db_error_arms.push(quote! {
                #error_enum_name::#variant_name(_, db_error) => Some(db_error)
            });
        }

        // Arms for custom exception variants
        for exception in &self.exceptions {
            match exception {
                PgException::Explicit(sql_state) => {
                    let variant_name =
                        if let Some(description) = config.exceptions.get(sql_state.code()) {
                            sql_to_rs_ident(description, Pascal)
                        } else {
                            exception.rs_name(config)
                        };
                    as_db_error_arms.push(quote! {
                        #error_enum_name::#variant_name(db_error) => Some(db_error)
                    });
                }
                PgException::CustomError(error_name) => {
                    let variant_name = sql_to_rs_ident(error_name, Pascal);
                    as_db_error_arms.push(quote! {
                        #error_enum_name::#variant_name(_) => None
                    });
                }
                _ => {}
            }
        }

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

    /// Get all type OIDs in the function signature (args + return type)
    pub fn ty_oids(&self) -> Vec<OID> {
        let mut oids = self.args.iter().map(|a| a.type_oid).collect_vec();
        oids.extend(self.out_args.iter().map(|a| a.type_oid));
        oids.push(self.return_type_oid);
        oids
    }

    /// Retrieve the body (between the `$$`)
    fn body(&self) -> &str {
        let (tag, start_ind) = quote_ind(&self.definition).unwrap();
        let end_ind = self.definition[start_ind..].find(tag).unwrap() + start_ind;

        &self.definition[start_ind..end_ind]
    }

    /// Infer nullability of OUT parameters for SQL language functions
    /// using the ViewNullabilityAnalyzer on the function body
    pub fn infer_out_param_nullability(
        &mut self,
        rel_index: &RelIndex,
        view_nullability_cache: &crate::view_nullability::ViewNullabilityCache,
    ) -> anyhow::Result<()> {
        // Only apply to SQL language functions with OUT parameters
        if !self.definition.contains("LANGUAGE sql")
            && !self.definition.contains("language sql")
            && !self.definition.contains("LANGUAGE SQL") {
            return Ok(());
        }

        if self.out_args.is_empty() {
            return Ok(());
        }

        // Parse @pgrpc_not_null annotations - these take precedence over inference
        let not_null_annotations = parse_out_param_not_null_annotations(&self.comment);

        // Extract the function body
        let body = self.body();

        // Parse the body as SQL
        let parsed = match pg_query::parse(body) {
            Ok(p) => p,
            Err(e) => {
                log::warn!(
                    "Failed to parse SQL function body for nullability analysis (function: {}): {}",
                    self.name,
                    e
                );
                return Ok(()); // Gracefully fall back to default nullable
            }
        };

        // Create analyzer
        let mut analyzer = crate::view_nullability::ViewNullabilityAnalyzer::new(
            rel_index,
            view_nullability_cache,
        );

        // Collect column names from OUT parameters
        let column_names: Vec<String> = self.out_args.iter().map(|arg| arg.name.clone()).collect();

        // Analyze the query
        match analyzer.analyze_view(body, &column_names) {
            Ok(nullability_map) => {
                log::info!(
                    "Inferred nullability for SQL function {}: {:?}",
                    self.name,
                    nullability_map
                );

                // Update OUT parameter nullability
                for out_arg in &mut self.out_args {
                    // Skip if annotation exists (annotations take precedence)
                    if not_null_annotations.contains(&out_arg.name) {
                        log::debug!(
                            "Function {}: OUT param {} has @pgrpc_not_null annotation, skipping inference",
                            self.name,
                            out_arg.name
                        );
                        out_arg.nullable = false;
                        continue;
                    }

                    // Apply inferred nullability
                    if let Some(&is_not_null) = nullability_map.get(&out_arg.name) {
                        out_arg.nullable = !is_not_null;
                        log::debug!(
                            "Function {}: OUT param {} inferred as {}",
                            self.name,
                            out_arg.name,
                            if is_not_null { "NOT NULL" } else { "NULLABLE" }
                        );
                    }
                }
            }
            Err(e) => {
                log::warn!(
                    "Failed to analyze SQL function nullability (function: {}): {}",
                    self.name,
                    e
                );
                // Gracefully fall back to default nullable behavior
            }
        }

        Ok(())
    }
}

impl PgFn {
    pub fn from_row(
        row: Row,
        rel_index: &RelIndex,
        trigger_index: Option<&TriggerIndex>,
    ) -> Result<Self, anyhow::Error> {
        let arg_type_oids: Vec<OID> = row.try_get("arg_oids")?;
        let arg_names: Vec<String> = row.try_get("arg_names")?;
        let arg_defaults: Vec<bool> = row.try_get("has_defaults")?;
        let comment: Option<String> = row.try_get("comment")?;
        let language: String = row.try_get("language")?;
        let prokind: i8 = row.try_get("prokind")?;

        // Get additional fields for OUT parameters
        let arg_modes: Option<Vec<i8>> = row.try_get("arg_modes")?;
        let all_arg_types: Option<Vec<OID>> = row.try_get("all_arg_types")?;
        let all_arg_names: Option<Vec<String>> = row.try_get("all_arg_names")?;

        let mut args: Vec<PgArg> = Vec::new();
        let mut out_args: Vec<PgArg> = Vec::new();
        let mut has_table_params = false;
        
        // Parse @pgrpc_not_null annotations for OUT parameters
        let not_null_params = parse_out_param_not_null_annotations(&comment);

        // If we have arg_modes, parse IN/OUT parameters separately
        if let (Some(modes), Some(types), Some(names)) = (arg_modes, all_arg_types, all_arg_names) {
            for (i, ((mode, &type_oid), name)) in
                modes.iter().zip(types.iter()).zip(names.iter()).enumerate()
            {
                match *mode {
                    105 | 98 => {
                        // 'i' (IN) or 'b' (INOUT) parameters
                        args.push(PgArg {
                            name: name.clone(),
                            type_oid,
                            has_default: arg_defaults.get(args.len()).copied().unwrap_or(false),
                            nullable: false, // IN parameters are never nullable in function signatures
                        });
                    }
                    _ => {} // Skip for now
                }

                if *mode == 111 || *mode == 98 || *mode == 116 {
                    // 'o' (OUT), 'b' (INOUT), or 't' (TABLE) parameters
                    if *mode == 116 {
                        has_table_params = true;
                    }
                    out_args.push(PgArg {
                        name: name.clone(),
                        type_oid,
                        has_default: false,
                        nullable: !not_null_params.contains(name),
                    });
                }
            }
        } else {
            // Fallback to original logic if no modes
            args = izip!(arg_type_oids, arg_names, arg_defaults)
                .map(|(oid, name, has_default)| PgArg {
                    name: name.clone(),
                    type_oid: oid,
                    has_default,
                    nullable: false, // IN parameters are never nullable
                })
                .collect();
        }

        let definition = row.try_get::<_, String>("function_definition")?;

        // Parse and analyze both PL/pgSQL and SQL functions
        let exceptions = match language.as_str() {
            "plpgsql" => {
                let parsed = parse_plpgsql(&definition)?;
                get_exceptions_with_triggers(
                    &parsed,
                    comment.as_ref(),
                    rel_index,
                    trigger_index,
                    None,
                )?
            }
            "sql" => {
                // Extract the function body and parse as SQL
                let fn_obj = Self {
                    oid: row.try_get("oid")?,
                    name: row.try_get("function_name")?,
                    schema: row.try_get("schema_name")?,
                    definition: definition.clone(),
                    returns_set: row.try_get("returns_set")?,
                    is_procedure: prokind == b'p' as i8,
                    has_table_params,
                    comment: comment.clone(),
                    args: args.clone(),
                    out_args: out_args.clone(),
                    return_type_oid: row.try_get("return_type")?,
                    exceptions: Vec::new(),
                };

                let body = fn_obj.body();
                // Parse SQL body to get queries
                match pg_query::parse(body) {
                    Ok(parsed) => {
                        // Convert parsed SQL to queries vector
                        let queries = vec![parsed];
                        let mut rel_deps = get_rel_deps(&queries, rel_index);

                        // Populate trigger exceptions if available
                        if let Some(trigger_idx) = trigger_index {
                            populate_trigger_exceptions(&mut rel_deps, trigger_idx);
                        }

                        // Get exceptions from table dependencies
                        let mut exceptions: Vec<PgException> = Vec::new();

                        // Collect constraint-based exceptions
                        for dep in &rel_deps {
                            if let Some(rel) = rel_index.get(&dep.rel_oid) {
                                for constraint in &rel.constraints {
                                    // Filter constraints based on the operation type
                                    let include = match &dep.cmd {
                                        Cmd::Update { cols } => constraint.contains_columns(cols),
                                        Cmd::Insert { cols, .. } => {
                                            constraint.contains_columns(cols)
                                                && !matches!(constraint, Constraint::Default(_))
                                        }
                                        Cmd::Delete => {
                                            matches!(constraint, Constraint::ForeignKey(_))
                                        }
                                        _ => false,
                                    };

                                    if include {
                                        exceptions
                                            .push(PgException::Constraint(constraint.clone()));
                                    }
                                }
                            }
                        }

                        // Add trigger-based exceptions if available
                        if let Some(trigger_idx) = trigger_index {
                            for dep in &rel_deps {
                                exceptions.extend(dep.trigger_exceptions.iter().cloned());
                            }
                        }

                        // Add comment exceptions
                        if let Some(ref comment_str) = comment {
                            exceptions.extend(get_comment_exceptions(comment_str));
                        }

                        exceptions
                    }
                    Err(e) => {
                        warn!("Failed to parse SQL function body: {}", e);
                        Vec::new()
                    }
                }
            }
            _ => {
                // Other languages not supported yet
                Vec::new()
            }
        };

        Ok(Self {
            oid: row.try_get("oid")?,
            name: row.try_get("function_name")?,
            schema: row.try_get("schema_name")?,
            definition,
            returns_set: row.try_get("returns_set")?,
            is_procedure: prokind == b'p' as i8, // 'p' for procedure, 'f' for function
            has_table_params,
            comment,
            args,
            out_args,
            return_type_oid: row.try_get("return_type")?,
            exceptions,
        })
    }
}

impl ToRust for PgArg {
    fn to_rust(&self, types: &BTreeMap<OID, PgType>, _config: &Config) -> TokenStream {
        // Strip 'p_' prefix if present for cleaner Rust parameter names
        let clean_name = if self.name.starts_with("p_") {
            &self.name[2..]
        } else {
            &self.name
        };
        let name = sql_to_rs_ident(clean_name, CaseType::Snake);
        let ty = match types.get(&self.type_oid).unwrap() {
            PgType::Int16 => quote! { i16 },
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
    fn to_rust(&self, types: &BTreeMap<OID, PgType>, config: &Config) -> TokenStream {
        // Use function-specific error type
        let error_enum_name = self.error_enum_name();
        let err_type = quote! { #error_enum_name };
        let fn_name_str = &self.name;

        let return_opt = self
            .comment
            .as_ref()
            .is_some_and(|c| annotations::has_return_opt(c));
        if return_opt && !self.returns_set {
            panic!(
                "{}: Only set-returning functions can have @pgrpc_return_opt.",
                self.name
            )
        }

        // Check if this is a RETURN TABLE function
        let is_return_table = self.has_table_params;

        let fn_body = {
            let (opt_args, req_args): (Vec<PgArg>, Vec<PgArg>) =
                self.args.iter().cloned().partition(|n| n.has_default);

            // Generate required query parameters positionally ($1, $2, ..., $n)
            let required_query_params = (1..=self.args.len() - opt_args.len())
                .map(|i| format!("${}", i))
                .collect::<Vec<_>>()
                .join(", ");

            let req_arg_names: Vec<TokenStream> = req_args.iter().map(|a| a.rs_name()).collect();
            let req_arg_refs: Vec<TokenStream> =
                req_args.iter().map(|a| a.param_reference(types)).collect();
            let opt_arg_names: Vec<TokenStream> = opt_args.iter().map(|a| a.rs_name()).collect();

            // Check if return type needs special handling
            let return_pg_type = types.get(&self.return_type_oid);
            let needs_expansion = if let Some(PgType::Composite { fields, .. }) = return_pg_type {
                fields.iter().any(|f| f.flatten)
            } else {
                false
            };

            let query_string = if self.is_procedure {
                format!("CALL {}.{}", self.schema, self.name)
            } else if self.returns_set || needs_expansion || (!self.is_procedure && self.out_args.len() > 1) {
                format!("select * from {}.{}", self.schema, self.name)
            } else {
                format!("select {}.{}", self.schema, self.name)
            };

            let query = if self.is_procedure {
                // Handle procedures differently
                if self.out_args.is_empty() {
                    // No OUT parameters - just execute
                    quote! {
                        client
                            .execute(&query, &params)
                            .await
                            .map(|_| ())
                            .map_err(#err_type::from)
                    }
                } else if self.out_args.len() == 1 {
                    // Single OUT parameter - return it directly
                    quote! {
                        client
                            .query_one(&query, &params)
                            .await
                            .and_then(|row| row.try_get(0))
                            .map_err(#err_type::from)
                    }
                } else {
                    // Multiple OUT parameters - return as tuple or struct
                    // For now, return as a Row that can be converted
                    quote! {
                        client
                            .query_one(&query, &params)
                            .await
                            .and_then(|row| row.try_into())
                            .map_err(#err_type::from)
                    }
                }
            } else if return_opt {
                quote! {
                    client
                        .query_opt(&query, &params)
                        .await
                        .and_then(|opt_row| match opt_row {
                            None => Ok(None),
                            Some(row) => row.try_get(0).map(Some),
                        })
                        .map_err(#err_type::from)
                }
            } else if self.returns_set {
                if self.return_type_oid == VOID_TYPE_OID {
                    // Void set-returning function - just return vec of units
                    quote! {
                        client
                            .query(&query, &params)
                            .await
                            .map(|rows| vec![(); rows.len()])
                            .map_err(#err_type::from)
                    }
                } else if is_return_table {
                    // RETURN TABLE functions need row conversion
                    quote! {
                        client
                            .query(&query, &params)
                            .await
                            .and_then(|rows| {
                                rows.into_iter().map(TryInto::try_into).collect()
                            }).map_err(#err_type::from)
                    }
                } else {
                    // Check if return type is composite or primitive
                    let return_pg_type = types.get(&self.return_type_oid).unwrap();
                    match return_pg_type {
                        PgType::Composite { .. } => {
                            // Composite types use TryInto
                            quote! {
                                client
                                    .query(&query, &params)
                                    .await
                                    .and_then(|rows| {
                                        rows.into_iter().map(TryInto::try_into).collect()
                                    }).map_err(#err_type::from)
                            }
                        }
                        _ => {
                            // Primitive types use try_get(0)
                            quote! {
                                client
                                    .query(&query, &params)
                                    .await
                                    .and_then(|rows| {
                                        rows.into_iter().map(|row| row.try_get(0)).collect()
                                    }).map_err(#err_type::from)
                            }
                        }
                    }
                }
            } else if !self.is_procedure && self.out_args.len() > 1 {
                // Functions with multiple OUT parameters return a struct via TryFrom<Row>
                quote! {
                    client
                        .query_one(&query, &params)
                        .await
                        .and_then(|r| r.try_into())
                        .map_err(#err_type::from)
                }
            } else if self.return_type_oid == VOID_TYPE_OID {
                // Void returning function
                quote! {
                    client
                        .execute(&query, &params)
                        .await
                        .map_err(#err_type::from)?;

                    Ok(())
                }
            } else if is_return_table {
                // Non-set RETURN TABLE function (single row)
                quote! {
                    client
                        .query_one(&query, &params)
                        .await
                        .and_then(|r| r.try_into())
                        .map_err(#err_type::from)
                }
            } else {
                // Check if return type is a composite type with custom TryFrom
                let return_pg_type = types.get(&self.return_type_oid).unwrap();
                match return_pg_type {
                    PgType::Composite { fields, .. } => {
                        // Check if any fields have flatten annotation
                        let has_flatten = fields.iter().any(|f| f.flatten);
                        if has_flatten {
                            // For composite types with flatten, we use SELECT * FROM function()
                            // which expands the composite type into columns that our TryFrom can handle
                            let return_not_null = self
                                .comment
                                .as_ref()
                                .is_some_and(|c| annotations::has_not_null(c));
                            if return_not_null {
                                quote! {
                                    client
                                        .query_one(&query, &params)
                                        .await
                                        .and_then(|r| r.try_into())
                                        .map_err(#err_type::from)
                                }
                            } else {
                                quote! {
                                    client
                                        .query_one(&query, &params)
                                        .await
                                        .and_then(|r| Ok(Some(r.try_into()?)))
                                        .map_err(#err_type::from)
                                }
                            }
                        } else {
                            // Regular composite types can use try_get
                            quote! {
                                client
                                    .query_one(&query, &params)
                                    .await
                                    .and_then(|r| r.try_get(0))
                                    .map_err(#err_type::from)
                            }
                        }
                    }
                    _ => {
                        // Non-composite types use try_get
                        quote! {
                            client
                                .query_one(&query, &params)
                                .await
                                .and_then(|r| r.try_get(0))
                                .map_err(#err_type::from)
                        }
                    }
                }
            };

            if !opt_args.is_empty() {
                // Generate code for functions with optional parameters using string building
                let opt_param_handling: Vec<TokenStream> = opt_args.iter().enumerate().map(|(i, arg)| {
                    let arg_name = arg.rs_name();
                    let sql_param_name = &arg.name;
                    let param_index = req_args.len() + i + 1;
                    // We need to use as_ref() to avoid moving the value out of the Option
                    let param_ref = if arg.needs_reference(types) {
                        quote! { val }  // val is already a reference from as_ref()
                    } else {
                        quote! { *val } // Dereference to get the value
                    };
                    quote! {
                        if let Some(val) = #arg_name.as_ref() {
                            params.push(#param_ref);
                            optional_parts.push(format!("{} := ${}", #sql_param_name, params.len()));
                        }
                    }
                }).collect();

                quote! {
                    let mut params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![#(#req_arg_refs),*];
                    let mut optional_parts = Vec::new();

                    #(#opt_param_handling)*

                    let query = if optional_parts.is_empty() {
                        if params.is_empty() {
                            format!("{}()", #query_string)
                        } else {
                            format!("{}({})", #query_string, #required_query_params)
                        }
                    } else {
                        let optional_clause = optional_parts.join(", ");
                        if params.len() == optional_parts.len() {
                            // Only optional parameters
                            format!("{}({})", #query_string, optional_clause)
                        } else {
                            // Both required and optional parameters
                            format!("{}({}, {})", #query_string, #required_query_params, optional_clause)
                        }
                    };

                    #query
                }
            } else {
                // Simple case with only required parameters
                quote! {
                    let params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![#(#req_arg_refs),*];
                    let query = format!("{}({})", #query_string, #required_query_params);

                    #query
                }
            }
        };

        // Build fn signature
        let rs_fn_name = self.rs_name();
        let args: Vec<TokenStream> = self.args.iter().map(|a| a.to_rust(types, config)).collect();
        let return_type = if self.is_procedure {
            // Procedures have different return types based on OUT parameters
            if self.out_args.is_empty() {
                quote! { () }
            } else if self.out_args.len() == 1 {
                // Single OUT parameter - return its type directly
                let out_type = types
                    .get(&self.out_args[0].type_oid)
                    .unwrap()
                    .to_rust_ident(types);
                if self.out_args[0].nullable {
                    quote! { Option<#out_type> }
                } else {
                    quote! { #out_type }
                }
            } else {
                // Multiple OUT parameters - use generated struct
                let struct_name = self.out_params_struct_name();
                quote! { #struct_name }
            }
        } else if self.return_type_oid == VOID_TYPE_OID {
            quote! { () }
        } else if is_return_table {
            // For RETURN TABLE functions, use the generated struct
            let struct_name = self.return_table_struct_name();
            if self.returns_set {
                quote! { Vec<#struct_name> }
            } else {
                quote! { #struct_name }
            }
        } else if !self.is_procedure && self.out_args.len() > 1 {
            // Functions with multiple OUT parameters use generated struct
            let struct_name = self.out_params_struct_name();
            quote! { #struct_name }
        } else {
            let inner_ty = types
                .get(&self.return_type_oid)
                .unwrap()
                .to_rust_ident(types);

            if self.returns_set {
                quote! { Vec<#inner_ty> }
            } else {
                let return_not_null = self
                    .comment
                    .as_ref()
                    .is_some_and(|c| annotations::has_not_null(c));
                if return_not_null {
                    inner_ty
                } else {
                    quote! { Option<#inner_ty> }
                }
            }
        };

        let doc_comment = match &self.comment {
            Some(comment) => {
                if self.is_procedure {
                    format!("Calls PostgreSQL procedure: {}\n\n{}", self.name, comment)
                } else {
                    comment.clone()
                }
            }
            None => {
                if self.is_procedure {
                    format!("Calls PostgreSQL procedure: {}", self.name)
                } else {
                    format!("Calls PostgreSQL function: {}", self.name)
                }
            }
        };
        let comment_macro = quote! { #[doc=#doc_comment] };

        // Generate struct for RETURN TABLE functions or procedures/functions with multiple OUT parameters
        let struct_def = if is_return_table || self.out_args.len() > 1 {
            let struct_name = if is_return_table {
                self.return_table_struct_name()
            } else {
                self.out_params_struct_name()
            };
            let field_tokens: Vec<TokenStream> = self
                .out_args
                .iter()
                .map(|arg| {
                    let field_name = arg.rs_out_name();
                    let pg_name = &arg.name;
                    let base_type = match types.get(&arg.type_oid).unwrap() {
                        PgType::Int16 => quote! { i16 },
                        PgType::Int32 => quote! { i32 },
                        PgType::Int64 => quote! { i64 },
                        PgType::Bool => quote! { bool },
                        PgType::Text => quote! { String },
                        t => {
                            let id = t.to_rust_ident(types);
                            quote! { #id }
                        }
                    };
                    let field_type = if arg.nullable {
                        quote! { Option<#base_type> }
                    } else {
                        base_type
                    };

                    let serde_attr = if arg.nullable {
                        quote! { #[serde(rename = #pg_name, default)] }
                    } else {
                        quote! { #[serde(rename = #pg_name)] }
                    };

                    quote! {
                        #serde_attr
                        pub #field_name: #field_type
                    }
                })
                .collect();

            let field_names: Vec<TokenStream> = self
                .out_args
                .iter()
                .map(|arg| arg.rs_out_name())
                .collect();
            let field_indices: Vec<usize> = (0..self.out_args.len()).collect();

            quote! {
                #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
                pub struct #struct_name {
                    #(#field_tokens),*
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
        } else {
            quote! {}
        };

        // Check if tracing is enabled
        let tracing_enabled = config.tracing.as_ref().map(|t| t.enabled).unwrap_or(false);
        let schema_str = &self.schema;

        // Generate the tracing::instrument attribute if enabled
        let tracing_attr = if tracing_enabled {
            // Build the span name as "schema.function_name"
            let span_name = format!("{}.{}", schema_str, fn_name_str);
            quote! {
                #[tracing::instrument(name = #span_name, skip(client), err(Debug))]
            }
        } else {
            quote! {}
        };

        quote! {
            #struct_def

            #comment_macro
            #tracing_attr
            #[builder(finish_fn = exec)]
            pub async fn #rs_fn_name(
                #[builder(finish_fn)]
                client: &impl deadpool_postgres::GenericClient,
                #(#args),*) -> Result<#return_type, #err_type> {
                #fn_body
            }
        }
    }
}



/// Extract custom error types from parsed queries by looking for calls to the raise_error function
/// Returns a set of error type names and optionally their SQLSTATEs
pub fn extract_custom_errors_from_queries(
    queries: &[ParseResult],
    errors_config: &crate::config::ErrorsConfig,
) -> BTreeSet<String> {
    let mut custom_errors = BTreeSet::new();
    let raise_function = errors_config.get_raise_function();
    let errors_schema = &errors_config.schema;

    for query in queries {
        for (node, _, _, _) in query.protobuf.nodes() {
            match node.to_enum() {
                // Handle CALL statements
                Node::CallStmt(stmt) => {
                    if let Some(funccall) = &stmt.funccall {
                        if is_raise_error_call(&funccall.funcname, raise_function) {
                            // First argument should be the error type cast
                            if let Some(first_arg) = funccall.args.first() {
                                if let Some(error_type) =
                                    extract_error_type_from_node(first_arg, errors_schema)
                                {
                                    custom_errors.insert(error_type);

                                    // Check for optional second argument (SQLSTATE)
                                    // We don't need to track the SQLSTATE here since we're using HINT-based detection
                                    // But we could extract it for future use
                                }
                            }
                        }
                    }
                }
                // Handle SELECT statements (from PERFORM)
                Node::SelectStmt(stmt) => {
                    // Look for function calls in the target list
                    for target in &stmt.target_list {
                        if let Some(Node::ResTarget(res_target)) = &target.node {
                            if let Some(Node::FuncCall(funccall)) =
                                &res_target.val.as_ref().and_then(|v| v.node.as_ref())
                            {
                                if is_raise_error_call(&funccall.funcname, raise_function) {
                                    // First argument should be the error type cast
                                    if let Some(first_arg) = funccall.args.first() {
                                        if let Some(error_type) =
                                            extract_error_type_from_node(first_arg, errors_schema)
                                        {
                                            custom_errors.insert(error_type);

                                            // Check for optional second argument (SQLSTATE)
                                            // We don't need to track the SQLSTATE here since we're using HINT-based detection
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    custom_errors
}

/// Check if a function name matches the raise_error function
fn is_raise_error_call(funcname: &[pg_query::protobuf::Node], expected: &str) -> bool {
    let parts: Vec<String> = funcname
        .iter()
        .filter_map(|n| {
            if let Some(Node::String(s)) = &n.node {
                Some(s.sval.clone())
            } else {
                None
            }
        })
        .collect();

    let full_name = parts.join(".");
    full_name == expected
}

/// Extract error type name from a node if it contains a type cast to errors schema
fn extract_error_type_from_node(
    node: &pg_query::protobuf::Node,
    errors_schema: &str,
) -> Option<String> {
    // Look for TypeCast nodes
    if let Some(Node::TypeCast(type_cast)) = &node.node {
        if let Some(type_name) = &type_cast.type_name {
            // Extract the type name parts
            let type_parts: Vec<String> = type_name
                .names
                .iter()
                .filter_map(|n| {
                    if let Some(Node::String(s)) = &n.node {
                        Some(s.sval.clone())
                    } else {
                        None
                    }
                })
                .collect();

            // Check if it's a type from the errors schema
            if type_parts.len() == 2 && type_parts[0] == errors_schema {
                return Some(type_parts[1].clone());
            }
        }
    }

    // Also check nested nodes (e.g., for ROW(...) expressions)
    None
}



/// Get the position of the first `$...$` tag
fn quote_ind(src: &str) -> Option<(&str, usize)> {
    let re = Regex::new(r"\$.*?\$").unwrap();

    // Find the first match
    if let Some(matched) = re.find(src) {
        let s = matched.as_str();
        Some((s, matched.end()))
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use crate::pg_fn::{extract_queries, get_rel_deps};
    use crate::pg_id::PgId;
    use crate::pg_rel::{PgRel, PgRelKind};
    use crate::rel_index::RelIndex;
    use pg_query::parse_plpgsql;
    use ustr::ustr;

    #[test]
    fn test_get_rels() -> anyhow::Result<()> {
        let fn_def = r#"
        create or replace function test() returns void as
        $$
        begin
            insert into a (field_1) values ('Example') on conflict do nothing;
            select * from b;
        end;
        $$ language plpgsql;"#;

        let f = parse_plpgsql(fn_def)?;

        let mut rel_index = RelIndex::default();
        rel_index.insert(
            1,
            PgRel {
                kind: PgRelKind::Table,
                oid: 1,
                id: PgId::new(None, ustr("a")),
                constraints: Vec::default(),
                columns: vec![ustr("field_1")],
                column_types: vec![],
            },
        );

        rel_index.insert(
            2,
            PgRel {
                kind: PgRelKind::Table,
                oid: 2,
                id: PgId::new(None, ustr("b")),
                constraints: Vec::default(),
                columns: Vec::default(),
                column_types: vec![],
            },
        );

        let queries = extract_queries(&f);
        let rels = get_rel_deps(&queries, &rel_index);
        // Only INSERT is detected, SELECT deps are not implemented (TODO in get_select_deps)
        assert_eq!(rels.len(), 1);

        Ok(())
    }

    #[test]
    fn test_setof_primitive_codegen() {
        use crate::codegen::ToRust;
        use crate::pg_fn::PgFn;
        use crate::pg_type::PgType;
        use std::collections::BTreeMap;

        // Create a function that returns SETOF text
        let pg_fn = PgFn {
            oid: 1234,
            schema: "public".to_string(),
            name: "get_names".to_string(),
            args: vec![],
            out_args: vec![],
            returns_set: true,
            is_procedure: false,
            has_table_params: false,
            return_type_oid: 25, // TEXT OID
            comment: None,
            definition: "".to_string(),
            exceptions: vec![],
        };

        // Create type index with Text type
        let mut types = BTreeMap::new();
        types.insert(25, PgType::Text);

        // Create a simple config
        let config = crate::config::Config {
            connection_string: None,
            output_path: None,
            schemas: vec!["public".to_string()],
            types: std::collections::HashMap::new(),
            exceptions: std::collections::HashMap::new(),
            task_queue: None,
            errors: None,
            infer_view_nullability: true,
            disable_deserialize: Vec::new(),
            queries: None,
            tracing: None,
        };

        // Generate the code
        let generated = pg_fn.to_rust(&types, &config);
        let generated_str = generated.to_string();

        // Verify that for primitive SETOF return types, we use row.try_get(0)
        assert!(generated_str
            .contains("rows . into_iter () . map (| row | row . try_get (0)) . collect ()"));
        // Should NOT contain TryInto::try_into for primitive types
        assert!(!generated_str.contains("TryInto :: try_into"));
    }

    #[test]
    fn test_setof_composite_codegen() {
        use crate::codegen::ToRust;
        use crate::pg_fn::PgFn;
        use crate::pg_type::{PgField, PgType};
        use std::collections::BTreeMap;

        // Create a function that returns SETOF composite_type
        let pg_fn = PgFn {
            oid: 1234,
            schema: "public".to_string(),
            name: "get_users".to_string(),
            args: vec![],
            out_args: vec![],
            returns_set: true,
            is_procedure: false,
            has_table_params: false,
            return_type_oid: 1000, // Custom composite type OID
            comment: None,
            definition: "".to_string(),
            exceptions: vec![],
        };

        // Create type index with a composite type
        let mut types = BTreeMap::new();
        types.insert(
            1000,
            PgType::Composite {
                schema: "public".to_string(),
                name: "user_type".to_string(),
                fields: vec![
                    PgField {
                        name: "id".to_string(),
                        type_oid: 23, // INT4
                        nullable: false,
                        comment: None,
                        flatten: false,
                    },
                    PgField {
                        name: "name".to_string(),
                        type_oid: 25, // TEXT
                        nullable: true,
                        comment: None,
                        flatten: false,
                    },
                ],
                comment: None,
                relkind: None,
                view_definition: None,
            },
        );
        types.insert(23, PgType::Int32);
        types.insert(25, PgType::Text);

        // Create a simple config
        let config = crate::config::Config {
            connection_string: None,
            output_path: None,
            schemas: vec!["public".to_string()],
            types: std::collections::HashMap::new(),
            exceptions: std::collections::HashMap::new(),
            task_queue: None,
            errors: None,
            infer_view_nullability: true,
            disable_deserialize: Vec::new(),
            queries: None,
            tracing: None,
        };

        // Generate the code
        let generated = pg_fn.to_rust(&types, &config);
        let generated_str = generated.to_string();

        // Verify that for composite SETOF return types, we use TryInto::try_into
        assert!(
            generated_str.contains("rows . into_iter () . map (TryInto :: try_into) . collect ()")
        );
        // Should NOT contain try_get(0) for composite types
        assert!(!generated_str.contains("row . try_get (0)"));
    }

    #[test]
    fn test_tracing_enabled_codegen() {
        use crate::codegen::ToRust;
        use crate::config::TracingConfig;
        use crate::pg_fn::PgFn;
        use crate::pg_type::PgType;
        use std::collections::BTreeMap;

        // Create a simple function
        let pg_fn = PgFn {
            oid: 1234,
            schema: "public".to_string(),
            name: "get_user".to_string(),
            args: vec![],
            out_args: vec![],
            returns_set: false,
            is_procedure: false,
            has_table_params: false,
            return_type_oid: 25, // TEXT OID
            comment: None,
            definition: "".to_string(),
            exceptions: vec![],
        };

        let mut types = BTreeMap::new();
        types.insert(25, PgType::Text);

        // Create config with tracing enabled
        let config = crate::config::Config {
            connection_string: None,
            output_path: None,
            schemas: vec!["public".to_string()],
            types: std::collections::HashMap::new(),
            exceptions: std::collections::HashMap::new(),
            task_queue: None,
            errors: None,
            infer_view_nullability: true,
            disable_deserialize: Vec::new(),
            queries: None,
            tracing: Some(TracingConfig {
                enabled: true,
            }),
        };

        // Generate the code
        let generated = pg_fn.to_rust(&types, &config);
        let generated_str = generated.to_string();

        // Verify that tracing::instrument attribute is included
        assert!(
            generated_str.contains("tracing :: instrument"),
            "Generated code should contain tracing::instrument attribute"
        );
        assert!(
            generated_str.contains("public.get_user"),
            "Span name should be schema.function_name"
        );
        assert!(
            generated_str.contains("skip (client)"),
            "Should skip client parameter"
        );
        assert!(
            generated_str.contains("err (Debug)"),
            "Should include err(Debug) to capture full error chain"
        );
    }

    #[test]
    fn test_tracing_disabled_codegen() {
        use crate::codegen::ToRust;
        use crate::pg_fn::PgFn;
        use crate::pg_type::PgType;
        use std::collections::BTreeMap;

        // Create a simple function
        let pg_fn = PgFn {
            oid: 1234,
            schema: "public".to_string(),
            name: "get_user".to_string(),
            args: vec![],
            out_args: vec![],
            returns_set: false,
            is_procedure: false,
            has_table_params: false,
            return_type_oid: 25, // TEXT OID
            comment: None,
            definition: "".to_string(),
            exceptions: vec![],
        };

        let mut types = BTreeMap::new();
        types.insert(25, PgType::Text);

        // Create config with tracing disabled (default)
        let config = crate::config::Config {
            connection_string: None,
            output_path: None,
            schemas: vec!["public".to_string()],
            types: std::collections::HashMap::new(),
            exceptions: std::collections::HashMap::new(),
            task_queue: None,
            errors: None,
            infer_view_nullability: true,
            disable_deserialize: Vec::new(),
            queries: None,
            tracing: None,
        };

        // Generate the code
        let generated = pg_fn.to_rust(&types, &config);
        let generated_str = generated.to_string();

        // Verify that tracing::instrument attribute is NOT included
        assert!(
            !generated_str.contains("tracing :: instrument"),
            "Generated code should NOT contain tracing::instrument when disabled"
        );
    }
}
