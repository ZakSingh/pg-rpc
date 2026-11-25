use crate::annotations::{self, Annotations, Site};
use crate::codegen::ToRust;
use crate::codegen::{FunctionName, SchemaName, OID};
use crate::config;
use crate::call_graph::{self, CallTarget};
use crate::constraint_analysis::RelDep;
use crate::exceptions::{get_exceptions_from_deps, PgException};
use crate::ident::{
    dedupe_idents, ident_tokens, sql_to_rs_ident, sql_to_rs_string,
    CaseType,
};
use crate::pg_constraint::Constraint;
use crate::pg_id::PgId;
use crate::pg_type::{unsupported_type, PgType};
use crate::rel_index::RelIndex;
use crate::unified_error::generate_error_enum;
use config::Config;
use itertools::Itertools;
use log::warn;
use pg_query::parse_plpgsql;
use postgres::Row;
use proc_macro2::TokenStream;
use quote::quote;
use regex::Regex;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

// Re-export constraint analysis types for backward compatibility
pub use crate::constraint_analysis::{
    applicable_constraints, extract_queries, get_rel_deps,
};
// Typed-error detection lives with the rest of the exception analysis.
pub use crate::exceptions::extract_custom_errors_from_queries;

const VOID_TYPE_OID: u32 = 2278;

/// How a function's single returned value is named in diagnostics and in
/// the body analysis.
const RETURN_VALUE: &str = "return value";

#[derive(Debug)]
pub struct PgFn {
    pub oid: OID,
    pub schema: SchemaName,
    pub name: FunctionName,
    pub args: Vec<PgArg>,
    pub out_args: Vec<PgArg>,
    pub return_type_oid: OID,
    pub returns_set: bool,
    /// `STRICT`: returns NULL, without running, when any argument is NULL.
    pub is_strict: bool,
    /// The single returned value of a `LANGUAGE sql` function was inferred
    /// NOT NULL from its body (see [`PgFn::infer_result_nullability`]).
    pub return_not_null: bool,
    pub is_procedure: bool,
    pub has_table_params: bool,
    /// `pg_language.lanname`: `sql` or `plpgsql`.
    pub language: String,
    pub definition: String,
    pub comment: Option<String>,
    /// Everything this function can raise: its own `RAISE`s, constraint
    /// violations from its DML, trigger exceptions, and (after
    /// `FunctionIndex::follow_calls`) the same for every function it calls.
    pub exceptions: Vec<PgException>,
    /// Relations this function writes, including those written by the
    /// functions it calls (after `FunctionIndex::follow_calls`). Drives the
    /// `XxxConstraint` variants of the error enum.
    pub rel_deps: Vec<RelDep>,
    /// Functions called from the body, by name as written, before resolution.
    pub callees: BTreeSet<CallTarget>,
}

#[derive(Debug, Clone)]
pub struct PgArg {
    /// The name as declared. An argument declared without one gets the name
    /// Postgres itself would show: `arg<n>` by input position for the Rust
    /// parameter, `column<n>` by output position for a result column.
    pub name: String,
    /// False when `name` was made up, in which case the argument can only be
    /// passed by position.
    pub named: bool,
    pub type_oid: OID,
    pub has_default: bool,
    /// Declared `VARIADIC`: `type_oid` is the array type and the call passes
    /// the whole array with the `VARIADIC` keyword.
    pub variadic: bool,
    /// The SQL type to cast this argument to in a call. Set for the
    /// arguments of an overloaded function, so that the call resolves to the
    /// overload the binding was generated from rather than whichever one
    /// Postgres prefers for untyped parameters.
    pub cast: Option<String>,
    pub nullable: bool,
    /// For an OUT parameter of a SQL function: NULL only because it comes
    /// from the nullable side of an outer join. A `prefix__` group whose
    /// members all have this set becomes `Option<Struct>`.
    pub nullable_due_to_join: bool,
}

/// The OUT parameters a function's comment lists in `@pgrpc_not_null(a, b)`
fn parse_out_param_not_null_annotations(comment: &Option<String>) -> BTreeSet<String> {
    Annotations::of(comment.as_deref()).not_null
}

/// Rust identifiers for a list of SQL argument names, with the conventional
/// `prefixes` (`p_`, `o_`) dropped for readability. A name that would then
/// clash with another argument (`p_id` next to `id`) keeps its prefix, and
/// whatever still clashes after case conversion gets a numeric suffix.
fn arg_idents<'a>(names: impl IntoIterator<Item = &'a str>, prefixes: &[&str]) -> Vec<TokenStream> {
    let names: Vec<&str> = names.into_iter().collect();
    let stripped: Vec<String> = names
        .iter()
        .map(|name| sql_to_rs_string(strip_arg_prefix(name, prefixes), CaseType::Snake))
        .collect();
    let idents = names
        .iter()
        .zip(&stripped)
        .map(|(name, ident)| {
            let clashes = stripped.iter().filter(|other| *other == ident).count() > 1;
            if clashes && strip_arg_prefix(name, prefixes) != *name {
                sql_to_rs_string(name, CaseType::Snake)
            } else {
                ident.clone()
            }
        })
        .collect();
    dedupe_idents(idents)
        .iter()
        .map(|ident| ident_tokens(ident))
        .collect()
}

/// `name` without the first of `prefixes` it starts with, unless that would
/// leave nothing.
fn strip_arg_prefix<'a>(name: &'a str, prefixes: &[&str]) -> &'a str {
    prefixes
        .iter()
        .find_map(|prefix| name.strip_prefix(prefix))
        .filter(|rest| !rest.is_empty())
        .unwrap_or(name)
}

/// Prefixes dropped from input argument names.
const IN_PREFIXES: &[&str] = &["p_"];
/// Prefixes dropped from OUT / INOUT / TABLE column names.
const OUT_PREFIXES: &[&str] = &["o_", "p_"];

impl PgArg {
    /// Returns the Rust name for this argument when used as an OUT parameter field.
    ///
    /// Strips both `o_` (OUT-only convention) and `p_` (INOUT-as-parameter convention)
    /// prefixes so the struct field reads cleanly and matches the Rust function arg
    /// name (which also strips `p_`).
    pub fn rs_out_name(&self) -> TokenStream {
        sql_to_rs_ident(strip_arg_prefix(&self.name, OUT_PREFIXES), CaseType::Snake)
    }

    /// `::type` for an argument that must be cast in the call, else nothing.
    fn cast_suffix(&self) -> String {
        self.cast
            .as_ref()
            .map(|type_name| format!("::{type_name}"))
            .unwrap_or_default()
    }

    /// The parameter type in the generated function's signature.
    fn rs_param_type(&self, types: &BTreeMap<OID, PgType>) -> TokenStream {
        let ty = match types.get(&self.type_oid) {
            Some(ty) => ty.to_rust_param(types),
            None => PgType::Unsupported { name: format!("oid {}", self.type_oid) }.to_rust_ident(types),
        };
        if self.has_default {
            quote! { Option<#ty> }
        } else {
            ty
        }
    }
}

impl PgFn {
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

    /// Generate the function-specific error enum
    pub fn generate_error_enum(
        &self,
        rel_index: &crate::rel_index::RelIndex,
        table_constraints: &BTreeMap<PgId, Vec<Constraint>>,
        config: &Config,
    ) -> TokenStream {
        // Constraints of the tables this function and everything it calls
        // write, keyed by the table the violation is reported on (a DELETE
        // blocked by a foreign key is reported on the referencing table).
        let mut used_constraints: BTreeMap<PgId, Vec<Constraint>> = BTreeMap::new();
        for dep in &self.rel_deps {
            for (table_id, constraint) in applicable_constraints(dep, rel_index) {
                if !table_constraints.contains_key(&table_id) {
                    continue;
                }
                // Merge: the same table may be written more than once
                // (several statements, or caller + callee).
                let entry = used_constraints.entry(table_id).or_default();
                if !entry.iter().any(|c| c.name() == constraint.name()) {
                    entry.push(constraint);
                }
            }
        }

        generate_error_enum(
            &self.error_enum_name(),
            &used_constraints,
            table_constraints,
            &self.exceptions,
            config,
        )
    }

    /// Get all type OIDs in the function signature (args + return type)
    pub fn ty_oids(&self) -> Vec<OID> {
        let mut oids = self.args.iter().map(|a| a.type_oid).collect_vec();
        oids.extend(self.out_args.iter().map(|a| a.type_oid));
        oids.push(self.return_type_oid);
        oids
    }

    /// Why no Rust binding can be generated for this signature, if so: an
    /// argument or the return type is a pg pseudo-type (`anyelement`,
    /// `internal`, …) or a type with no Rust mapping (a range, `interval`, …).
    ///
    /// Pseudo-typed functions are almost always internal helpers (e.g.
    /// `core.raise_error(p_error anyelement)`) nobody expects to call from
    /// Rust, so they are skipped quietly; an unmapped type gets a warning.
    pub fn unrepresentable_type(&self, types: &BTreeMap<OID, PgType>) -> Option<Unrepresentable> {
        self.ty_oids().iter().find_map(|oid| match types.get(oid) {
            Some(PgType::Pseudo(name)) => Some(Unrepresentable::Pseudo(name.clone())),
            _ => unsupported_type(*oid, types).map(Unrepresentable::Unsupported),
        })
    }

    /// What the function's comment is attached to, for checking its
    /// annotations: bare `@pgrpc_not_null` needs a single returned value,
    /// the list form names OUT / TABLE parameters.
    pub fn annotation_problems(&self) -> Vec<String> {
        let Some(comment) = &self.comment else {
            return Vec::new();
        };
        let outs: Vec<&str> = self.out_args.iter().map(|a| a.name.as_str()).collect();
        let returns_value = !self.is_procedure
            && !self.returns_set
            && outs.is_empty()
            && self.return_type_oid != VOID_TYPE_OID;
        annotations::check(comment, Site::Function { returns_value, outs: &outs })
    }

    /// The function body as SQL text; empty if none was found.
    fn body(&self) -> Cow<'_, str> {
        fn_body(&self.definition).unwrap_or_default()
    }

    pub fn is_sql(&self) -> bool {
        self.language == "sql"
    }

    /// Infer what a `LANGUAGE sql` function's result can be NULL in, from
    /// its body: the OUT parameters, or the single returned value.
    ///
    /// A function that does not return a set yields NULL (every OUT column
    /// NULL) when its last statement returns no row, so nothing is inferred
    /// NOT NULL for it unless that statement provably returns exactly one
    /// row; nor when the function is STRICT and can be called with a NULL
    /// argument, which returns NULL without running the body.
    pub fn infer_result_nullability(
        &mut self,
        rel_index: &RelIndex,
        view_nullability_cache: &crate::view_nullability::ViewNullabilityCache,
        composite_index: &crate::composite_index::CompositeIndex,
    ) -> anyhow::Result<()> {
        if !self.is_sql() || self.is_procedure {
            return Ok(());
        }
        let scalar = self.out_args.is_empty();
        if scalar && (self.returns_set || self.return_type_oid == VOID_TYPE_OID) {
            // Rows of a set are never NULL-wrapped; void has no value.
            return Ok(());
        }

        // Extract the function body, and make sure it parses as SQL
        let body = self.body().into_owned();
        let parsed = match pg_query::parse(&body) {
            Ok(parsed) => parsed,
            Err(e) => {
                log::warn!(
                    "Failed to parse SQL function body for nullability analysis (function: {}): {}",
                    self.name,
                    e
                );
                return Ok(()); // Gracefully fall back to default nullable
            }
        };
        let certain_row = self.returns_set || self.always_yields_a_row(&parsed.protobuf, rel_index);

        // Create analyzer
        let mut analyzer = crate::view_nullability::ViewNullabilityAnalyzer::new(
            rel_index,
            view_nullability_cache,
        )
        .with_composite_index(composite_index);

        // The output columns: the OUT parameters, or the one returned value
        let column_names: Vec<String> = if scalar {
            vec![RETURN_VALUE.to_string()]
        } else {
            self.out_args.iter().map(|arg| arg.name.clone()).collect()
        };

        // Analyze the query
        let nullability_map = match analyzer.analyze_function_body(&body, &column_names) {
            Ok(map) => map,
            Err(e) => {
                log::warn!(
                    "Failed to analyze SQL function nullability (function: {}): {}",
                    self.name,
                    e
                );
                return Ok(()); // Gracefully fall back to default nullable
            }
        };
        log::info!(
            "Inferred nullability for SQL function {} (certain row: {}): {:?}",
            self.name,
            certain_row,
            nullability_map
        );
        let owner = format!("function {}.{}", self.schema, self.name);

        if scalar {
            let inferred = certain_row
                && nullability_map.get(RETURN_VALUE).is_some_and(|cn| cn.is_not_null());
            if inferred && Annotations::of(self.comment.as_deref()).not_null_this {
                annotations::warn_redundant(&owner, RETURN_VALUE);
            }
            self.return_not_null = inferred;
            return Ok(());
        }

        // @pgrpc_not_null annotations take precedence over inference
        let not_null_annotations = parse_out_param_not_null_annotations(&self.comment);
        for out_arg in &mut self.out_args {
            let cn = nullability_map.get(&out_arg.name);
            let inferred = certain_row && cn.is_some_and(|cn| cn.is_not_null());

            if not_null_annotations.contains(&out_arg.name) {
                if cn.is_some_and(|cn| cn.nullable_due_to_join) {
                    log::warn!(
                        "{}: @pgrpc_not_null on OUT param `{}`, which comes from \
                         the nullable side of an outer join; rows without a match will fail to decode",
                        owner,
                        out_arg.name
                    );
                }
                if inferred {
                    annotations::warn_redundant(&owner, &out_arg.name);
                }
                out_arg.nullable = false;
                continue;
            }

            if let Some(cn) = cn {
                out_arg.nullable = !inferred;
                out_arg.nullable_due_to_join = cn.nullable_due_to_join;
            }
        }

        Ok(())
    }

    /// Whether a call always produces a row from the body's last statement:
    /// the statement returns exactly one row whatever the data, and the
    /// function is not one that skips its body. A STRICT function returns
    /// NULL when given a NULL argument, which a caller can only do by leaving
    /// out an argument whose default is (or may be) NULL.
    fn always_yields_a_row(&self, parsed: &pg_query::protobuf::ParseResult, rel_index: &RelIndex) -> bool {
        if self.is_strict && self.args.iter().any(|arg| arg.has_default) {
            return false;
        }
        let Some(last) = parsed.stmts.last() else {
            return false;
        };
        let last_only = pg_query::protobuf::ParseResult {
            version: parsed.version,
            stmts: vec![last.clone()],
        };
        crate::cardinality_inference::CardinalityAnalyzer::new(rel_index)
            .infer_parsed(&last_only)
            .is_ok_and(|inference| {
                inference.cardinality == crate::cardinality_inference::InferredCardinality::One
            })
    }
}

/// Why a function gets no Rust binding (see [`PgFn::unrepresentable_type`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unrepresentable {
    Pseudo(String),
    Unsupported(String),
}

impl PgFn {
    pub fn from_row(
        row: Row,
        rel_index: &RelIndex,
        errors_config: Option<&crate::config::ErrorsConfig>,
    ) -> Result<Self, anyhow::Error> {
        let comment: Option<String> = row.try_get("comment")?;
        let language: String = row.try_get("language")?;
        let prokind: i8 = row.try_get("prokind")?;

        // `proargtypes` and the defaults flags cover the input arguments
        // only. When a function has OUT, INOUT, VARIADIC or TABLE arguments
        // `proallargtypes`/`proargmodes` list every argument; otherwise they
        // are NULL and every argument is IN. `proargnames` parallels
        // whichever list applies, with an empty string for an unnamed
        // argument, and is NULL when no argument has a name.
        let input_types: Vec<OID> = row.try_get("arg_oids")?;
        let arg_defaults: Vec<bool> = row.try_get("has_defaults")?;
        let all_types: Vec<OID> = row
            .try_get::<_, Option<Vec<OID>>>("all_arg_types")?
            .unwrap_or(input_types);
        let modes: Vec<i8> = row
            .try_get::<_, Option<Vec<i8>>>("arg_modes")?
            .unwrap_or_else(|| vec![b'i' as i8; all_types.len()]);
        let names: Vec<String> = row
            .try_get::<_, Option<Vec<String>>>("all_arg_names")?
            .unwrap_or_default();

        // Parse @pgrpc_not_null annotations for OUT parameters
        let not_null_params = parse_out_param_not_null_annotations(&comment);

        let (mut args, out_args, has_table_params) =
            parse_args(&all_types, &modes, &names, &arg_defaults, &not_null_params);

        if row.try_get::<_, i64>("overload_count")? > 1 {
            let type_names: Vec<String> = row.try_get("arg_type_names")?;
            for (arg, type_name) in args.iter_mut().zip(type_names) {
                arg.cast = Some(type_name);
            }
        }

        let definition = row.try_get::<_, String>("function_definition")?;
        let name: String = row.try_get("function_name")?;
        let schema: String = row.try_get("schema_name")?;

        let (exceptions, rel_deps, callees) = analyze_body(
            &schema,
            &name,
            &language,
            &definition,
            rel_index,
            errors_config,
        )?;

        Ok(Self {
            oid: row.try_get("oid")?,
            name,
            schema,
            definition,
            returns_set: row.try_get("returns_set")?,
            is_strict: row.try_get("is_strict")?,
            return_not_null: false,
            is_procedure: prokind == b'p' as i8, // 'p' for procedure, 'f' for function
            has_table_params,
            language,
            comment,
            args,
            out_args,
            return_type_oid: row.try_get("return_type")?,
            exceptions,
            rel_deps,
            callees,
        })
    }
}

/// Analyze a function body: what it can raise, which relations it writes,
/// and which functions it calls. A body that libpg_query cannot parse is a
/// warning, not a failure: the function still gets a binding, its error enum
/// just has no specific variants.
fn analyze_body(
    schema: &str,
    name: &str,
    language: &str,
    definition: &str,
    rel_index: &RelIndex,
    errors_config: Option<&crate::config::ErrorsConfig>,
) -> anyhow::Result<(Vec<PgException>, Vec<RelDep>, BTreeSet<CallTarget>)> {
    let scope = format!("{schema}.{name}");
    Ok(match language {
        "plpgsql" => match parse_plpgsql(definition) {
            Ok(parsed) => {
                let queries = extract_queries(&parsed, &scope);
                let rel_deps = get_rel_deps(&queries, rel_index, &scope);
                let exceptions = get_exceptions_from_deps(
                    &parsed,
                    &rel_deps,
                    rel_index,
                    errors_config,
                    &scope,
                )?;
                let callees = call_graph::plpgsql_callees(&parsed);
                (exceptions, rel_deps, callees)
            }
            Err(e) => {
                warn!(
                    "Failed to parse PL/pgSQL function {}.{}: {}. Its error enum will only have the `Other` variant.",
                    schema, name, e
                );
                Default::default()
            }
        },
        "sql" => match fn_body(definition).map(|body| pg_query::parse(&body)) {
            Some(Ok(parsed)) => {
                let callees = call_graph::sql_callees(&parsed);
                let queries = vec![parsed];
                let rel_deps = get_rel_deps(&queries, rel_index, &scope);

                let mut exceptions: Vec<PgException> = Vec::new();

                // Typed errors raised through the configured raise function
                if let Some(cfg) = errors_config {
                    exceptions.extend(
                        extract_custom_errors_from_queries(&queries, cfg)
                            .into_iter()
                            .map(PgException::CustomError),
                    );
                }

                (exceptions, rel_deps, callees)
            }
            Some(Err(e)) => {
                warn!(
                    "Failed to parse SQL function body of {}.{}: {}",
                    schema, name, e
                );
                Default::default()
            }
            None => {
                warn!(
                    "Could not find the body of SQL function {}.{}",
                    schema, name
                );
                Default::default()
            }
        },
        _ => {
            // Other languages not supported yet
            Default::default()
        }
    })
}

/// Splits a function's declared arguments into its inputs (IN, INOUT,
/// VARIADIC) and its result columns (OUT, INOUT, TABLE), and says whether it
/// is a `RETURNS TABLE` function.
///
/// `types`, `modes` and `names` are parallel, one entry per declared argument
/// (`names` may be shorter, or empty); `defaults` is per input argument.
fn parse_args(
    types: &[OID],
    modes: &[i8],
    names: &[String],
    defaults: &[bool],
    not_null_params: &BTreeSet<String>,
) -> (Vec<PgArg>, Vec<PgArg>, bool) {
    let mut args: Vec<PgArg> = Vec::new();
    let mut out_args: Vec<PgArg> = Vec::new();
    let mut has_table_params = false;

    for (i, (&type_oid, &mode)) in types.iter().zip(modes).enumerate() {
        let declared = names.get(i).filter(|name| !name.is_empty());
        let mode = mode as u8;

        if matches!(mode, b'i' | b'b' | b'v') {
            let position = args.len() + 1;
            args.push(PgArg {
                name: declared
                    .cloned()
                    .unwrap_or_else(|| format!("arg{position}")),
                named: declared.is_some(),
                type_oid,
                has_default: defaults.get(position - 1).copied().unwrap_or(false),
                variadic: mode == b'v',
                cast: None,
                nullable: false, // IN parameters are never nullable in function signatures
                nullable_due_to_join: false,
            });
        }

        if matches!(mode, b'o' | b'b' | b't') {
            has_table_params |= mode == b't';
            // Postgres names an unnamed result column `column<n>`.
            let name = declared
                .cloned()
                .unwrap_or_else(|| format!("column{}", out_args.len() + 1));
            out_args.push(PgArg {
                nullable: !not_null_params.contains(&name),
                name,
                named: declared.is_some(),
                type_oid,
                has_default: false,
                variadic: false,
                cast: None,
                nullable_due_to_join: false,
            });
        }
    }

    // A defaulted argument is omitted by leaving it out of a call written in
    // named notation, which an unnamed argument cannot take part in. It, and
    // every argument before it, is therefore always passed by position.
    if let Some(last) = args.iter().rposition(|a| a.has_default && !a.named) {
        for arg in &mut args[..=last] {
            arg.has_default = false;
        }
    }

    (args, out_args, has_table_params)
}

/// The body of a function from its `pg_get_functiondef` definition, as SQL
/// text: what is between the dollar quotes, or the statements of a
/// SQL-standard body (`BEGIN ATOMIC ... END`, or `RETURN expr` as `SELECT expr`).
fn fn_body(definition: &str) -> Option<Cow<'_, str>> {
    let dollar = quote_ind(definition);
    // `pg_get_functiondef` indents every header line after the first and
    // starts the body at the beginning of a line.
    let standard = ["\nBEGIN ATOMIC", "\nRETURN "]
        .iter()
        .filter_map(|marker| definition.find(marker))
        .min()
        .map(|at| at + 1);

    match (dollar, standard) {
        (Some((tag, start)), standard)
            if standard.is_none_or(|standard| start - tag.len() < standard) =>
        {
            let end = definition[start..].find(tag)? + start;
            Some(Cow::Borrowed(&definition[start..end]))
        }
        (_, Some(start)) => {
            let body = definition[start..].trim();
            if let Some(statements) = body.strip_prefix("BEGIN ATOMIC") {
                let statements = statements.trim_end();
                let statements = statements.strip_suffix("END").unwrap_or(statements);
                Some(Cow::Borrowed(statements.trim()))
            } else {
                let expr = body.strip_prefix("RETURN")?;
                Some(Cow::Owned(format!("SELECT {}", expr.trim())))
            }
        }
        _ => None,
    }
}

/// How many rows a call yields, which picks the client method and the
/// wrapper around the row type.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cardinality {
    /// Exactly one row: `T`.
    One,
    /// A set-returning function: `Vec<T>`.
    Many,
}

impl ToRust for PgFn {
    fn to_rust(&self, types: &BTreeMap<OID, PgType>, config: &Config) -> TokenStream {
        // Skip functions whose signature involves a type we can't represent
        // in Rust.
        match self.unrepresentable_type(types) {
            Some(Unrepresentable::Pseudo(_)) => return TokenStream::new(),
            Some(Unrepresentable::Unsupported(type_name)) => {
                warn!(
                    "Skipping function {}.{}: its signature uses `{}`, which has no Rust mapping",
                    self.schema, self.name, type_name
                );
                return TokenStream::new();
            }
            None => {}
        }

        // Use function-specific error type
        let error_enum_name = self.error_enum_name();
        let err_type = quote! { #error_enum_name };
        let fn_name_str = &self.name;

        let cardinality = if self.is_procedure || !self.returns_set {
            Cardinality::One
        } else {
            Cardinality::Many
        };

        let (row_type, decode_row) = self.result_row(types, &err_type);

        let return_type = match cardinality {
            Cardinality::One => row_type,
            Cardinality::Many => quote! { Vec<#row_type> },
        };

        let query = run_statement(cardinality, decode_row, &err_type);

        let arg_names = arg_idents(self.args.iter().map(|a| a.name.as_str()), IN_PREFIXES);

        let fn_body = self.call_body(types, &arg_names, &err_type, &query);

        // Build fn signature
        let rs_fn_name = self.rs_name();
        let args: Vec<TokenStream> = self
            .args
            .iter()
            .zip(&arg_names)
            .map(|(arg, name)| {
                let ty = arg.rs_param_type(types);
                quote! { #name: #ty }
            })
            .collect();

        let doc_comment = self.doc_comment();
        let comment_macro = quote! { #[doc=#doc_comment] };

        let struct_def = self.result_struct(types);

        // Check if tracing is enabled
        let tracing_enabled = config.tracing.as_ref().map(|t| t.enabled).unwrap_or(false);
        let schema_str = &self.schema;

        // Generate the tracing::instrument attribute if enabled
        let tracing_attr = if tracing_enabled {
            // Build the span name as "schema.function_name"
            let span_name = format!("{}.{}", schema_str, fn_name_str);
            quote! {
                #[tracing::instrument(name = #span_name, skip_all)]
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

impl PgFn {
    /// What one result row is, and how it is read (`None` when there is
    /// nothing to read).
    fn result_row(
        &self,
        types: &BTreeMap<OID, PgType>,
        err_type: &TokenStream,
    ) -> (TokenStream, Option<TokenStream>) {
        // Attribution for scalar decode failures: the function, and the
        // single OUT parameter or the (unnamed) return value.
        let item = format!("{}.{}", self.schema, self.name);
        let result_col = self
            .out_args
            .first()
            .map(|a| a.name.clone())
            .unwrap_or_else(|| "result".to_string());
        let decode = crate::codegen::decode_err(&item, &result_col, 0);
        let decode_struct = quote! { row.try_into().map_err(#err_type::from) };
        let decode_scalar = quote! { row.try_get(0).map_err(#decode).map_err(#err_type::from) };

        // What one result row is, and how it is read: nothing, a generated
        // struct (TABLE columns, several OUT parameters, or the attributes
        // of a composite selected as columns), or one value.
        if self.has_table_params {
            (self.return_table_struct_name(), Some(decode_struct))
        } else if self.out_args.len() > 1 {
            (self.out_params_struct_name(), Some(decode_struct))
        } else if self.is_procedure && self.out_args.is_empty() {
            (quote! { () }, None)
        } else if self.is_procedure {
            // Single OUT parameter - return its type directly
            let out_type = crate::pg_type::rust_type_of(self.out_args[0].type_oid, types);
            let out_type = if self.out_args[0].nullable {
                quote! { Option<#out_type> }
            } else {
                out_type
            };
            (out_type, Some(decode_scalar))
        } else if self.return_type_oid == VOID_TYPE_OID {
            (quote! { () }, None)
        } else {
            let inner_ty = crate::pg_type::rust_type_of(self.return_type_oid, types);
            let is_composite = matches!(
                types.get(&self.return_type_oid),
                Some(PgType::Composite { .. })
            );
            if self.returns_set {
                // `select <attributes> from f()` spreads a composite over
                // columns; anything else arrives as the one column.
                let decode_row = if is_composite { decode_struct } else { decode_scalar };
                (inner_ty, Some(decode_row))
            } else {
                let return_not_null = self.return_not_null
                    || Annotations::of(self.comment.as_deref()).not_null_this;
                let ty = if return_not_null {
                    inner_ty
                } else {
                    quote! { Option<#inner_ty> }
                };
                (ty, Some(decode_scalar))
            }
        }
    }

    /// The body of the generated function: the parameter list, the call
    /// statement, its preparation, and `query`, which runs it.
    fn call_body(
        &self,
        types: &BTreeMap<OID, PgType>,
        arg_names: &[TokenStream],
        err_type: &TokenStream,
        query: &TokenStream,
    ) -> TokenStream {
        // Arguments with defaults are optional and passed by name;
        // the rest are passed by position, in declaration order.
        type NamedArgs<'a> = Vec<(&'a PgArg, &'a TokenStream)>;
        let (opt_args, req_args): (NamedArgs, NamedArgs) =
            self.args.iter().zip(arg_names).partition(|(a, _)| a.has_default);

        let required_query_params = req_args
            .iter()
            .enumerate()
            .map(|(i, (arg, _))| {
                format!(
                    "{}${}{}",
                    if arg.variadic { "VARIADIC " } else { "" },
                    i + 1,
                    arg.cast_suffix()
                )
            })
            .join(", ");

        let req_arg_refs: Vec<TokenStream> =
            req_args.iter().map(|(_, name)| quote! { &#name }).collect();

        // Quoted, so a mixed-case or reserved-word name still resolves.
        let callee = format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name));
        let query_string = if self.is_procedure {
            format!("CALL {callee}")
        } else if self.returns_set || self.out_args.len() > 1 {
            format!("select {} from {callee}", self.result_column_list(types))
        } else {
            format!("select {callee}")
        };

        // Cache the prepared statement on the per-connection StatementCache.
        // The SQL string is built at runtime (varies with which optional params
        // were passed), but each distinct SQL still gets its own cache entry,
        // so callers see prepare-once behaviour per arg-shape per connection.
        //
        // We use prepare_cached (untyped) rather than prepare_typed_cached:
        // function args frequently involve user-defined composites/domains/
        // enums whose OIDs aren't builtins, and the describe round-trip saved
        // by prepare_typed_cached is one-time per cache miss anyway.
        let prepare_step = quote! {
            let stmt = client.prepare_cached(&query).await.map_err(#err_type::from)?;
        };

        if !opt_args.is_empty() {
            // Generate code for functions with optional parameters using string building
            let opt_param_handling: Vec<TokenStream> = opt_args
                .iter()
                .map(|(arg, arg_name)| {
                    // `"name" := $n`, or `VARIADIC "name" := $n` to pass
                    // the variadic array whole.
                    let assignment = format!(
                        "{}{} := $",
                        if arg.variadic { "VARIADIC " } else { "" },
                        quote_ident(&arg.name)
                    );
                    let cast = arg.cast_suffix();
                    // as_ref() avoids moving the value out of the Option
                    quote! {
                        if let Some(val) = #arg_name.as_ref() {
                            params.push(val);
                            optional_parts.push(format!("{}{}{}", #assignment, params.len(), #cast));
                        }
                    }
                })
                .collect();

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

                #prepare_step

                #query
            }
        } else {
            // Simple case with only required parameters
            quote! {
                let params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![#(#req_arg_refs),*];
                let query = format!("{}({})", #query_string, #required_query_params);

                #prepare_step

                #query
            }
        }
    }

    /// The generated function's doc comment.
    fn doc_comment(&self) -> String {
        match &self.comment {
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
        }
    }

    /// The struct a row is decoded into, for a function that has one.
    fn result_struct(&self, types: &BTreeMap<OID, PgType>) -> TokenStream {
        // Generate struct for RETURN TABLE functions or procedures/functions with multiple OUT parameters
        let is_return_table = self.has_table_params;
        if is_return_table || self.out_args.len() > 1 {
            let struct_name = if is_return_table {
                self.return_table_struct_name()
            } else {
                self.out_params_struct_name()
            };

            // Check if any out_args use double-underscore naming for grouping
            let has_grouped = self
                .out_args
                .iter()
                .any(|a| a.name.strip_prefix("o_").unwrap_or(&a.name).contains("__"));

            self.generate_out_struct(&struct_name, types, has_grouped)
        } else {
            quote! {}
        }
    }
}

/// Runs the prepared statement and decodes its result for the cardinality.
fn run_statement(
    cardinality: Cardinality,
    decode_row: Option<TokenStream>,
    err_type: &TokenStream,
) -> TokenStream {
    match (cardinality, decode_row) {
        (Cardinality::One, None) => quote! {
            client
                .execute(&stmt, &params)
                .await
                .map(|_| ())
                .map_err(#err_type::from)
        },
        (Cardinality::Many, None) => quote! {
            client
                .query(&stmt, &params)
                .await
                .map(|rows| vec![(); rows.len()])
                .map_err(#err_type::from)
        },
        (Cardinality::One, Some(decode_row)) => quote! {
            client
                .query_one(&stmt, &params)
                .await
                .map_err(#err_type::from)
                .and_then(|row| #decode_row)
        },
        (Cardinality::Many, Some(decode_row)) => quote! {
            client
                .query(&stmt, &params)
                .await
                .map_err(#err_type::from)
                .and_then(|rows| {
                    rows.into_iter()
                        .map(|row| #decode_row)
                        .collect()
                })
        },
    }
}

/// A SQL identifier, double-quoted so it is taken exactly as the catalog
/// spells it.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

impl PgFn {
    /// The select list for a call that returns rows: the columns known at
    /// generation time, by name.
    ///
    /// Naming them instead of `select *` keeps the statement's result type
    /// fixed when a column is later appended to the row type. A client
    /// generated before the change then never receives a column it does not
    /// know, and its cached prepared statement does not fail with "cached
    /// plan must not change result type".
    ///
    /// Falls back to `*` when the result is a single anonymous column.
    fn result_column_list(&self, types: &BTreeMap<OID, PgType>) -> String {
        let names: Vec<&str> = if !self.out_args.is_empty() {
            self.out_args.iter().map(|a| a.name.as_str()).collect()
        } else {
            match types.get(&self.return_type_oid) {
                Some(PgType::Composite { fields, .. }) => {
                    fields.iter().map(|f| f.name.as_str()).collect()
                }
                _ => Vec::new(),
            }
        };
        if names.is_empty() || names.iter().any(|n| n.is_empty()) {
            return "*".to_string();
        }
        names.iter().map(|n| quote_ident(n)).join(", ")
    }

    /// The struct for OUT parameters / RETURN TABLE columns, read by position.
    ///
    /// With `grouped`, `prefix__field` columns become nested structs. The
    /// `o_`/`p_` prefixes are SQL-side conventions for OUT/INOUT parameters
    /// and leak into neither the Rust field names nor the serde JSON names.
    fn generate_out_struct(
        &self,
        struct_name: &TokenStream,
        types: &BTreeMap<OID, PgType>,
        grouped: bool,
    ) -> TokenStream {
        use crate::row_struct::{flat_layout, grouped_layout, Decode, FieldParts, RowShape};

        let args = &self.out_args;
        let clean: Vec<&str> = args
            .iter()
            .map(|a| strip_arg_prefix(&a.name, OUT_PREFIXES))
            .collect();
        // What a field is called when nothing forces another name. A grouped
        // struct's own fields have the prefix stripped once more.
        let natural = |place: crate::row_struct::Place| match place.member {
            Some(member) => sql_to_rs_string(member, CaseType::Snake),
            None if grouped => sql_to_rs_string(
                strip_arg_prefix(clean[place.index], OUT_PREFIXES),
                CaseType::Snake,
            ),
            None => sql_to_rs_string(clean[place.index], CaseType::Snake),
        };
        let flat_idents = arg_idents(args.iter().map(|a| a.name.as_str()), OUT_PREFIXES);

        let layout = if grouped {
            grouped_layout(
                clean
                    .iter()
                    .zip(args)
                    .map(|(name, arg)| (*name, arg.nullable_due_to_join)),
            )
        } else {
            flat_layout(args.len())
        };

        let shape = RowShape::build(
            struct_name.clone(),
            &struct_name.to_string(),
            layout,
            |place| {
                if grouped {
                    natural(place)
                } else {
                    // A name that would clash once stripped keeps its prefix.
                    flat_idents[place.index].to_string()
                }
            },
            |place, ident| {
                let arg = &args[place.index];
                let sql_name = place.member.unwrap_or(clean[place.index]);
                // Inside a group a field has its natural nullability: on the
                // base column or because of the join.
                let nullable =
                    arg.nullable || (place.member.is_some() && arg.nullable_due_to_join);

                // A field that kept its prefix or got a suffix to stay
                // distinct serializes under its Rust name.
                let rs_name = ident.to_string();
                let needs_rename = rs_name != sql_name && rs_name == natural(place);
                let attrs = match (needs_rename, nullable) {
                    (true, true) => quote! { #[serde(rename = #sql_name, default)] },
                    (true, false) => quote! { #[serde(rename = #sql_name)] },
                    (false, true) => quote! { #[serde(default)] },
                    (false, false) => quote! {},
                };

                let base_type = crate::pg_type::rust_type_of(arg.type_oid, types);
                FieldParts {
                    column: arg.name.clone(),
                    label: if grouped { sql_name.to_string() } else { arg.name.clone() },
                    ty: if nullable {
                        quote! { Option<#base_type> }
                    } else {
                        base_type
                    },
                    attrs,
                }
            },
        );

        let derives = quote! { #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)] };
        let structs = shape.emit_structs(&derives, &derives);
        let decoder = shape.emit_decoder(Decode::Position);
        quote! {
            #structs

            #decoder
        }
    }
}

/// The first dollar-quote tag (`$$`, `$function$`) and where the text after
/// it starts. Only what Postgres accepts as a tag matches, so a `$user` in a
/// `SET search_path` clause is not mistaken for one.
fn quote_ind(src: &str) -> Option<(&str, usize)> {
    static TAG: std::sync::LazyLock<Regex> =
        std::sync::LazyLock::new(|| Regex::new(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$").unwrap());
    TAG.find(src).map(|tag| (tag.as_str(), tag.end()))
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

        let queries = extract_queries(&f, "test");
        let rels = get_rel_deps(&queries, &rel_index, "test");
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
            is_strict: false,
            return_not_null: false,
            is_procedure: false,
            has_table_params: false,
            language: "sql".to_string(),
            return_type_oid: 25, // TEXT OID
            comment: None,
            definition: "".to_string(),
            exceptions: vec![],
            rel_deps: vec![],
            callees: Default::default(),
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
            exceptions: std::collections::BTreeMap::new(),
            task_queue: None,
            errors: None,
            infer_view_nullability: true,
            disable_deserialize: Vec::new(),
            strict_domains: Vec::new(),
            builders: Vec::new(),
            queries: None,
            tracing: None,
        };

        // Generate the code
        let generated = pg_fn.to_rust(&types, &config);
        let generated_str = generated.to_string();

        // Verify that for primitive SETOF return types, we use row.try_get(0),
        // attributing a decode failure to the function's result column.
        assert!(
            generated_str.contains(
                "rows . into_iter () . map (| row | row . try_get (0) . map_err (super :: errors :: DecodeError :: at ("
            ),
            "{generated_str}"
        );
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
            is_strict: false,
            return_not_null: false,
            is_procedure: false,
            has_table_params: false,
            language: "sql".to_string(),
            return_type_oid: 1000, // Custom composite type OID
            comment: None,
            definition: "".to_string(),
            exceptions: vec![],
            rel_deps: vec![],
            callees: Default::default(),
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
                        nullable_due_to_join: false,
                        comment: None,
                    },
                    PgField {
                        name: "name".to_string(),
                        type_oid: 25, // TEXT
                        nullable: true,
                        nullable_due_to_join: false,
                        comment: None,
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
            exceptions: std::collections::BTreeMap::new(),
            task_queue: None,
            errors: None,
            infer_view_nullability: true,
            disable_deserialize: Vec::new(),
            strict_domains: Vec::new(),
            builders: Vec::new(),
            queries: None,
            tracing: None,
        };

        // Generate the code
        let generated = pg_fn.to_rust(&types, &config);
        let generated_str = generated.to_string();

        // Verify that for composite SETOF return types, we convert each row
        assert!(
            generated_str.contains("rows . into_iter () . map (| row | row . try_into () . map_err ("),
            "{generated_str}"
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
            is_strict: false,
            return_not_null: false,
            is_procedure: false,
            has_table_params: false,
            language: "sql".to_string(),
            return_type_oid: 25, // TEXT OID
            comment: None,
            definition: "".to_string(),
            exceptions: vec![],
            rel_deps: vec![],
            callees: Default::default(),
        };

        let mut types = BTreeMap::new();
        types.insert(25, PgType::Text);

        // Create config with tracing enabled
        let config = crate::config::Config {
            connection_string: None,
            output_path: None,
            schemas: vec!["public".to_string()],
            types: std::collections::HashMap::new(),
            exceptions: std::collections::BTreeMap::new(),
            task_queue: None,
            errors: None,
            infer_view_nullability: true,
            disable_deserialize: Vec::new(),
            strict_domains: Vec::new(),
            builders: Vec::new(),
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
            generated_str.contains("skip_all"),
            "Span must not record any argument: parameters can carry secrets"
        );
        assert!(
            !generated_str.contains("err (Debug)"),
            "Should not include err(Debug)"
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
            is_strict: false,
            return_not_null: false,
            is_procedure: false,
            has_table_params: false,
            language: "sql".to_string(),
            return_type_oid: 25, // TEXT OID
            comment: None,
            definition: "".to_string(),
            exceptions: vec![],
            rel_deps: vec![],
            callees: Default::default(),
        };

        let mut types = BTreeMap::new();
        types.insert(25, PgType::Text);

        // Create config with tracing disabled (default)
        let config = crate::config::Config {
            connection_string: None,
            output_path: None,
            schemas: vec!["public".to_string()],
            types: std::collections::HashMap::new(),
            exceptions: std::collections::BTreeMap::new(),
            task_queue: None,
            errors: None,
            infer_view_nullability: true,
            disable_deserialize: Vec::new(),
            strict_domains: Vec::new(),
            builders: Vec::new(),
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

    #[test]
    fn unnamed_arguments_get_positional_names() {
        use super::parse_args;
        let none = Default::default();

        // f(int, text): proargnames is NULL, no modes.
        let (args, out_args, table) =
            parse_args(&[23, 25], &[b'i' as i8; 2], &[], &[false, false], &none);
        let names: Vec<&str> = args.iter().map(|a| a.name.as_str()).collect();
        assert_eq!(names, ["arg1", "arg2"]);
        assert!(args.iter().all(|a| !a.named && !a.has_default));
        assert!(out_args.is_empty() && !table);

        // f(p_a int, int, OUT int, OUT o_b text): partly named.
        let modes = [b'i', b'i', b'o', b'o'].map(|m| m as i8);
        let arg_names = ["p_a", "", "", "o_b"].map(String::from);
        let (args, out_args, _) =
            parse_args(&[23, 23, 23, 25], &modes, &arg_names, &[false, false], &none);
        let names: Vec<&str> = args.iter().map(|a| a.name.as_str()).collect();
        assert_eq!(names, ["p_a", "arg2"]);
        let out_names: Vec<&str> = out_args.iter().map(|a| a.name.as_str()).collect();
        assert_eq!(out_names, ["column1", "o_b"]);
    }

    #[test]
    fn variadic_and_unnamed_defaults() {
        use super::parse_args;
        let none = Default::default();

        // f(p_a int, VARIADIC p_rest int[] DEFAULT '{}')
        let modes = [b'i', b'v'].map(|m| m as i8);
        let arg_names = ["p_a", "p_rest"].map(String::from);
        let (args, _, _) = parse_args(&[23, 1007], &modes, &arg_names, &[false, true], &none);
        assert!(!args[0].variadic && args[1].variadic);
        assert!(args[1].has_default);

        // f(p_a int DEFAULT 1, int DEFAULT 2, p_c int DEFAULT 3): the unnamed
        // default cannot be skipped by name, so it and `p_a` before it are
        // required; `p_c` stays optional.
        let arg_names = ["p_a", "", "p_c"].map(String::from);
        let (args, _, _) = parse_args(
            &[23, 23, 23],
            &[b'i' as i8; 3],
            &arg_names,
            &[true, true, true],
            &none,
        );
        let defaults: Vec<bool> = args.iter().map(|a| a.has_default).collect();
        assert_eq!(defaults, [false, false, true]);
    }

    #[test]
    fn argument_idents_stay_distinct() {
        use super::{arg_idents, IN_PREFIXES, OUT_PREFIXES};
        let idents = |names: &[&str], prefixes: &[&str]| -> Vec<String> {
            arg_idents(names.iter().copied(), prefixes)
                .iter()
                .map(|t| t.to_string())
                .collect()
        };
        assert_eq!(idents(&["p_id", "p_name"], IN_PREFIXES), ["id", "name"]);
        assert_eq!(idents(&["p_id", "id"], IN_PREFIXES), ["p_id", "id"]);
        assert_eq!(idents(&["p_", "self", "type"], IN_PREFIXES), ["p", "self_", "r#type"]);
        assert_eq!(idents(&["o_id", "p_id", "id"], OUT_PREFIXES), ["o_id", "p_id", "id"]);
        assert_eq!(idents(&["a-b", "a_b"], IN_PREFIXES), ["a_b", "a_b2"]);
    }

    #[test]
    fn function_bodies_dollar_quoted_and_sql_standard() {
        use super::fn_body;

        let dollar = "CREATE OR REPLACE FUNCTION public.f(a integer)\n RETURNS integer\n LANGUAGE sql\n SET search_path TO \"$user\", 'public'\nAS $function$ select a + 1 $function$\n";
        assert_eq!(fn_body(dollar).as_deref(), Some(" select a + 1 "));

        let atomic = "CREATE OR REPLACE FUNCTION public.f(a integer)\n RETURNS integer\n LANGUAGE sql\nBEGIN ATOMIC\n INSERT INTO t (x) VALUES (a);\n SELECT $$END$$;\nEND\n";
        assert_eq!(
            fn_body(atomic).as_deref(),
            Some("INSERT INTO t (x) VALUES (a);\n SELECT $$END$$;")
        );

        let ret = "CREATE OR REPLACE FUNCTION public.f(a integer)\n RETURNS integer\n LANGUAGE sql\nRETURN (a + 1)\n";
        assert_eq!(fn_body(ret).as_deref(), Some("SELECT (a + 1)"));

        // A plpgsql body is not mistaken for a SQL-standard one.
        let plpgsql = "CREATE OR REPLACE FUNCTION public.f()\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$\nBEGIN\nRETURN 1;\nEND $function$\n";
        assert_eq!(fn_body(plpgsql).as_deref(), Some("\nBEGIN\nRETURN 1;\nEND "));

        assert_eq!(fn_body("CREATE FUNCTION f() RETURNS int LANGUAGE c"), None);
    }
}
