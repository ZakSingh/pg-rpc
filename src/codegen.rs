use crate::annotations::{self, Site};
use crate::config::Config;
use crate::exceptions::PgException;
use crate::fn_index::FunctionIndex;
use crate::ident::schema_mod_ident;
use crate::pg_fn::PgFn;
use crate::pg_type::{field_serde_attrs, unsupported_type, PgType};
use crate::rel_index::RelIndex;
use crate::ty_index::TypeIndex;
use crate::unified_error;
/// Postgres object ID
/// Uniquely identifies database objects
#[allow(clippy::upper_case_acronyms)] // spelled as Postgres spells it
pub type OID = u32;

/// Postgres schema identifier
pub type SchemaName = String;

/// Postgres function identifier
pub type FunctionName = String;
use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::quote;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

pub trait ToRust {
    fn to_rust(&self, types: &BTreeMap<OID, PgType>, config: &Config) -> TokenStream;
}

/// Module of the shared serde codecs for date and timestamp fields.
pub const DATETIME_SERDE_MODULE: &str = "datetime_serde";
/// Module of the shared `TsVector` / `TsQuery` types.
pub const TSVECTOR_MODULE: &str = "tsvector";

/// `pub use` of the shared `date_serde` and `timestamp_serde` codecs, for a
/// generated module whose fields name them in `#[serde(with = "date_serde")]`.
/// They used to be defined in each such module, so this also keeps
/// `<module>::date_serde` resolving.
pub fn datetime_serde_reexport() -> TokenStream {
    quote! {
        pub use super::datetime_serde::{date_serde, timestamp_serde};
    }
}

/// Codegen for the shared `datetime_serde` module: the `date_serde` and
/// `timestamp_serde` codecs for `time::Date` and `time::PrimitiveDateTime`.
fn generate_datetime_serde_module() -> TokenStream {
    quote! {
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

        /// Custom serde module for time::PrimitiveDateTime (`timestamp`
        /// without time zone): ISO 8601 without an offset,
        /// `YYYY-MM-DDTHH:MM:SS.fff`. Fractional seconds are optional on input.
        pub mod timestamp_serde {
            use serde::{self, Deserialize, Deserializer, Serializer};
            use time::PrimitiveDateTime;

            const FORMAT: &[time::format_description::FormatItem] = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]");
            const WHOLE_SECONDS: &[time::format_description::FormatItem] = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");

            fn parse<E: serde::de::Error>(s: &str) -> Result<PrimitiveDateTime, E> {
                PrimitiveDateTime::parse(s, &FORMAT)
                    .or_else(|_| PrimitiveDateTime::parse(s, &WHOLE_SECONDS))
                    .map_err(E::custom)
            }

            pub fn serialize<S>(value: &PrimitiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let s = value.format(&FORMAT).map_err(serde::ser::Error::custom)?;
                serializer.serialize_str(&s)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<PrimitiveDateTime, D::Error>
            where
                D: Deserializer<'de>,
            {
                let s = String::deserialize(deserializer)?;
                parse(&s)
            }

            pub mod option {
                use serde::{Deserialize, Deserializer, Serializer};
                use time::PrimitiveDateTime;

                pub fn serialize<S>(value: &Option<PrimitiveDateTime>, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: Serializer,
                {
                    match value {
                        Some(v) => {
                            let s = v.format(&super::FORMAT).map_err(serde::ser::Error::custom)?;
                            serializer.serialize_some(&s)
                        }
                        None => serializer.serialize_none(),
                    }
                }

                pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<PrimitiveDateTime>, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    let opt = Option::<String>::deserialize(deserializer)?;
                    opt.map(|s| super::parse(&s)).transpose()
                }
            }
        }
    }
}

/// Codegen for the shared `tsvector` module. It is this crate's own
/// `tsvector` module, verbatim (minus its tests), so the two cannot drift.
fn generate_tsvector_module() -> TokenStream {
    let source = include_str!("tsvector.rs");
    let source = source.split("#[cfg(test)]").next().unwrap_or(source);
    source.parse().expect("src/tsvector.rs is valid Rust")
}

/// `super::errors::DecodeError::at(item, column, index)`: the `map_err`
/// closure that attributes a `try_get` failure to one column.
pub(crate) fn decode_err(item: &str, column: &str, index: usize) -> TokenStream {
    quote! { super::errors::DecodeError::at(#item, #column, #index) }
}

/// Lint suppressions prepended to every file `codegen_split` generates.
const WARNING_IGNORES: &str = "#![allow(dead_code)]\n#![allow(unused_variables)]\n#![allow(unused_imports)]\n#![allow(unused_mut)]\n#![allow(non_snake_case)]\n\n";

/// Check every `@pgrpc_*` annotation the run has read: on all indexed
/// functions (codegen schema or not, so a stray tag on a helper is caught),
/// on the types being generated and their columns, and on constraints.
fn check_annotations(
    fn_index: &FunctionIndex,
    ty_index: &TypeIndex,
    rel_index: &RelIndex,
    config: &Config,
) -> anyhow::Result<()> {
    let mut problems: Vec<String> = Vec::new();

    for pg_fn in fn_index.values() {
        for problem in pg_fn.annotation_problems() {
            problems.push(format!("function {}.{}: {}", pg_fn.schema, pg_fn.name, problem));
        }
    }

    let errors_schema = config.errors.as_ref().map(|e| e.schema.as_str());
    for pg_type in ty_index.values() {
        match pg_type {
            PgType::Composite { schema, name, fields, comment, relkind, .. } => {
                let children: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
                if let Some(comment) = comment {
                    let site = Site::Type {
                        children: &children,
                        // A standalone composite type: nothing to infer from.
                        all: relkind.as_deref().is_none_or(|k| k == "c"),
                        message: errors_schema == Some(schema.as_str()),
                    };
                    for problem in annotations::check(comment, site) {
                        problems.push(format!("{schema}.{name}: {problem}"));
                    }
                }
                for field in fields {
                    if let Some(comment) = &field.comment {
                        for problem in annotations::check(comment, Site::Column) {
                            problems.push(format!("column {schema}.{name}.{}: {problem}", field.name));
                        }
                    }
                }
            }
            PgType::Domain { schema, name, comment: Some(comment), .. } => {
                for problem in annotations::check(comment, Site::Other("a domain")) {
                    problems.push(format!("domain {schema}.{name}: {problem}"));
                }
            }
            PgType::Enum { schema, name, comment: Some(comment), .. } => {
                for problem in annotations::check(comment, Site::Other("an enum")) {
                    problems.push(format!("enum {schema}.{name}: {problem}"));
                }
            }
            _ => {}
        }
    }

    for rel in rel_index.values() {
        for constraint in &rel.constraints {
            if let Some(comment) = constraint.comment() {
                for problem in annotations::check(comment, Site::Constraint) {
                    problems.push(format!(
                        "constraint {} on {}.{}: {}",
                        constraint.name(),
                        rel.id.schema(),
                        rel.id.name(),
                        problem
                    ));
                }
            }
        }
    }

    annotations::fail_on(problems)
}

/// Generate code split by schema
pub fn codegen_split(
    fn_index: &FunctionIndex,
    ty_index: &TypeIndex,
    rel_index: &RelIndex,
    config: &Config,
) -> anyhow::Result<BTreeMap<SchemaName, String>> {
    // An annotation that is unknown, malformed or on the wrong kind of
    // object fails the run, naming where it is.
    check_annotations(fn_index, ty_index, rel_index, config)?;

    // Constraints per table: the constraint enums, and the arms that map a
    // violation to them in every function's error enum.
    let table_constraints = unified_error::collect_table_constraints(rel_index);

    // Generate unified error types
    let error_types_code = generate_unified_errors(fn_index, &table_constraints, config);

    // Generate types and collect referenced schemas
    let (type_def_code, type_schema_refs) = codegen_types_with_refs(ty_index, config);
    let (fn_code, fn_schema_refs) =
        codegen_fns_with_refs(fn_index, ty_index, rel_index, &table_constraints, config);

    // Merge schema references
    let mut all_schema_refs: BTreeMap<SchemaName, BTreeSet<SchemaName>> = BTreeMap::new();
    for (schema, refs) in type_schema_refs {
        all_schema_refs.entry(schema).or_default().extend(refs);
    }
    for (schema, refs) in fn_schema_refs {
        all_schema_refs.entry(schema).or_default().extend(refs);
    }

    // Concatenate via `TokenStream::extend` rather than `quote! { #acc #ts }`,
    // which would re-tokenize the accumulator on every step (O(n²) in tokens).
    let mut schema_code: BTreeMap<SchemaName, String> = type_def_code
        .into_iter()
        .chain(fn_code)
        .into_grouping_map()
        .fold(TokenStream::new(), |mut acc, _, ts| {
            acc.extend(ts);
            acc
        })
        .iter()
        .map(|(schema, tokens)| {
            let referenced_schemas = all_schema_refs.get(schema).cloned().unwrap_or_default();
            (schema.clone(), schema_file(schema, referenced_schemas, tokens))
        })
        .collect();

    for (module, code) in shared_modules(error_types_code) {
        schema_code.insert(module.to_string(), WARNING_IGNORES.to_string() + &code.to_string());
    }

    Ok(schema_code)
}

/// The file for one schema module: imports of the schema modules it refers
/// to, the shared runtime modules, then its types and functions.
fn schema_file(
    schema: &SchemaName,
    referenced_schemas: BTreeSet<SchemaName>,
    tokens: &TokenStream,
) -> String {
    // Generate use statements for referenced schemas
    let schema_imports: Vec<TokenStream> = referenced_schemas
        .into_iter()
        .filter(|s| s != schema) // Don't import self
        .map(|s| {
            let schema_ident = schema_mod_ident(&s);
            quote! { use super::#schema_ident; }
        })
        .collect();

    // The shared runtime modules that used to be defined in every
    // schema module, still reachable under their old paths.
    let datetime_serde = datetime_serde_reexport();

    let code = quote! {
            #(#schema_imports)*

            use postgres_types::private::BytesMut;
            use postgres_types::{IsNull, ToSql, Type};
            use rust_decimal::Decimal;
            use bon::builder;
            use postgis_butmaintained::ewkb;

            #datetime_serde
            pub use super::tsvector;

            #tokens
        }.to_string();
    WARNING_IGNORES.to_string() + &code
}

/// The modules generated once and shared by every schema module, `queries`,
/// `tasks` and `custom_errors`, by module name.
fn shared_modules(error_types_code: TokenStream) -> Vec<(&'static str, TokenStream)> {
    vec![
        // The unified error types.
        ("errors", error_types_code),
        // Null-probe helper. Used by the optional-group row decoder to
        // detect whether a LEFT JOIN'd group should be `None` without
        // committing to a concrete `FromSql` type for the sentinel column:
        // `super::nullable::NullProbe`.
        ("nullable", generate_nullable_module()),
        // By-name composite codec. Every generated `FromSql`/`ToSql` impl
        // for a composite type goes through it, as `super::composite::*`.
        ("composite", generate_composite_module()),
        // Cache invalidation for connections that outlive a schema change.
        ("schema_cache", generate_schema_cache_module()),
        // Serde codecs for date and timestamp fields, named by
        // `#[serde(with = ...)]` in every module that has such a field.
        (DATETIME_SERDE_MODULE, generate_datetime_serde_module()),
        // `TsVector` / `TsQuery`, one definition for all schemas:
        // `super::tsvector::TsVector`.
        (TSVECTOR_MODULE, generate_tsvector_module()),
    ]
}

/// Codegen for the shared `schema_cache` module.
///
/// A pooled connection remembers two things about the schema: tokio-postgres
/// caches each type's definition, and deadpool caches prepared statements,
/// which hold their own copy of the types they were prepared with. After a
/// migration both are stale. Reads cope (see the `composite` module), but a
/// composite is encoded from the cached attribute list, so writing one fails
/// until the connection forgets it.
///
/// Generated functions only get a `GenericClient`, which exposes neither
/// cache, so this is driven from the pool: the application calls
/// `invalidate()` when it learns the schema changed, and the `hook()` it
/// installed on each pool clears a connection's caches the next time that
/// connection is checked out.
fn generate_schema_cache_module() -> TokenStream {
    quote! {
        use std::sync::RwLock;
        use std::time::Instant;

        /// When the schema was last reported changed, process-wide.
        static INVALIDATED_AT: RwLock<Option<Instant>> = RwLock::new(None);

        /// Report that the database schema changed, for example on a
        /// migration tool's "apply committed" notification.
        ///
        /// Every pooled connection last checked out before this call drops
        /// its type and statement caches at its next checkout, on every pool
        /// in the process that has [`hook`] installed. A connection that is
        /// checked out right now keeps its caches until it is returned.
        pub fn invalidate() {
            *INVALIDATED_AT.write().unwrap() = Some(Instant::now());
        }

        /// Drop one connection's type and statement caches now.
        pub fn clear(client: &deadpool_postgres::ClientWrapper) {
            client.clear_type_cache();
            client.statement_cache.clear();
        }

        /// A `pre_recycle` hook that applies [`invalidate`] to the connection
        /// being checked out. Install it on every pool:
        ///
        /// ```ignore
        /// let pool = config
        ///     .builder(tls)?
        ///     .runtime(deadpool_postgres::Runtime::Tokio1)
        ///     .pre_recycle(schema_cache::hook())
        ///     .build()?;
        /// ```
        pub fn hook() -> deadpool_postgres::Hook {
            deadpool_postgres::Hook::sync_fn(|client, metrics| {
                // `recycled` is this connection's previous checkout; a
                // connection that has never been recycled dates from its
                // creation.
                let last_used = metrics.recycled.unwrap_or(metrics.created);
                let stale = INVALIDATED_AT
                    .read()
                    .unwrap()
                    .is_some_and(|at| last_used <= at);
                if stale {
                    clear(client);
                }
                Ok(())
            })
        }
    }
}

/// Codegen for the shared `composite` module: the by-name composite codec.
///
/// `postgres-derive` matches a Rust struct against the live catalog type
/// exactly, so appending an attribute to a composite makes every read and
/// write of it fail in clients generated before the change. This codec matches
/// attributes by name instead:
///
/// - `accepts` requires every attribute the client knows; extra ones are fine.
/// - `read` hands over the attributes the connection knows by name and skips
///   the rest. It also tolerates a value carrying more attributes than the
///   connection's cached type definition, which is what a connection opened
///   before the `ALTER TYPE` sees.
/// - `write` emits every catalog attribute in catalog order, sending NULL for
///   one the client was not generated with.
///
/// Removing, renaming or retyping an attribute still fails.
fn generate_composite_module() -> TokenStream {
    quote! {
        use postgres_types::private::BytesMut;
        use postgres_types::{Field, IsNull, Kind, Type};
        use std::error::Error;

        type BoxError = Box<dyn Error + Sync + Send>;

        /// The attributes of composite type `ty`, looking through a domain.
        pub fn fields(ty: &Type) -> Option<&[Field]> {
            let ty = match ty.kind() {
                Kind::Domain(inner) => inner,
                _ => ty,
            };
            match ty.kind() {
                Kind::Composite(fields) => Some(fields),
                _ => None,
            }
        }

        /// True if `ty` is a composite (or a domain over one) named one of
        /// `names` that has every attribute in `known` with an acceptable
        /// type. Attributes the client does not know are allowed.
        pub fn accepts(ty: &Type, names: &[&str], known: &[(&str, fn(&Type) -> bool)]) -> bool {
            if !names.contains(&ty.name()) {
                return false;
            }
            let Some(fields) = fields(ty) else {
                return false;
            };
            known.iter().all(|(name, accepts)| {
                fields
                    .iter()
                    .any(|f| f.name() == *name && accepts(f.type_()))
            })
        }

        fn read_i32(buf: &mut &[u8]) -> Result<i32, BoxError> {
            if buf.len() < 4 {
                return Err("invalid buffer size".into());
            }
            let (head, tail) = buf.split_at(4);
            *buf = tail;
            Ok(i32::from_be_bytes([head[0], head[1], head[2], head[3]]))
        }

        /// Decode a composite value, calling `visit` with each attribute and
        /// its raw value (`None` for NULL).
        ///
        /// A value may carry more attributes than `ty` lists: the connection
        /// caches a type's definition, so one opened before an attribute was
        /// appended still has the shorter list. The extra values are skipped.
        pub fn read<'a>(
            ty: &Type,
            mut buf: &'a [u8],
            mut visit: impl FnMut(&Field, Option<&'a [u8]>) -> Result<(), BoxError>,
        ) -> Result<(), BoxError> {
            let fields = fields(ty).ok_or("not a composite type")?;
            let count = read_i32(&mut buf)?;
            if count < 0 || (count as usize) < fields.len() {
                return Err(format!(
                    "invalid field count for {}: {} vs {}",
                    ty.name(),
                    count,
                    fields.len()
                )
                .into());
            }
            for i in 0..count as usize {
                let oid = read_i32(&mut buf)? as u32;
                let len = read_i32(&mut buf)?;
                let raw = if len < 0 {
                    None
                } else {
                    let len = len as usize;
                    if len > buf.len() {
                        return Err("invalid buffer size".into());
                    }
                    let (head, tail) = buf.split_at(len);
                    buf = tail;
                    Some(head)
                };
                if let Some(field) = fields.get(i) {
                    if oid != field.type_().oid() {
                        return Err(format!(
                            "unexpected type for {}.{}: oid {} vs {}",
                            ty.name(),
                            field.name(),
                            oid,
                            field.type_().oid()
                        )
                        .into());
                    }
                    visit(field, raw)?;
                }
            }
            Ok(())
        }

        /// Encode a composite value. `visit` writes the attribute it is given
        /// and returns `Some`, or returns `None` for an attribute the client
        /// does not know, which is sent as NULL.
        pub fn write(
            ty: &Type,
            out: &mut BytesMut,
            mut visit: impl FnMut(&Field, &mut BytesMut) -> Option<Result<IsNull, BoxError>>,
        ) -> Result<IsNull, BoxError> {
            let fields = fields(ty).ok_or("not a composite type")?;
            out.extend_from_slice(&(fields.len() as i32).to_be_bytes());
            for field in fields {
                out.extend_from_slice(&field.type_().oid().to_be_bytes());
                let base = out.len();
                out.extend_from_slice(&[0; 4]);
                let is_null = match visit(field, out) {
                    Some(result) => result?,
                    None => IsNull::Yes,
                };
                let len = match is_null {
                    IsNull::Yes => {
                        out.truncate(base + 4);
                        -1
                    }
                    IsNull::No => i32::try_from(out.len() - base - 4)
                        .map_err(|_| "value too large to transmit")?,
                };
                out[base..base + 4].copy_from_slice(&len.to_be_bytes());
            }
            Ok(IsNull::No)
        }

        /// Error for an attribute the client needs that the value lacked.
        pub fn missing(ty: &str, field: &str) -> BoxError {
            format!("composite {} has no attribute {}", ty, field).into()
        }
    }
}

/// Codegen for the shared `nullable` module.
///
/// `NullProbe` implements `FromSql` for any Postgres type — its only job is to
/// answer "is this cell NULL?". `accepts` returns true unconditionally so
/// `try_get::<_, NullProbe>(i)` never trips tokio-postgres's type-compatibility
/// check, and the underlying `from_sql_nullable` machinery routes NULL cells to
/// `from_sql_null` and non-NULL cells to `from_sql`, so we just record which
/// branch fired.
fn generate_nullable_module() -> TokenStream {
    quote! {
        use postgres_types::{FromSql, Type};
        use std::error::Error;

        /// Cheap null-check probe. `try_get::<_, NullProbe>(i)?.is_null` is
        /// true iff the column at `i` is SQL NULL, regardless of its
        /// Postgres type.
        #[derive(Debug, Clone, Copy)]
        pub struct NullProbe {
            pub is_null: bool,
        }

        impl<'a> FromSql<'a> for NullProbe {
            fn from_sql(_ty: &Type, _raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
                Ok(NullProbe { is_null: false })
            }

            fn from_sql_null(_ty: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
                Ok(NullProbe { is_null: true })
            }

            fn accepts(_ty: &Type) -> bool {
                true
            }
        }
    }
}

/// Generate type definitions and collect referenced schemas
fn codegen_types_with_refs(
    type_index: &TypeIndex,
    config: &Config,
) -> (
    BTreeMap<SchemaName, TokenStream>,
    BTreeMap<SchemaName, BTreeSet<SchemaName>>,
) {
    let mut schema_refs: BTreeMap<SchemaName, BTreeSet<SchemaName>> = BTreeMap::new();

    let type_code: BTreeMap<SchemaName, Vec<(TokenStream, BTreeSet<String>)>> = type_index
        .deref()
        .values()
        .map(|t| {
            let schema = t.schema();
            let tokens = t.to_rust(type_index, config);
            let refs = collect_type_refs(t, type_index);
            (schema, (tokens, refs))
        })
        .filter(|(_, (tokens, _))| !tokens.is_empty())
        .into_group_map()
        .into_iter()
        .collect();

    let result: BTreeMap<SchemaName, TokenStream> = type_code
        .into_iter()
        .map(|(schema, items)| {
            let mut all_refs = BTreeSet::new();
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
fn collect_type_refs(pg_type: &PgType, type_index: &TypeIndex) -> BTreeSet<String> {
    let mut refs = BTreeSet::new();

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
    table_constraints: &BTreeMap<crate::pg_id::PgId, Vec<crate::pg_constraint::Constraint>>,
    config: &Config,
) -> TokenStream {
    // Collect all custom exceptions from functions
    let mut all_exceptions: BTreeSet<PgException> = BTreeSet::new();
    for pg_fn in fn_index.codegen_fns() {
        all_exceptions.extend(pg_fn.exceptions.iter().cloned());
    }

    // Generate constraint enums and the name-keyed message lookup
    let constraint_enums = unified_error::generate_constraint_enums(table_constraints);
    let constraint_lookup = unified_error::generate_constraint_message_lookup(table_constraints);
    // Typed-error messages live in custom_errors.rs, which only exists when
    // an errors schema is configured.
    let typed_message_lookup = if config.errors.is_some() {
        quote! { return super::custom_errors::custom_error_message(&type_name); }
    } else {
        quote! {
            let _ = type_name;
            return None;
        }
    };

    // Generate the unified error type
    let error_enum =
        unified_error::generate_unified_error(table_constraints, &all_exceptions, config);

    quote! {
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

            /// `true` when the underlying error is a typed payload raised
            /// through the raise helper (JSON-hinted). Its `message()` is a
            /// JSON document, not something to show a user.
            fn is_typed_error(&self) -> bool {
                self.as_db_error()
                    .is_some_and(|e| e.hint() == Some("application/json"))
            }

            /// The `type` name inside a typed error's JSON message
            /// (`new_item_image_required`), if this is one.
            fn typed_error_name(&self) -> Option<String> {
                let db = self.as_db_error()?;
                if db.hint() != Some("application/json") {
                    return None;
                }
                serde_json::from_str::<serde_json::Value>(db.message())
                    .ok()?
                    .get("type")?
                    .as_str()
                    .map(str::to_string)
            }

            /// The user-facing message for this error, if the schema carries
            /// one: the `@pgrpc_message` on the violated constraint (looked up
            /// by schema, table or domain, and constraint name), or the one on
            /// a typed error's payload type. Works for every generated error
            /// enum and for a bare `DbError`, including violations that
            /// surfaced as `Other(PgRpcError)` without a typed variant.
            fn user_message(&self) -> Option<ConstraintMessage> {
                let db = self.as_db_error()?;
                if let Some(type_name) = self.typed_error_name() {
                    #typed_message_lookup
                }
                let constraint = db.constraint()?;
                if let Some(table) = db.table() {
                    if let Some(found) = constraint_message(db.schema(), table, constraint) {
                        return Some(found);
                    }
                }
                if let Some(datatype) = db.datatype() {
                    return domain_constraint_message(db.schema(), datatype, constraint);
                }
                None
            }
        }

        /// Blanket implementation of PgRpcErrorExt for all types that implement AsDbError
        impl<T: AsDbError> PgRpcErrorExt for T {}

        /// A bare `DbError`, as destructured from any variant, has the same helpers.
        impl AsDbError for tokio_postgres::error::DbError {
            fn as_db_error(&self) -> Option<&tokio_postgres::error::DbError> {
                Some(self)
            }
        }

        impl AsDbError for tokio_postgres::Error {
            fn as_db_error(&self) -> Option<&tokio_postgres::error::DbError> {
                tokio_postgres::Error::as_db_error(self)
            }
        }

        /// A row column could not be decoded into its generated Rust type.
        ///
        /// Typically the database the code was generated from and the one it
        /// runs against disagree: a column was removed, renamed or retyped,
        /// or a result now has two columns with the same name.
        #[derive(Debug, thiserror::Error)]
        #[error("{item}: failed to decode column `{column}` (index {index}): {source}")]
        pub struct DecodeError {
            /// The generated function, query or row struct the column belongs to.
            pub item: &'static str,
            /// The SQL column name as it was at generation time.
            pub column: &'static str,
            /// Zero-based position in the row. For a column that could not
            /// be located, its position at generation time.
            pub index: usize,
            /// A `tokio_postgres::Error` when a value failed to convert; a
            /// plain message when the column could not be located.
            #[source]
            pub source: Box<dyn std::error::Error + Send + Sync>,
        }

        impl DecodeError {
            /// A `map_err` closure attributing a decode failure to one column.
            pub fn at(
                item: &'static str,
                column: &'static str,
                index: usize,
            ) -> impl FnOnce(tokio_postgres::Error) -> DecodeError {
                move |source| DecodeError { item, column, index, source: Box::new(source) }
            }
        }

        /// Where each of `names` sits in `row`, looked up by column name.
        ///
        /// Query row structs decode through this instead of fixed positions,
        /// so a column added to the result after the code was generated
        /// (through a `*` over a table or view that gained one) is ignored
        /// wherever it lands. A column that is missing, or that now appears
        /// more than once, is an error: guessing would read the wrong value.
        pub fn column_positions<const N: usize>(
            item: &'static str,
            names: &[&'static str; N],
            row: &tokio_postgres::Row,
        ) -> Result<[usize; N], DecodeError> {
            let columns = row.columns();
            let mut at = [0usize; N];
            for (i, name) in names.iter().enumerate() {
                let mut matches = columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.name() == *name)
                    .map(|(position, _)| position);
                let located = match (matches.next(), matches.next()) {
                    (Some(position), None) => Ok(position),
                    (None, _) => Err("the result has no column of that name"),
                    (Some(_), Some(_)) => Err("the result has more than one column of that name"),
                };
                at[i] = located.map_err(|reason| DecodeError {
                    item,
                    column: name,
                    index: i,
                    source: reason.into(),
                })?;
            }
            Ok(at)
        }

        #constraint_lookup

        #(#constraint_enums)*

        #error_enum
    }
}

/// Generate function definitions and collect referenced schemas
fn codegen_fns_with_refs(
    fns: &FunctionIndex,
    types: &TypeIndex,
    rel_index: &RelIndex,
    table_constraints: &BTreeMap<crate::pg_id::PgId, Vec<crate::pg_constraint::Constraint>>,
    config: &Config,
) -> (
    BTreeMap<SchemaName, TokenStream>,
    BTreeMap<SchemaName, BTreeSet<SchemaName>>,
) {
    let mut schema_refs: BTreeMap<SchemaName, BTreeSet<SchemaName>> = BTreeMap::new();

    let schemas: BTreeMap<&str, Vec<&PgFn>> = fns
        .codegen_fns()
        .into_group_map_by(|f| f.schema.as_str())
        .into_iter()
        .collect();

    let result: BTreeMap<SchemaName, TokenStream> = schemas
        .into_iter()
        .map(|(schema, fns)| {
            let mut all_refs = BTreeSet::new();

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
                    let fn_impl = f.to_rust(types, config);
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
fn collect_fn_refs(pg_fn: &PgFn, types: &TypeIndex) -> BTreeSet<String> {
    let mut refs = BTreeSet::new();

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

/// The most parameters of one query that may fall back to the column
/// DEFAULT. Each combination of given/omitted needs its own SQL text, so the
/// generated function carries `2^n` of them.
const MAX_DEFAULT_PARAMS: usize = 8;

/// Given a set of param positions to replace with DEFAULT,
/// rewrite SQL and return the new SQL string.
/// Parameters are renumbered after DEFAULT replacements.
///
/// Only real parameter tokens are touched: a `$1` inside a string literal,
/// a quoted identifier or a comment is text, and stays as written.
fn rewrite_sql_for_defaults(
    sql: &str,
    total_params: usize,
    default_positions: &[usize], // 1-indexed positions to replace with DEFAULT
) -> anyhow::Result<String> {
    // Build mapping: old position -> new position (or None if DEFAULT)
    let mut new_positions: Vec<Option<usize>> = Vec::with_capacity(total_params);
    let mut next_pos = 1usize;

    for pos in 1..=total_params {
        if default_positions.contains(&pos) {
            new_positions.push(None);
        } else {
            new_positions.push(Some(next_pos));
            next_pos += 1;
        }
    }

    let scanned = pg_query::scan(sql)
        .map_err(|e| anyhow::anyhow!("could not tokenize query to rewrite its parameters: {e}"))?;
    let param_token = pg_query::protobuf::Token::Param as i32;

    let mut rewritten = String::with_capacity(sql.len());
    let mut copied_to = 0usize;
    for token in scanned.tokens.iter().filter(|t| t.token == param_token) {
        let (start, end) = (token.start as usize, token.end as usize);
        // `$` followed by the parameter number.
        let Some(param_num) = sql
            .get(start + 1..end)
            .and_then(|digits| digits.parse::<usize>().ok())
        else {
            continue;
        };
        // Keep unchanged if out of range
        let Some(new_position) = param_num.checked_sub(1).and_then(|i| new_positions.get(i))
        else {
            continue;
        };
        rewritten.push_str(&sql[copied_to..start]);
        match new_position {
            Some(new_pos) => rewritten.push_str(&format!("${new_pos}")),
            None => rewritten.push_str("DEFAULT"),
        }
        copied_to = end;
    }
    rewritten.push_str(&sql[copied_to..]);
    Ok(rewritten)
}

/// Build the `&[postgres_types::Type]` slice expression for `prepare_typed_cached`,
/// or `None` if any parameter has a user-defined OID that postgres-types can't resolve.
///
/// We use `Type::from_oid` at runtime so we don't have to hard-code the OID→const mapping
/// here — postgres-types already maintains that mapping for every builtin. If `from_oid`
/// returns `None` for any param (i.e. user-defined composite/domain/enum/array-of-those),
/// we bail out and the caller falls back to untyped `prepare_cached`.
fn build_prepared_types_slice(
    params: &[crate::query_introspector::QueryParam],
) -> Option<TokenStream> {
    // Use the resolved postgres type oid, not the domain-preserving inferred oid.
    // postgres-types' Type::from_oid only knows builtin OIDs, and the prepared statement
    // itself was created with the resolved types anyway.
    let oids: Vec<OID> = params.iter().map(|p| p.postgres_type_oid).collect();
    if oids
        .iter()
        .any(|&oid| postgres_types::Type::from_oid(oid).is_none())
    {
        return None;
    }
    let lits: Vec<TokenStream> = oids.iter().map(|oid| quote! { #oid }).collect();
    Some(quote! {
        &[#(postgres_types::Type::from_oid(#lits).unwrap()),*]
    })
}

/// Generate code for SQL query files
pub fn codegen_queries(
    query_index: &crate::query_index::QueryIndex,
    type_index: &TypeIndex,
    rel_index: &RelIndex,
    config: &Config,
) -> anyhow::Result<TokenStream> {
    // Collect table constraints for error generation
    let table_constraints = unified_error::collect_table_constraints(rel_index);

    let query_code: Vec<TokenStream> = query_index
        .values()
        .map(|query| generate_query_code(query, type_index, &table_constraints, config))
        .collect::<anyhow::Result<_>>()?;

    Ok(quote! {
        #(#query_code)*
    })
}

/// Reports the types of a query's parameters and result columns that codegen
/// cannot do right by. Returns true if the query has to be left out: one of
/// them has no Rust mapping at all. A type that was simply not introspected
/// only gets a warning, and degrades to `serde_json::Value` (a column) or
/// `&dyn ToSql` (a parameter).
fn query_uses_unsupported_type(
    query: &crate::query_introspector::IntrospectedQuery,
    type_index: &TypeIndex,
) -> bool {
    let params = query.params.iter().map(|p| ("parameter", &p.name, p.type_oid));
    let columns = query
        .return_columns
        .iter()
        .flatten()
        .map(|c| ("column", &c.name, c.type_oid));

    let mut skip = false;
    for (kind, name, type_oid) in params.chain(columns) {
        if let Some(type_name) = unsupported_type(type_oid, type_index) {
            log::warn!(
                "Skipping query `{}` ({}:{}): {} `{}` has type `{}`, which has no Rust mapping",
                query.name, query.file_path, query.line_number, kind, name, type_name
            );
            skip = true;
        } else if type_index.get(&type_oid).is_none() {
            log::warn!(
                "Query `{}` ({}:{}): the type of {} `{}` (oid {}) is unknown; it is generated as {}",
                query.name,
                query.file_path,
                query.line_number,
                kind,
                name,
                type_oid,
                if kind == "column" { "serde_json::Value" } else { "&dyn ToSql" }
            );
        }
    }
    skip
}

/// Generate code for a single query
fn generate_query_code(
    query: &crate::query_introspector::IntrospectedQuery,
    type_index: &TypeIndex,
    table_constraints: &BTreeMap<crate::pg_id::PgId, Vec<crate::pg_constraint::Constraint>>,
    config: &Config,
) -> anyhow::Result<TokenStream> {
    use crate::ident::{sql_to_rs_ident, CaseType};
    use crate::sql_parser::QueryType;

    if query_uses_unsupported_type(query, type_index) {
        return Ok(TokenStream::new());
    }

    let fn_name = sql_to_rs_ident(&query.name, CaseType::Snake);
    let sql = &query.sql;
    let item = query.name.as_str();

    // Generate error enum for this query
    let error_enum = generate_query_error_enum(query, table_constraints, config);
    let error_enum_name = query_error_enum_ident(query);

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
    let (return_type, query_execution, row_struct) = match (&query.query_type, &query.return_columns) {
        (QueryType::One | QueryType::Opt | QueryType::Many, Some(columns)) => {
            // What one row is, how `row` is read, and how all `rows` are: a
            // single column is returned bare, several as a generated struct.
            let (row_type, row_struct, decode_row, decode_rows) = if columns.len() == 1 {
                let scalar_type = get_column_type(&columns[0], type_index);
                let decode = decode_err(&query.name, &columns[0].name, 0);
                let decode_row = quote! {
                    row.try_get(0).map_err(#decode).map_err(#error_enum_name::from)
                };
                let decode_rows = quote! {
                    rows.into_iter().map(|row| #decode_row).collect()
                };
                (scalar_type, quote! {}, decode_row, decode_rows)
            } else {
                let struct_name = quote::format_ident!(
                    "{}Row",
                    crate::ident::sql_to_rs_string(&query.name, CaseType::Pascal)
                        .trim_start_matches("r#")
                );
                let row_struct = generate_row_struct(&struct_name, columns, type_index);
                (
                    quote! { #struct_name },
                    row_struct,
                    quote! { #struct_name::try_from(row).map_err(#error_enum_name::from) },
                    quote! { #struct_name::decode_all(rows).map_err(#error_enum_name::from) },
                )
            };

            let (return_type, execution) = match query.query_type {
                QueryType::One => (
                    quote! { Result<#row_type, #error_enum_name> },
                    quote! {
                        let row = client.query_opt(&stmt, &params).await
                            .map_err(#error_enum_name::from)?
                            .ok_or(super::errors::PgRpcError::NoRows { item: #item })?;
                        #decode_row
                    },
                ),
                QueryType::Opt => (
                    quote! { Result<Option<#row_type>, #error_enum_name> },
                    quote! {
                        client.query_opt(&stmt, &params).await
                            .map_err(#error_enum_name::from)?
                            .map(|row| #decode_row)
                            .transpose()
                    },
                ),
                _ => (
                    quote! { Result<Vec<#row_type>, #error_enum_name> },
                    quote! {
                        let rows = client.query(&stmt, &params).await
                            .map_err(#error_enum_name::from)?;
                        #decode_rows
                    },
                ),
            };
            (return_type, execution, row_struct)
        }
        // A row-returning query without result columns should not happen.
        (QueryType::One, None) => (
            quote! { Result<(), #error_enum_name> },
            quote! {
                client.execute(&stmt, &params).await?;
                Ok(())
            },
            quote! {},
        ),
        (QueryType::Opt, None) => (
            quote! { Result<Option<()>, #error_enum_name> },
            quote! {
                client.execute(&stmt, &params).await?;
                Ok(Some(()))
            },
            quote! {},
        ),
        (QueryType::Many, None) => (
            quote! { Result<Vec<()>, #error_enum_name> },
            quote! {
                let rows = client.query(&stmt, &params).await?;
                Ok(vec![(); rows.len()])
            },
            quote! {},
        ),
        (QueryType::Exec, _) => (
            quote! { Result<(), #error_enum_name> },
            quote! {
                client.execute(&stmt, &params).await?;
                Ok(())
            },
            quote! {},
        ),
        (QueryType::ExecRows, _) => (
            quote! { Result<u64, #error_enum_name> },
            quote! {
                let rows_affected = client.execute(&stmt, &params).await?;
                Ok(rows_affected)
            },
            quote! {},
        ),
    };

    // Identify default-eligible params: nullable params where the column is NOT NULL but has DEFAULT
    // These should emit DEFAULT instead of NULL when None is passed
    let default_eligible_params: Vec<(usize, &crate::query_introspector::QueryParam)> = query
        .params
        .iter()
        .enumerate()
        .filter(|(_, p)| p.nullable && p.has_default)
        .collect();
    if default_eligible_params.len() > MAX_DEFAULT_PARAMS {
        anyhow::bail!(
            "query `{}` ({}:{}) has {} optional parameters that fall back to a column DEFAULT ({}). \
             Each combination needs its own statement, {} in all; the limit is {} such parameters. \
             Split the query, or pass some of the values explicitly (e.g. `coalesce(:name, <default>)`).",
            query.name,
            query.file_path,
            query.line_number,
            default_eligible_params.len(),
            default_eligible_params.iter().map(|(_, p)| p.name.as_str()).join(", "),
            1u128 << default_eligible_params.len(),
            MAX_DEFAULT_PARAMS,
        );
    }

    // Generate doc comment with SQL
    let doc_comment = format!("Query: {}\n\nSQL:\n```sql\n{}\n```", query.name, sql);

    // Check if tracing is enabled
    let tracing_enabled = config.tracing.as_ref().map(|t| t.enabled).unwrap_or(false);

    // Generate the tracing::instrument attribute if enabled
    let tracing_attr = if tracing_enabled {
        // Use "queries.<query_name>" as the span name for SQL queries
        let span_name = format!("queries.{}", query.name);
        quote! {
            #[tracing::instrument(name = #span_name, skip_all)]
        }
    } else {
        quote! {}
    };

    // Generate .into() conversions for nullable parameters (impl Into<Option<T>> -> Option<T>)
    let param_conversions: Vec<TokenStream> = query
        .params
        .iter()
        .filter(|p| p.nullable)
        .map(|p| {
            let name = sql_to_rs_ident(&p.name, CaseType::Snake);
            quote! { let #name = #name.into(); }
        })
        .collect();

    // Generate query and params setup based on whether we have default-eligible params
    let query_and_params_setup = if default_eligible_params.is_empty() {
        // No default-eligible params - use simple approach
        let mut nullable_domain_bindings: Vec<TokenStream> = Vec::new();
        let param_refs: Vec<TokenStream> = query
            .params
            .iter()
            .map(|param| {
                generate_param_ref(param, type_index, &mut nullable_domain_bindings)
            })
            .collect();

        quote! {
            let query = #sql;
            #(#nullable_domain_bindings)*
            let params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![#(#param_refs),*];
        }
    } else {
        // Generate match arms for all 2^N combinations of default-eligible params
        generate_default_match_arms(query, &default_eligible_params, sql, type_index)
            .map_err(|e| e.context(format!("query `{}` ({}:{})", query.name, query.file_path, query.line_number)))?
    };

    // Prepare the statement using the per-connection cache.
    //
    // When the SQL is static (no default-eligible params) AND all params are builtin
    // postgres types, pass the type slice so prepare_typed_cached can skip an extra
    // describe round-trip on first prepare. Otherwise fall back to untyped
    // prepare_cached — the cache key includes the (rewritten) SQL string, so each
    // default-match-arm SQL variant still gets cached on its own.
    let has_default_arms = !default_eligible_params.is_empty();
    let prepare_step = match (has_default_arms, build_prepared_types_slice(&query.params)) {
        (false, Some(types_slice)) => quote! {
            let stmt = client.prepare_typed_cached(query, #types_slice).await
                .map_err(#error_enum_name::from)?;
        },
        _ => quote! {
            let stmt = client.prepare_cached(query).await
                .map_err(#error_enum_name::from)?;
        },
    };

    Ok(quote! {
        #row_struct

        #error_enum

        #[doc = #doc_comment]
        #tracing_attr
        pub async fn #fn_name(
            client: &impl deadpool_postgres::GenericClient,
            #(#params),*
        ) -> #return_type {
            #(#param_conversions)*
            #query_and_params_setup
            #prepare_step

            #query_execution
        }
    })
}

/// `{QueryName}Error`, the query's error enum.
fn query_error_enum_ident(query: &crate::query_introspector::IntrospectedQuery) -> proc_macro2::Ident {
    use crate::ident::{sql_to_rs_string, CaseType};
    quote::format_ident!(
        "{}Error",
        sql_to_rs_string(&query.name, CaseType::Pascal).trim_start_matches("r#")
    )
}

/// Chain of `.as_inner()` calls that peels `depth` domain layers.
/// Used instead of direct `.0` field access because strict domains have a
/// private inner field that is not visible from the `queries` module, and
/// method calls auto-deref through reference layers just like field access.
fn domain_unwrap_chain(depth: usize) -> TokenStream {
    (0..depth).map(|_| quote! { .as_inner() }).collect()
}

/// The expression that passes `param` in a query's parameter list: a
/// reference to it, with domain layers peeled down to the type Postgres
/// expects. A nullable domain needs a binding for the peeled `Option`, which
/// is pushed to `nullable_domain_bindings`.
fn generate_param_ref(
    param: &crate::query_introspector::QueryParam,
    type_index: &TypeIndex,
    nullable_domain_bindings: &mut Vec<TokenStream>,
) -> TokenStream {
    use crate::ident::{sql_to_rs_ident, CaseType};

    let param_name = sql_to_rs_ident(&param.name, CaseType::Snake);

    // Compute how many domain layers to unwrap.
    // This handles nested domains (e.g., level3 → level2 → level1 → citext).
    let unwrap_depth = compute_domain_unwrap_depth(
        param.type_oid,
        param.postgres_type_oid,
        type_index,
    );
    if unwrap_depth == 0 {
        // `Option<T>` implements ToSql too, so a reference always works.
        return quote! { &#param_name };
    }

    let unwrap_chain = domain_unwrap_chain(unwrap_depth);
    if param.nullable {
        // For nullable domain: create a let binding to unwrap, then reference it
        use heck::ToSnakeCase;
        let unwrapped_name_str = format!("{}_unwrapped", param.name.to_snake_case());
        let unwrapped_name = sql_to_rs_ident(&unwrapped_name_str, CaseType::Snake);

        nullable_domain_bindings.push(
            quote! { let #unwrapped_name = #param_name.as_ref().map(|v| v #unwrap_chain); }
        );
        quote! { &#unwrapped_name }
    } else {
        // For non-nullable domain: pass param.as_inner().as_inner()... (depth times)
        quote! { #param_name #unwrap_chain }
    }
}

/// Generate match arms for all 2^N combinations of default-eligible params
fn generate_default_match_arms(
    query: &crate::query_introspector::IntrospectedQuery,
    default_eligible_params: &[(usize, &crate::query_introspector::QueryParam)],
    sql: &str,
    type_index: &TypeIndex,
) -> anyhow::Result<TokenStream> {
    use crate::ident::{sql_to_rs_string, CaseType};

    let n = default_eligible_params.len();
    let total_combinations = 1usize << n; // 2^n, bounded by MAX_DEFAULT_PARAMS
    let as_dyn = quote! { as &(dyn postgres_types::ToSql + Sync) };

    // Get param names for match tuple
    let match_param_names: Vec<TokenStream> = default_eligible_params
        .iter()
        .map(|(_, p)| crate::ident::sql_to_rs_ident(&p.name, CaseType::Snake))
        .collect();

    let mut match_arms: Vec<TokenStream> = Vec::with_capacity(total_combinations);

    for combination in 0..total_combinations {
        // For this combination, determine which default-eligible params are Some (bit=1) vs None (bit=0)
        let mut default_positions: Vec<usize> = Vec::new();
        let mut pattern_parts: Vec<TokenStream> = Vec::with_capacity(n);
        let mut some_val_names: Vec<(usize, proc_macro2::Ident)> = Vec::new(); // (param_index, val_name)

        for (bit_index, (param_index, param)) in default_eligible_params.iter().enumerate() {
            let is_some = (combination >> bit_index) & 1 == 1;
            if is_some {
                // This param is Some - use a binding
                let val_name = quote::format_ident!(
                    "{}_val",
                    sql_to_rs_string(&param.name, CaseType::Snake).trim_start_matches("r#")
                );
                pattern_parts.push(quote! { Some(#val_name) });
                some_val_names.push((*param_index, val_name));
            } else {
                // This param is None - will use DEFAULT
                pattern_parts.push(quote! { None });
                default_positions.push(param.position);
            }
        }

        // Generate the rewritten SQL for this combination
        let rewritten_sql = rewrite_sql_for_defaults(sql, query.params.len(), &default_positions)?;

        // Generate params vec for this combination
        // Include all non-default-eligible params plus the Some values from default-eligible params
        let mut param_refs: Vec<TokenStream> = Vec::new();
        let mut nullable_domain_bindings: Vec<TokenStream> = Vec::new();

        for (param_index, param) in query.params.iter().enumerate() {
            // A default-eligible param that is Some here is bound in the
            // pattern; one that is None is DEFAULT in the SQL and not passed.
            if default_eligible_params.iter().any(|(idx, _)| *idx == param_index) {
                let Some((_, val_name)) = some_val_names.iter().find(|(idx, _)| *idx == param_index)
                else {
                    continue;
                };

                // Handle domain unwrapping for the Some value
                let unwrap_chain = domain_unwrap_chain(compute_domain_unwrap_depth(
                    param.type_oid,
                    param.postgres_type_oid,
                    type_index,
                ));
                param_refs.push(quote! { #val_name #unwrap_chain #as_dyn });
            } else {
                // Regular param (not default-eligible)
                let param_ref = generate_param_ref(param, type_index, &mut nullable_domain_bindings);
                param_refs.push(quote! { #param_ref #as_dyn });
            }
        }

        let pattern = if n == 1 {
            // Single param - don't use tuple
            let p = &pattern_parts[0];
            quote! { #p }
        } else {
            quote! { (#(#pattern_parts),*) }
        };

        match_arms.push(quote! {
            #pattern => {
                #(#nullable_domain_bindings)*
                (#rewritten_sql, vec![#(#param_refs),*])
            }
        });
    }

    let match_expr = if n == 1 {
        let name = &match_param_names[0];
        quote! { &#name }
    } else {
        quote! { (#(&#match_param_names),*) }
    };

    Ok(quote! {
        let (query, params): (&str, Vec<&(dyn postgres_types::ToSql + Sync)>) = match #match_expr {
            #(#match_arms),*
        };
    })
}

/// Generate error enum for a single query
fn generate_query_error_enum(
    query: &crate::query_introspector::IntrospectedQuery,
    table_constraints: &BTreeMap<crate::pg_id::PgId, Vec<crate::pg_constraint::Constraint>>,
    config: &Config,
) -> TokenStream {
    let error_enum_name = query_error_enum_ident(query);

    // SQLSTATEs first, then the typed
    // errors raised by functions the query calls.
    let (sql_states, typed): (Vec<PgException>, Vec<PgException>) = query
        .exceptions
        .iter()
        .cloned()
        .partition(|e| matches!(e, PgException::Explicit(_)));
    let exceptions = [sql_states, typed].concat();

    unified_error::generate_error_enum(
        &quote! { #error_enum_name },
        &query.table_dependencies,
        table_constraints,
        &exceptions,
        config,
    )
}

/// Generate a row struct for query results, decoded by column name.
///
/// Columns with double-underscore names (`prefix__field`) are automatically
/// grouped into nested structs.
fn generate_row_struct(
    struct_name: &proc_macro2::Ident,
    columns: &[crate::query_introspector::QueryColumn],
    type_index: &TypeIndex,
) -> TokenStream {
    use crate::ident::{sql_to_rs_string, CaseType};
    use crate::row_struct::{grouped_layout, Decode, FieldParts, RowShape};

    let layout = grouped_layout(
        columns
            .iter()
            .map(|c| (c.name.as_str(), c.nullable_due_to_join)),
    );
    let shape = RowShape::build(
        quote! { #struct_name },
        &struct_name.to_string(),
        layout,
        |place| {
            let name = place.member.unwrap_or(&columns[place.index].name);
            sql_to_rs_string(name, CaseType::Snake)
        },
        |place, _| {
            let col = &columns[place.index];
            // A field keeps the column's name, less the group prefix. Inside
            // a group it has its natural nullability: on the base column or
            // because of the join.
            let name = place.member.unwrap_or(&col.name);
            let nullable = col.nullable || (place.member.is_some() && col.nullable_due_to_join);
            FieldParts {
                column: col.name.clone(),
                label: name.to_string(),
                ty: column_type(col.type_oid, nullable, type_index),
                attrs: field_serde_attrs(type_index.get(&col.type_oid), nullable, Some(name)),
            }
        },
    );

    let derives = quote! { #[derive(Debug, Clone, serde::Serialize)] };
    let structs = shape.emit_structs(&derives, &derives);
    let decoder = shape.emit_decoder(Decode::ResolvedName);
    quote! {
        #structs

        #decoder
    }
}

/// Get Rust type for a parameter
fn get_param_type(type_oid: OID, type_index: &TypeIndex, nullable: bool) -> TokenStream {
    let base_type = match type_index.get(&type_oid) {
        // Query parameters take floats by value; function arguments have
        // always taken them by reference.
        Some(PgType::Float32) => quote! { f32 },
        Some(PgType::Float64) => quote! { f64 },
        Some(pg_type) => pg_type.to_rust_param(type_index),
        // Unknown type; `generate_query_code` has warned about it.
        None => quote! { &(dyn postgres_types::ToSql + Sync) },
    };

    if nullable {
        quote! { impl Into<Option<#base_type>> }
    } else {
        base_type
    }
}

/// Compute how many domain layers to unwrap to reach the target type.
/// Returns 0 if no unwrapping needed, or the number of `.as_inner()` calls required.
///
/// PostgreSQL's parameter type inference varies by context:
/// - Comparisons (WHERE slug = $1): PostgreSQL expects the base type (citext)
/// - Assignments (SET slug = $1): PostgreSQL expects the domain type (slug)
///
/// For nested domains like `level3` → `level2` → `level1` → `citext`:
/// - PostgreSQL may expect `citext` (the ultimate base)
/// - A single `.as_inner()` call only removes one layer
/// - This function computes how many `.as_inner()` calls are needed
fn compute_domain_unwrap_depth(
    from_oid: OID,
    to_oid: OID,
    type_index: &TypeIndex,
) -> usize {
    if from_oid == to_oid {
        return 0;
    }

    let mut current = from_oid;
    let mut depth = 0;

    while current != to_oid {
        match type_index.get(&current) {
            Some(PgType::Domain { type_oid: base_oid, .. }) => {
                // Skip composite domains (they have custom ToSql)
                if matches!(type_index.get(base_oid), Some(PgType::Composite { .. })) {
                    return 0;
                }
                current = *base_oid;
                depth += 1;
            }
            _ => {
                // Reached a non-domain type but haven't found target.
                // This means we've unwrapped as far as we can.
                break;
            }
        }
    }

    depth
}

/// Get Rust type for a column
fn get_column_type(
    column: &crate::query_introspector::QueryColumn,
    type_index: &TypeIndex,
) -> TokenStream {
    column_type(column.type_oid, column.nullable, type_index)
}

/// The Rust type of a result column of type `type_oid`.
fn column_type(type_oid: OID, nullable: bool, type_index: &TypeIndex) -> TokenStream {
    let base_type = match type_index.get(&type_oid) {
        Some(pg_type) => pg_type.to_rust_ident(type_index),
        // Unknown type; `generate_query_code` has warned about it.
        None => quote! { serde_json::Value },
    };

    if nullable {
        quote! { Option<#base_type> }
    } else {
        base_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_sql_for_defaults_no_defaults() {
        let sql = "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)";
        let result = rewrite_sql_for_defaults(sql, 3, &[]).unwrap();
        assert_eq!(result, "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)");
    }

    #[test]
    fn test_rewrite_sql_for_defaults_single_default() {
        let sql = "INSERT INTO t (a, b) VALUES ($1, $2)";
        // Replace $2 with DEFAULT
        let result = rewrite_sql_for_defaults(sql, 2, &[2]).unwrap();
        assert_eq!(result, "INSERT INTO t (a, b) VALUES ($1, DEFAULT)");
    }

    #[test]
    fn test_rewrite_sql_for_defaults_first_param() {
        let sql = "INSERT INTO t (a, b) VALUES ($1, $2)";
        // Replace $1 with DEFAULT, $2 becomes $1
        let result = rewrite_sql_for_defaults(sql, 2, &[1]).unwrap();
        assert_eq!(result, "INSERT INTO t (a, b) VALUES (DEFAULT, $1)");
    }

    #[test]
    fn test_rewrite_sql_for_defaults_multiple_defaults() {
        let sql = "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)";
        // Replace $2 and $3 with DEFAULT
        let result = rewrite_sql_for_defaults(sql, 3, &[2, 3]).unwrap();
        assert_eq!(result, "INSERT INTO t (a, b, c) VALUES ($1, DEFAULT, DEFAULT)");
    }

    #[test]
    fn test_rewrite_sql_for_defaults_middle_param() {
        let sql = "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)";
        // Replace $2 with DEFAULT, $3 becomes $2
        let result = rewrite_sql_for_defaults(sql, 3, &[2]).unwrap();
        assert_eq!(result, "INSERT INTO t (a, b, c) VALUES ($1, DEFAULT, $2)");
    }

    #[test]
    fn test_rewrite_sql_for_defaults_all_params() {
        let sql = "INSERT INTO t (a, b) VALUES ($1, $2)";
        // Replace all with DEFAULT
        let result = rewrite_sql_for_defaults(sql, 2, &[1, 2]).unwrap();
        assert_eq!(result, "INSERT INTO t (a, b) VALUES (DEFAULT, DEFAULT)");
    }

    #[test]
    fn test_rewrite_sql_for_defaults_preserves_double_digit_params() {
        let sql = "SELECT * FROM t WHERE a = $1 AND b = $10 AND c = $11";
        // Replace $10 with DEFAULT, $11 becomes $10
        let result = rewrite_sql_for_defaults(sql, 11, &[10]).unwrap();
        assert_eq!(result, "SELECT * FROM t WHERE a = $1 AND b = DEFAULT AND c = $10");
    }

    #[test]
    fn test_rewrite_sql_for_defaults_leaves_literals_and_comments_alone() {
        let sql = "INSERT INTO t (a, b, c) -- $2 is the price\n\
                   VALUES ($1, $2, 'costs $2') /* not $1 */ RETURNING \"$1\", $tag$ $1 $tag$";
        let result = rewrite_sql_for_defaults(sql, 2, &[1]).unwrap();
        assert_eq!(
            result,
            "INSERT INTO t (a, b, c) -- $2 is the price\n\
             VALUES (DEFAULT, $1, 'costs $2') /* not $1 */ RETURNING \"$1\", $tag$ $1 $tag$"
        );
    }

    /// A query with `n` parameters that each fall back to a column DEFAULT.
    fn query_with_default_params(n: usize) -> crate::query_introspector::IntrospectedQuery {
        let placeholders: Vec<String> = (1..=n).map(|i| format!("${i}")).collect();
        crate::query_introspector::IntrospectedQuery {
            name: "insert_wide".to_string(),
            query_type: crate::sql_parser::QueryType::Exec,
            sql: format!("INSERT INTO t VALUES ({})", placeholders.join(", ")),
            params: (1..=n)
                .map(|i| crate::query_introspector::QueryParam {
                    name: format!("c{i}"),
                    type_oid: 23,
                    postgres_type_oid: 23,
                    position: i,
                    nullable: true,
                    has_default: true,
                })
                .collect(),
            return_columns: None,
            file_path: "queries.sql".to_string(),
            line_number: 1,
            exceptions: vec![],
            table_dependencies: BTreeMap::new(),
        }
    }

    fn int_types() -> TypeIndex {
        TypeIndex::from_types(BTreeMap::from([(23, PgType::Int32)]))
    }

    #[test]
    fn default_params_up_to_the_cap_generate_every_combination() {
        let query = query_with_default_params(MAX_DEFAULT_PARAMS);
        let code = generate_query_code(&query, &int_types(), &BTreeMap::new(), &Config::default())
            .expect("the cap itself is allowed")
            .to_string();
        assert_eq!(
            code.matches("INSERT INTO t VALUES").count(),
            // One statement per combination, and the doc comment.
            (1 << MAX_DEFAULT_PARAMS) + 1
        );
    }

    #[test]
    fn too_many_default_params_is_an_error_not_an_explosion() {
        let query = query_with_default_params(MAX_DEFAULT_PARAMS + 1);
        let err = generate_query_code(&query, &int_types(), &BTreeMap::new(), &Config::default())
            .expect_err("more than the cap must fail");
        let message = format!("{err:#}");
        assert!(message.contains("insert_wide"), "{message}");
        assert!(message.contains("queries.sql:1"), "{message}");
        assert!(message.contains("9 optional parameters"), "{message}");
        assert!(message.contains("512"), "{message}");
    }
}
