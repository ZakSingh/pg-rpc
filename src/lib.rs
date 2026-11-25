use crate::codegen::{codegen_split, datetime_serde_reexport};
use crate::config::Config;
use crate::db::Db;
use crate::fn_index::FunctionIndex;
use crate::rel_index::RelIndex;
use crate::task_index::{generate_task_enum, TaskIndex};
use crate::ty_index::TypeIndex;
use anyhow::Context;
use proc_macro2::TokenStream;
use quote::quote;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

// Re-export public types for use in tests and external code
pub use crate::config::{ErrorsConfig, QueriesConfig, TaskQueueConfig, TracingConfig};

pub mod annotations;
pub mod call_graph;
pub mod cardinality_inference;
mod check_expr;
mod codegen;
mod column_grouping;
mod config;
pub mod constraint_analysis;
mod db;
pub mod composite_index;
pub mod domain_index;
mod error_type_index;
mod exceptions;
mod fn_index;
mod ident;
mod parse_check_enum;
mod parse_check_facts;
mod parse_domain;
mod where_predicates;
mod pg_constraint;
mod pg_fn;
mod pg_id;
mod pg_rel;
mod pg_type;
pub mod query_index;
pub mod query_introspector;
pub mod rel_index;
mod row_struct;
pub mod sql_parser;
mod sql_state;
pub(crate) mod task_index;
mod trigger_index;
mod tsvector;
pub mod ty_index;
mod unified_error;
pub mod view_nullability;

// Re-export tsvector types for use in generated code
pub use tsvector::{TsQuery, TsVector};

/// Builder for configuring and running pgrpc code generation
#[derive(Default)]
pub struct PgrpcBuilder {
    config: Config,
    output_path: Option<PathBuf>,
}

impl PgrpcBuilder {
    /// Create a new PgrpcBuilder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the connection string
    pub fn connection_string(mut self, connection_string: impl Into<String>) -> Self {
        self.config.connection_string = Some(connection_string.into());
        self
    }

    /// Add a schema to generate code for
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.config.schemas.push(schema.into());
        self
    }

    /// Add multiple schemas to generate code for
    pub fn schemas(mut self, schemas: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.config.schemas.extend(schemas.into_iter().map(Into::into));
        self
    }

    /// Add a custom type mapping
    pub fn type_mapping(
        mut self,
        pg_type: impl Into<String>,
        rust_type: impl Into<String>,
    ) -> Self {
        self.config.types.insert(pg_type.into(), rust_type.into());
        self
    }

    /// Add a custom exception mapping
    pub fn exception(mut self, sql_state: impl Into<String>, message: impl Into<String>) -> Self {
        self.config.exceptions.insert(sql_state.into(), message.into());
        self
    }

    /// Set the output directory for the generated code
    pub fn output_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.output_path = Some(path.into());
        self
    }

    /// Enable task queue generation with default configuration
    pub fn enable_task_queue(mut self, schema: impl Into<String>) -> Self {
        self.config.task_queue = Some(TaskQueueConfig {
            schema: schema.into(),
            task_name_column: "task_name".to_string(),
            payload_column: "payload".to_string(),
            table_schema: None,
            table_name: None,
        });
        self
    }

    /// Configure the task queue table schema (defaults to "mq")
    pub fn task_queue_table_schema(mut self, schema: impl Into<String>) -> Self {
        let task_config = self.config.task_queue.get_or_insert_with(TaskQueueConfig::default);
        task_config.table_schema = Some(schema.into());
        self
    }

    /// Configure the task queue table name (defaults to "task")
    pub fn task_queue_table_name(mut self, name: impl Into<String>) -> Self {
        let task_config = self.config.task_queue.get_or_insert_with(TaskQueueConfig::default);
        task_config.table_name = Some(name.into());
        self
    }

    /// Configure the task name column (defaults to "task_name")
    pub fn task_queue_task_name_column(mut self, column: impl Into<String>) -> Self {
        let task_config = self.config.task_queue.get_or_insert_with(TaskQueueConfig::default);
        task_config.task_name_column = column.into();
        self
    }

    /// Configure the payload column (defaults to "payload")
    pub fn task_queue_payload_column(mut self, column: impl Into<String>) -> Self {
        let task_config = self.config.task_queue.get_or_insert_with(TaskQueueConfig::default);
        task_config.payload_column = column.into();
        self
    }

    /// Configure the complete task queue settings at once
    pub fn task_queue(mut self, config: TaskQueueConfig) -> Self {
        self.config.task_queue = Some(config);
        self
    }

    /// Enable custom error types with default configuration
    pub fn enable_errors(mut self, schema: impl Into<String>) -> Self {
        self.config.errors = Some(config::ErrorsConfig {
            schema: schema.into(),
            raise_function: Some("core.raise_error".to_string()),
        });
        self
    }

    /// Configure the errors schema
    pub fn errors_schema(mut self, schema: impl Into<String>) -> Self {
        let errors_config = self.config.errors.get_or_insert_with(config::ErrorsConfig::default);
        errors_config.schema = schema.into();
        self
    }

    /// Configure the complete errors settings at once
    pub fn errors(mut self, config: config::ErrorsConfig) -> Self {
        self.config.errors = Some(config);
        self
    }

    /// Enable or disable automatic view nullability inference (defaults to true)
    pub fn infer_view_nullability(mut self, infer: bool) -> Self {
        self.config.infer_view_nullability = infer;
        self
    }

    /// Add a type that should not have Deserialize derived
    pub fn disable_deserialize(mut self, type_name: impl Into<String>) -> Self {
        self.config.disable_deserialize.push(type_name.into());
        self
    }

    /// Add multiple types that should not have Deserialize derived
    pub fn disable_deserialize_types(mut self, types: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.config.disable_deserialize.extend(types.into_iter().map(Into::into));
        self
    }

    /// Mark a domain (as "schema.name") as strict: the generated newtype gets a
    /// private inner field and deserialization is routed through a user-provided
    /// `TryFrom<Inner>` impl.
    pub fn strict_domain(mut self, type_name: impl Into<String>) -> Self {
        self.config.strict_domains.push(type_name.into());
        self
    }

    /// Mark multiple domains (as "schema.name") as strict
    pub fn strict_domains(mut self, types: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.config.strict_domains.extend(types.into_iter().map(Into::into));
        self
    }

    /// Also derive `bon::Builder` for a composite type or view (as "schema.name")
    pub fn builder_type(mut self, type_name: impl Into<String>) -> Self {
        self.config.builders.push(type_name.into());
        self
    }

    /// Also derive `bon::Builder` for several composite types or views
    pub fn builder_types(mut self, types: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.config.builders.extend(types.into_iter().map(Into::into));
        self
    }

    /// Configure SQL query files for code generation
    pub fn queries_config(mut self, config: config::QueriesConfig) -> Self {
        self.config.queries = Some(config);
        self
    }

    /// Add a SQL query file path or glob pattern for code generation
    ///
    /// Can be called multiple times to add multiple paths.
    ///
    /// # Example
    /// ```rust,no_run
    /// use pgrpc::PgrpcBuilder;
    ///
    /// PgrpcBuilder::new()
    ///     .connection_string("postgres://localhost/mydb")
    ///     .schema("public")
    ///     .query_path("queries/**/*.sql")
    ///     .query_path("sql/queries/*.sql")
    ///     .output_path("src/generated")
    ///     .build()
    ///     .expect("Failed to generate code");
    /// ```
    pub fn query_path(mut self, path: impl Into<String>) -> Self {
        let queries_config = self.config.queries.get_or_insert_with(|| config::QueriesConfig {
            paths: Vec::new(),
        });
        queries_config.paths.push(path.into());
        self
    }

    /// Enable or disable tracing spans around generated functions
    ///
    /// When enabled, generated functions will be wrapped with tracing spans
    /// that include the function name and schema. This makes it easier to
    /// identify which function caused an error when using tracing.
    ///
    /// Spans record only the span name (`schema.function` or `queries.name`).
    /// Arguments are never captured, so secrets passed to generated functions
    /// stay out of logs and traces.
    ///
    /// Users must add `tracing = "0.1"` to their Cargo.toml when this is enabled.
    pub fn enable_tracing(mut self, enabled: bool) -> Self {
        let tracing_config = self.config.tracing.get_or_insert_with(config::TracingConfig::default);
        tracing_config.enabled = enabled;
        self
    }

    /// Load configuration from a TOML file
    pub fn from_config_file(config_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let conf_str = fs::read_to_string(config_path)?;
        let config: Config = toml::from_str(&conf_str)?;
        let output_path = config.output_path.as_ref().map(PathBuf::from);

        Ok(Self {
            config,
            output_path,
        })
    }

    /// Generate and write the code to the specified output directory
    pub fn build(&self) -> anyhow::Result<()> {
        // Initialize env_logger if not already initialized
        let _ = env_logger::try_init();

        let output_path = self.output_path.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "Output path is required. Specify either -o in CLI or output_path in pgrpc.toml"
            )
        })?;

        let start = Instant::now();

        let connection_string = if let Some(conn_str) = &self.config.connection_string {
            conn_str.clone()
        } else if let Ok(env_conn_str) = std::env::var("DATABASE_URL") {
            env_conn_str
        } else {
            return Err(anyhow::anyhow!("Connection string is required. Provide it via config file, builder method, or DATABASE_URL environment variable"));
        };

        if self.config.schemas.is_empty() {
            return Err(anyhow::anyhow!("At least one schema must be specified"));
        }
        check_reserved_modules(&self.config)?;

        let config = &self.config;

        let mut db = Db::new(&connection_string)?;
        let rel_index = RelIndex::new(&mut db.client)?;
        // Composite attribute metadata (annotations, domain CHECKs) so view,
        // query, and SQL-function nullability inference can see through
        // `(col).attr` projections.
        let composite_index = composite_index::CompositeIndex::new(&mut db.client)?;
        // Indexes every user schema (so calls into helpers elsewhere can be
        // followed); only `config.schemas` get Rust output. Triggers come
        // after: trigger functions call indexed functions, and the triggers'
        // effects are then layered onto every function whose DML fires them.
        let mut fn_index = FunctionIndex::new(
            &mut db.client,
            &rel_index,
            &config.schemas,
            config.errors.as_ref(),
        )?;
        let trigger_index = trigger_index::TriggerIndex::new(
            &mut db.client,
            &rel_index,
            config.errors.as_ref(),
            &fn_index,
        )?;
        fn_index.apply_triggers(&trigger_index);

        // Collect type OIDs from functions, task types, and error types
        let mut type_oids = fn_index.get_type_oids();

        // Collect view type OIDs
        let view_type_oids = rel_index.get_view_type_oids(&mut db.client)?;
        type_oids.extend(view_type_oids);

        // If task queue is configured, collect task type OIDs as well
        if let Some(task_config) = &config.task_queue {
            let task_type_oids = task_index::collect_task_type_oids(&mut db.client, task_config)?;
            type_oids.extend(task_type_oids);
        }

        // If errors are configured, collect error type OIDs as well
        if let Some(errors_config) = &config.errors {
            let error_type_oids =
                error_type_index::collect_error_type_oids(&mut db.client, errors_config)?;
            type_oids.extend(error_type_oids);
        }

        // Build view nullability cache if enabled
        // This will be shared between TypeIndex (for view types) and QueryIndex (for query analysis)
        let view_nullability_cache = if config.infer_view_nullability {
            build_view_nullability_cache(&mut db.client, &rel_index, &composite_index)?
        } else {
            view_nullability::ViewNullabilityCache::new()
        };

        // If queries are configured, parse and introspect to collect type OIDs
        // We need a temporary TypeIndex for introspection, then rebuild with all OIDs
        let query_index_opt = if let Some(queries_config) = &config.queries {
            // Map of domain name -> OID so cast-to-domain columns
            // (`expr::currency`) keep the domain instead of the base type that
            // prepared-statement metadata reports. Covers all non-system
            // schemas (a cast may target a domain outside the codegen schema
            // list); resolved in-memory during introspection.
            let domain_index = domain_index::DomainIndex::new(&mut db.client)?;

            // Build query index using the shared view nullability cache and trigger index
            let query_index = query_index::QueryIndex::new(
                &connection_string,
                &rel_index,
                &view_nullability_cache,
                queries_config,
                Some(&trigger_index),
                &domain_index,
                &composite_index,
                Some(&fn_index),
                config.errors.as_ref(),
            )?;

            // Collect query type OIDs
            let query_type_oids = query_index.get_type_oids();
            type_oids.extend(query_type_oids);

            Some(query_index)
        } else {
            None
        };

        // Extract CHECK-inferred enum types from relations
        let check_enums = rel_index.get_check_enum_infos();
        log::info!("Found {} CHECK-inferred enum types", check_enums.len());

        let mut ty_index =
            TypeIndex::new_with_check_enums(
            &mut db.client,
            type_oids.as_slice(),
            &check_enums,
            &config.types,
        )?;

        // Apply view nullability inference using the pre-built cache
        if config.infer_view_nullability {
            ty_index.apply_view_nullability_from_cache(&view_nullability_cache)?;
        }

        // Apply nullability inference to SQL language functions with OUT parameters
        if config.infer_view_nullability {
            fn_index.apply_sql_function_nullability(
                &rel_index,
                &view_nullability_cache,
                &composite_index,
            )?;
        }

        let schema_files = codegen_split(&fn_index, &ty_index, &rel_index, config)?;

        // Every generated module, keyed by module name, in the order mod.rs
        // declares them. Schema modules first; the optional extras follow.
        let mut modules: BTreeMap<String, String> = schema_files
            .into_iter()
            .map(|(schema, code)| (module_name(&schema), code))
            .collect();

        if let Some(task_config) = &config.task_queue {
            let task_code = generate_task_code(&mut db.client, task_config, &ty_index, config)
                .context("failed to generate task queue types")?;
            if let Some(task_code) = task_code {
                modules.insert(TASKS_MODULE.to_string(), task_code);
            }
        }

        if let Some(errors_config) = &config.errors {
            let error_code = generate_error_code(&mut db.client, errors_config, &ty_index, config)
                .context("failed to generate custom error types")?;
            modules.insert(CUSTOM_ERRORS_MODULE.to_string(), error_code);
        }

        if let Some(query_index) = &query_index_opt {
            if !query_index.is_empty() {
                let query_code = generate_query_code(query_index, &ty_index, &rel_index, config)
                    .context("failed to generate query code")?;
                modules.insert(QUERIES_MODULE.to_string(), query_code);
            }
        }

        fs::create_dir_all(output_path)?;

        let mut files_written = 0;
        let mut files_unchanged = 0;
        let mut write = |file_name: String, content: &str| -> std::io::Result<()> {
            if write_if_changed(&output_path.join(file_name), content)? {
                files_written += 1;
            } else {
                files_unchanged += 1;
            }
            Ok(())
        };

        for (module, code) in &modules {
            write(format!("{module}.rs"), code)?;
        }
        write("mod.rs".to_string(), &generate_mod_file(&modules))?;

        // Not worth a line each in a build log, but worth knowing about.
        let redundant = annotations::redundant_count();
        if redundant > 0 {
            let note = format!(
                "pgrpc: {redundant} @pgrpc_not_null annotation(s) are redundant \
                 (already inferred); run with RUST_LOG=pgrpc=warn to list them"
            );
            // `OUT_DIR` is set for build scripts, where stderr is hidden.
            if std::env::var_os("OUT_DIR").is_some() {
                println!("cargo:warning={note}");
            } else {
                eprintln!("{note}");
            }
        }

        let duration = start.elapsed().as_secs_f64();
        let total_files = files_written + files_unchanged;

        let update_message = if files_unchanged == 0 {
            format!("{} files written", files_written)
        } else if files_written == 0 {
            format!("{} files unchanged", files_unchanged)
        } else {
            format!(
                "{} files updated, {} unchanged",
                files_written, files_unchanged
            )
        };

        println!(
            "✅  PgRPC: {} ({} total) in {:.2}s",
            update_message, total_files, duration
        );

        Ok(())
    }
}

/// Lint suppressions prepended to every generated file.
const WARNING_IGNORES: &str = "#![allow(dead_code)]\n#![allow(unused_variables)]\n#![allow(unused_imports)]\n#![allow(unused_mut)]\n\n";

const TASKS_MODULE: &str = "tasks";
const CUSTOM_ERRORS_MODULE: &str = "custom_errors";
const QUERIES_MODULE: &str = "queries";

/// Rust module (and file) name for a schema.
fn module_name(schema: &str) -> String {
    crate::ident::sql_to_rs_ident(schema, crate::ident::CaseType::Snake).to_string()
}

/// Generated support modules live next to the per-schema modules, so a schema
/// whose module name matches one of them would be silently overwritten. Two
/// schemas that map to the same module name collide the same way.
fn check_reserved_modules(config: &Config) -> anyhow::Result<()> {
    let mut reserved = vec![
        "errors",
        "nullable",
        "composite",
        "schema_cache",
        codegen::DATETIME_SERDE_MODULE,
        codegen::TSVECTOR_MODULE,
        "mod",
    ];
    if config.task_queue.is_some() {
        reserved.push(TASKS_MODULE);
    }
    if config.errors.is_some() {
        reserved.push(CUSTOM_ERRORS_MODULE);
    }
    if config.queries.is_some() {
        reserved.push(QUERIES_MODULE);
    }

    let mut seen: HashMap<String, &str> = HashMap::new();
    for schema in &config.schemas {
        let module = module_name(schema);
        if reserved.contains(&module.as_str()) {
            anyhow::bail!(
                "schema \"{schema}\" cannot be generated: its module name `{module}` is reserved \
                 for a module pgrpc generates itself"
            );
        }
        if let Some(other) = seen.insert(module.clone(), schema) {
            if other != schema {
                anyhow::bail!(
                    "schemas \"{other}\" and \"{schema}\" both map to the module name `{module}`"
                );
            }
        }
    }
    Ok(())
}

/// Generate task queue code if task types are found
fn generate_task_code(
    db_client: &mut postgres::Client,
    task_config: &TaskQueueConfig,
    ty_index: &TypeIndex,
    config: &Config,
) -> anyhow::Result<Option<String>> {
    let task_index = TaskIndex::new(db_client, task_config)?;

    if task_index.is_empty() {
        return Ok(None);
    }

    let (task_enum_code, referenced_schemas) = generate_task_enum(&task_index, ty_index, config);

    // Generate use statements for referenced schemas
    let schema_imports: Vec<TokenStream> = referenced_schemas
        .into_iter()
        .map(|schema| {
            let schema_ident = quote::format_ident!("{}", module_name(&schema));
            quote! { use super::#schema_ident; }
        })
        .collect();

    let date_serde_module = datetime_serde_reexport();

    let task_code = quote::quote! {
            #(#schema_imports)*

            use serde_json;
            use time;
            use uuid;
            use rust_decimal;
            use std::net::IpAddr;

            #date_serde_module

            #task_enum_code
        }.to_string();

    Ok(Some(WARNING_IGNORES.to_string() + &task_code))
}

/// Generate the custom error module.
///
/// Always emitted when errors are configured: `errors.rs` refers to this
/// module (`PgRpcError::CustomError`, `custom_error_message`) even when the
/// errors schema has no types yet.
fn generate_error_code(
    db_client: &mut postgres::Client,
    errors_config: &config::ErrorsConfig,
    ty_index: &TypeIndex,
    config: &Config,
) -> anyhow::Result<String> {
    let error_index = error_type_index::ErrorTypeIndex::new(db_client, errors_config)?;

    // Error payloads are decoded from the JSON hint, so `disable_deserialize`
    // must not reach them: the generated `from_json` needs the derive.
    let config = &Config {
        disable_deserialize: Vec::new(),
        ..config.clone()
    };

    let (payload_structs, referenced_schemas) =
        error_type_index::generate_error_payload_structs(&error_index, ty_index, config);
    let error_enum = error_type_index::generate_error_type_enum(&error_index);
    let message_lookup = error_type_index::generate_custom_error_message_lookup(&error_index);
    let schema_imports: Vec<TokenStream> = referenced_schemas
        .iter()
        .filter(|s: &&String| s.as_str() != "custom_errors")
        .map(|s| {
            let ident = quote::format_ident!("{}", module_name(s));
            quote::quote! { use super::#ident; }
        })
        .collect();

    let error_code = quote::quote! {
            use serde::{Deserialize, Serialize};
            use serde_json;
            use time;
            use uuid;
            use rust_decimal;
            use std::net::IpAddr;
            #(#schema_imports)*

            #payload_structs

            #error_enum

            #message_lookup
        }.to_string();

    Ok(WARNING_IGNORES.to_string() + &error_code)
}

fn generate_mod_file(modules: &BTreeMap<String, String>) -> String {
    modules
        .keys()
        .map(|module| format!("pub mod {module};\n"))
        .collect()
}

/// Build view nullability cache by analyzing all views in the database
fn build_view_nullability_cache(
    db: &mut postgres::Client,
    rel_index: &RelIndex,
    composite_index: &composite_index::CompositeIndex,
) -> anyhow::Result<view_nullability::ViewNullabilityCache> {
    use petgraph::algo::toposort;
    use petgraph::graph::{DiGraph, NodeIndex};
    use std::collections::{HashMap, HashSet};

    log::info!("Building view nullability cache");

    // Query all views from the database
    let view_query = r#"
        SELECT
            n.nspname as schema,
            c.relname as view_name,
            pg_get_viewdef(c.oid) as view_definition,
            array_agg(a.attname ORDER BY a.attnum) as column_names,
            c.oid as oid
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        WHERE c.relkind IN ('v', 'm')
        GROUP BY n.nspname, c.relname, c.oid
    "#;

    let rows = db.query(view_query, &[])?;

    // Collect all views. Ordered so that graph construction — and therefore
    // the topological sort's tie-breaking — is the same on every run.
    let mut views: BTreeMap<(String, String), (String, Vec<String>)> = BTreeMap::new();
    // The cache and the dependency extraction identify a view by relation OID.
    let mut view_oids: HashMap<(String, String), u32> = HashMap::new();
    let mut views_by_oid: HashMap<u32, (String, String)> = HashMap::new();

    for row in rows {
        let schema: String = row.get(0);
        let view_name: String = row.get(1);
        let view_def: String = row.get(2);
        let column_names: Vec<String> = row.get(3);

        let oid: u32 = row.get(4);

        let view_key = (schema, view_name);
        view_oids.insert(view_key.clone(), oid);
        views_by_oid.insert(oid, view_key.clone());
        views.insert(view_key, (view_def, column_names));
    }

    log::info!("Found {} views to analyze", views.len());

    if views.is_empty() {
        return Ok(view_nullability::ViewNullabilityCache::new());
    }

    // Build dependency graph
    let mut graph = DiGraph::new();
    let mut node_map: HashMap<(String, String), NodeIndex> = HashMap::new();
    let mut node_to_view: HashMap<NodeIndex, (String, String)> = HashMap::new();

    // Add nodes
    for view_key in views.keys() {
        let node = graph.add_node(view_key.clone());
        node_map.insert(view_key.clone(), node);
        node_to_view.insert(node, view_key.clone());
    }

    // Create a temporary cache for dependency extraction
    let temp_cache = view_nullability::ViewNullabilityCache::new();

    // Add edges based on dependencies
    for (view_key, (view_def, _columns)) in &views {
        let analyzer = view_nullability::ViewNullabilityAnalyzer::new(rel_index, &temp_cache);

        // Extract dependencies, but only consider views we're actually analyzing
        if let Ok(raw_deps) = analyzer.extract_view_dependencies(view_def) {
            // Filter to only include views that exist in our set
            let deps: HashSet<_> = raw_deps
                .into_iter()
                .filter_map(|dep| views_by_oid.get(&dep).cloned())
                .collect();

            log::debug!("View {:?} depends on: {:?}", view_key, deps);

            let from_node = node_map[view_key];
            for dep in deps {
                if let Some(&to_node) = node_map.get(&dep) {
                    // Add edge from dependency TO dependent (reversed for topological sort)
                    graph.add_edge(to_node, from_node, ());
                }
            }
        }
    }

    // Perform topological sort
    let sorted_nodes = match toposort(&graph, None) {
        Ok(nodes) => nodes,
        Err(_) => {
            log::warn!("Circular dependency detected in views, falling back to name-ordered analysis");
            let mut nodes: Vec<NodeIndex> = node_to_view.keys().cloned().collect();
            nodes.sort_by(|a, b| node_to_view[a].cmp(&node_to_view[b]));
            nodes
        }
    };

    // Analyze views in topological order
    let mut nullability_cache = view_nullability::ViewNullabilityCache::new();

    for node in sorted_nodes {
        if let Some(view_key) = node_to_view.get(&node) {
            if let Some((view_def, column_names)) = views.get(view_key) {
                log::info!("Analyzing view {:?} in topological order", view_key);

                let mut analyzer =
                    view_nullability::ViewNullabilityAnalyzer::new(rel_index, &nullability_cache)
                        .with_composite_index(composite_index);

                match analyzer.analyze_view(view_def, column_names) {
                    Ok(nullability_map) => {
                        log::info!(
                            "View nullability analysis results for {:?}: {:?}",
                            view_key,
                            nullability_map
                        );

                        // Store in cache for dependent views
                        nullability_cache.insert(
                            view_oids[view_key],
                            &view_key.0,
                            &view_key.1,
                            nullability_map,
                        );
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to analyze view nullability for {:?}: {}",
                            view_key,
                            e
                        );
                    }
                }
            }
        }
    }

    log::info!(
        "Built view nullability cache with {} entries",
        nullability_cache.len()
    );

    Ok(nullability_cache)
}

/// Generate query code from parsed SQL files
fn generate_query_code(
    query_index: &query_index::QueryIndex,
    ty_index: &TypeIndex,
    rel_index: &rel_index::RelIndex,
    config: &Config,
) -> anyhow::Result<String> {
    let query_code_tokens = codegen::codegen_queries(query_index, ty_index, rel_index, config)?;
    let date_serde_module = datetime_serde_reexport();

    let query_code = quote! {
            use postgres_types::private::BytesMut;
            use postgres_types::{IsNull, ToSql, Type};
            use rust_decimal::Decimal;

            #date_serde_module

            #query_code_tokens
        }.to_string();

    Ok(WARNING_IGNORES.to_string() + &query_code)
}

/// Write content to a file only if it has changed
/// Returns true if the file was written, false if it was unchanged
fn write_if_changed(path: &Path, content: &str) -> std::io::Result<bool> {
    // Check if file exists and has the same content
    if path.exists() {
        match fs::read_to_string(path) {
            Ok(existing_content) => {
                if existing_content == content {
                    // Content is the same, no need to write
                    return Ok(false);
                }
            }
            Err(_) => {
                // If we can't read the existing file, we'll write the new content
            }
        }
    }

    // Write the file (either it doesn't exist or content is different)
    fs::write(path, content)?;
    Ok(true)
}
