use crate::codegen::codegen_split;
use crate::config::Config;
use crate::db::Db;
use crate::fn_index::FunctionIndex;
use crate::rel_index::RelIndex;
use crate::task_index::{generate_task_enum, TaskIndex};
use crate::ty_index::TypeIndex;
use proc_macro2::TokenStream;
use quote::quote;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

// Re-export public types for use in tests and external code
pub use crate::config::{ErrorsConfig, QueriesConfig, TaskQueueConfig, TracingConfig};

pub mod annotations;
pub mod cardinality_inference;
mod codegen;
mod config;
pub mod constraint_analysis;
mod db;
mod error_type_index;
mod exceptions;
mod flatten;
mod fn_index;
mod ident;
mod parse_domain;
mod pg_constraint;
mod pg_fn;
mod pg_id;
mod pg_rel;
mod pg_type;
pub mod query_index;
pub mod query_introspector;
pub mod rel_index;
pub mod sql_parser;
mod sql_state;
pub(crate) mod task_index;
mod tests;
mod trigger_index;
mod tsvector;
pub mod ty_index;
mod types;
mod unified_error;
pub mod view_nullability;

// Re-export tsvector types for use in generated code
pub use tsvector::{TsQuery, TsVector};

/// Builder for configuring and running pgrpc code generation
pub struct PgrpcBuilder {
    connection_string: Option<String>,
    schemas: Vec<String>,
    types: HashMap<String, String>,
    exceptions: HashMap<String, String>,
    output_path: Option<PathBuf>,
    task_queue: Option<TaskQueueConfig>,
    errors: Option<config::ErrorsConfig>,
    infer_view_nullability: bool,
    disable_deserialize: Vec<String>,
    queries: Option<config::QueriesConfig>,
    tracing: Option<config::TracingConfig>,
}

impl Default for PgrpcBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PgrpcBuilder {
    /// Create a new PgrpcBuilder
    pub fn new() -> Self {
        Self {
            connection_string: None,
            schemas: Vec::new(),
            types: HashMap::new(),
            exceptions: HashMap::new(),
            output_path: None,
            task_queue: None,
            errors: None,
            infer_view_nullability: true,
            disable_deserialize: Vec::new(),
            queries: None,
            tracing: None,
        }
    }

    /// Set the connection string
    pub fn connection_string(mut self, connection_string: impl Into<String>) -> Self {
        self.connection_string = Some(connection_string.into());
        self
    }

    /// Add a schema to generate code for
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schemas.push(schema.into());
        self
    }

    /// Add multiple schemas to generate code for
    pub fn schemas(mut self, schemas: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.schemas.extend(schemas.into_iter().map(Into::into));
        self
    }

    /// Add a custom type mapping
    pub fn type_mapping(
        mut self,
        pg_type: impl Into<String>,
        rust_type: impl Into<String>,
    ) -> Self {
        self.types.insert(pg_type.into(), rust_type.into());
        self
    }

    /// Add a custom exception mapping
    pub fn exception(mut self, sql_state: impl Into<String>, message: impl Into<String>) -> Self {
        self.exceptions.insert(sql_state.into(), message.into());
        self
    }

    /// Set the output directory for the generated code
    pub fn output_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.output_path = Some(path.into());
        self
    }

    /// Enable task queue generation with default configuration
    pub fn enable_task_queue(mut self, schema: impl Into<String>) -> Self {
        self.task_queue = Some(TaskQueueConfig {
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
        let task_config = self.task_queue.get_or_insert(TaskQueueConfig::default());
        task_config.table_schema = Some(schema.into());
        self
    }

    /// Configure the task queue table name (defaults to "task")
    pub fn task_queue_table_name(mut self, name: impl Into<String>) -> Self {
        let task_config = self.task_queue.get_or_insert(TaskQueueConfig::default());
        task_config.table_name = Some(name.into());
        self
    }

    /// Configure the task name column (defaults to "task_name")
    pub fn task_queue_task_name_column(mut self, column: impl Into<String>) -> Self {
        let task_config = self.task_queue.get_or_insert(TaskQueueConfig::default());
        task_config.task_name_column = column.into();
        self
    }

    /// Configure the payload column (defaults to "payload")
    pub fn task_queue_payload_column(mut self, column: impl Into<String>) -> Self {
        let task_config = self.task_queue.get_or_insert(TaskQueueConfig::default());
        task_config.payload_column = column.into();
        self
    }

    /// Configure the complete task queue settings at once
    pub fn task_queue(mut self, config: TaskQueueConfig) -> Self {
        self.task_queue = Some(config);
        self
    }

    /// Enable custom error types with default configuration
    pub fn enable_errors(mut self, schema: impl Into<String>) -> Self {
        self.errors = Some(config::ErrorsConfig {
            schema: schema.into(),
            raise_function: Some("core.raise_error".to_string()),
        });
        self
    }

    /// Configure the errors schema
    pub fn errors_schema(mut self, schema: impl Into<String>) -> Self {
        let errors_config = self.errors.get_or_insert(config::ErrorsConfig::default());
        errors_config.schema = schema.into();
        self
    }

    /// Configure the complete errors settings at once
    pub fn errors(mut self, config: config::ErrorsConfig) -> Self {
        self.errors = Some(config);
        self
    }

    /// Enable or disable automatic view nullability inference (defaults to true)
    pub fn infer_view_nullability(mut self, infer: bool) -> Self {
        self.infer_view_nullability = infer;
        self
    }

    /// Add a type that should not have Deserialize derived
    pub fn disable_deserialize(mut self, type_name: impl Into<String>) -> Self {
        self.disable_deserialize.push(type_name.into());
        self
    }

    /// Add multiple types that should not have Deserialize derived
    pub fn disable_deserialize_types(mut self, types: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.disable_deserialize.extend(types.into_iter().map(Into::into));
        self
    }

    /// Configure SQL query files for code generation
    pub fn queries_config(mut self, config: config::QueriesConfig) -> Self {
        self.queries = Some(config);
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
        let queries_config = self.queries.get_or_insert_with(|| config::QueriesConfig {
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
    /// Users must add `tracing = "0.1"` to their Cargo.toml when this is enabled.
    pub fn enable_tracing(mut self, enabled: bool) -> Self {
        let tracing_config = self.tracing.get_or_insert(config::TracingConfig::default());
        tracing_config.enabled = enabled;
        self
    }

    /// Load configuration from a TOML file
    pub fn from_config_file(config_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let conf_str = fs::read_to_string(config_path)?;
        let config: Config = toml::from_str(&conf_str)?;

        Ok(Self {
            connection_string: config.connection_string,
            schemas: config.schemas,
            types: config.types,
            exceptions: config.exceptions,
            output_path: config.output_path.map(PathBuf::from),
            task_queue: config.task_queue,
            errors: config.errors,
            infer_view_nullability: config.infer_view_nullability,
            disable_deserialize: config.disable_deserialize,
            queries: config.queries,
            tracing: config.tracing,
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

        let connection_string = if let Some(conn_str) = &self.connection_string {
            conn_str.clone()
        } else if let Ok(env_conn_str) = std::env::var("DATABASE_URL") {
            env_conn_str
        } else {
            return Err(anyhow::anyhow!("Connection string is required. Provide it via config file, builder method, or DATABASE_URL environment variable"));
        };

        if self.schemas.is_empty() {
            return Err(anyhow::anyhow!("At least one schema must be specified"));
        }

        let config = Config {
            connection_string: Some(connection_string.clone()),
            output_path: None, // Not used in internal Config creation
            schemas: self.schemas.clone(),
            types: self.types.clone(),
            exceptions: self.exceptions.clone(),
            task_queue: self.task_queue.clone(),
            errors: self.errors.clone(),
            infer_view_nullability: self.infer_view_nullability,
            disable_deserialize: self.disable_deserialize.clone(),
            queries: self.queries.clone(),
            tracing: self.tracing.clone(),
        };

        let mut db = Db::new(&connection_string)?;
        let rel_index = RelIndex::new(&mut db.client)?;
        let trigger_index =
            trigger_index::TriggerIndex::new(&mut db.client, &rel_index, &config.schemas)?;
        let mut fn_index =
            FunctionIndex::new(&mut db.client, &rel_index, &trigger_index, &config.schemas)?;

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
            build_view_nullability_cache(&mut db.client, &rel_index)?
        } else {
            view_nullability::ViewNullabilityCache::new()
        };

        // If queries are configured, parse and introspect to collect type OIDs
        // We need a temporary TypeIndex for introspection, then rebuild with all OIDs
        let query_index_opt = if let Some(queries_config) = &config.queries {
            // Build a temporary type index with what we have so far
            let temp_ty_index = TypeIndex::new(&mut db.client, type_oids.as_slice())?;

            // Build query index using the shared view nullability cache and trigger index
            let query_index = query_index::QueryIndex::new(
                &mut db.client,
                &rel_index,
                &temp_ty_index,
                &view_nullability_cache,
                queries_config,
                Some(&trigger_index),
            )?;

            // Collect query type OIDs
            let query_type_oids = query_index.get_type_oids();
            type_oids.extend(query_type_oids);

            Some(query_index)
        } else {
            None
        };

        let mut ty_index = TypeIndex::new(&mut db.client, type_oids.as_slice())?;

        // Apply view nullability inference using the pre-built cache
        if config.infer_view_nullability {
            ty_index.apply_view_nullability_from_cache(&view_nullability_cache)?;
        }

        // Apply nullability inference to SQL language functions with OUT parameters
        if config.infer_view_nullability {
            fn_index.apply_sql_function_nullability(&rel_index, &view_nullability_cache)?;
        }

        let schema_files = codegen_split(&fn_index, &ty_index, &rel_index, &config)?;

        // Create output directory if it doesn't exist
        fs::create_dir_all(output_path)?;

        // Track how many files were written vs unchanged
        let mut files_written = 0;
        let mut files_unchanged = 0;

        // Write each schema file
        for (schema, code) in &schema_files {
            let schema_name =
                crate::ident::sql_to_rs_ident(schema, crate::ident::CaseType::Snake).to_string();
            let file_path = output_path.join(format!("{}.rs", schema_name));
            if write_if_changed(&file_path, code)? {
                files_written += 1;
            } else {
                files_unchanged += 1;
            }
        }

        // Generate task enum if task queue is configured
        let mut final_schema_files = schema_files.clone();
        if let Some(task_config) = &config.task_queue {
            match generate_task_code(&mut db.client, task_config, &ty_index, &config) {
                Ok(Some(task_code)) => {
                    let task_file_path = output_path.join("tasks.rs");
                    if write_if_changed(&task_file_path, &task_code)? {
                        files_written += 1;
                    } else {
                        files_unchanged += 1;
                    }
                    final_schema_files.insert("tasks".to_string(), task_code);
                }
                Ok(None) => {
                    // No task types found, skip task generation
                }
                Err(e) => {
                    eprintln!("Warning: Failed to generate task queue types: {}", e);
                }
            }
        }

        // Generate error types if errors are configured
        if let Some(errors_config) = &config.errors {
            match generate_error_code(&mut db.client, errors_config, &ty_index) {
                Ok(Some(error_code)) => {
                    let error_file_path = output_path.join("custom_errors.rs");
                    if write_if_changed(&error_file_path, &error_code)? {
                        files_written += 1;
                    } else {
                        files_unchanged += 1;
                    }
                    final_schema_files.insert("custom_errors".to_string(), error_code);
                }
                Ok(None) => {
                    // No error types found, skip error generation
                }
                Err(e) => {
                    eprintln!("Warning: Failed to generate custom error types: {}", e);
                }
            }
        }

        // Generate queries if configured
        if let Some(query_index) = &query_index_opt {
            if !query_index.is_empty() {
                match generate_query_code(query_index, &ty_index, &rel_index, &config) {
                    Ok(query_code) => {
                        let query_file_path = output_path.join("queries.rs");
                        if write_if_changed(&query_file_path, &query_code)? {
                            files_written += 1;
                        } else {
                            files_unchanged += 1;
                        }
                        final_schema_files.insert("queries".to_string(), query_code);
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to generate query code: {}", e);
                    }
                }
            }
        }

        // Generate and write mod.rs
        let mod_content = generate_mod_file(&final_schema_files);
        if write_if_changed(&output_path.join("mod.rs"), &mod_content)? {
            files_written += 1;
        } else {
            files_unchanged += 1;
        }

        let duration = start.elapsed().as_secs_f64();
        let total_files = files_written + files_unchanged;

        // Create a detailed message based on what was updated
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
    let warning_ignores = "#![allow(dead_code)]\n#![allow(unused_variables)]\n#![allow(unused_imports)]\n#![allow(unused_mut)]\n\n";

    // Generate use statements for referenced schemas
    let schema_imports: Vec<TokenStream> = referenced_schemas
        .into_iter()
        .map(|schema| {
            let schema_ident = quote::format_ident!("{}", schema);
            quote! { use super::#schema_ident; }
        })
        .collect();

    let task_code = prettyplease::unparse(
        &syn::parse2::<syn::File>(quote::quote! {
            #(#schema_imports)*

            use serde_json;
            use time;
            use uuid;
            use rust_decimal;
            use std::net::IpAddr;

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

            #task_enum_code
        })
        .expect("task enum code to parse"),
    );

    Ok(Some(warning_ignores.to_string() + &task_code))
}

/// Generate custom error type code if error types are found
fn generate_error_code(
    db_client: &mut postgres::Client,
    errors_config: &config::ErrorsConfig,
    ty_index: &TypeIndex,
) -> anyhow::Result<Option<String>> {
    let error_index = error_type_index::ErrorTypeIndex::new(db_client, errors_config)?;

    if error_index.is_empty() {
        return Ok(None);
    }

    let config = crate::config::Config {
        errors: Some(errors_config.clone()),
        ..Default::default()
    };
    let payload_structs =
        error_type_index::generate_error_payload_structs(&error_index, ty_index, &config);
    let error_enum = error_type_index::generate_error_type_enum(&error_index);

    let warning_ignores = "#![allow(dead_code)]\n#![allow(unused_variables)]\n#![allow(unused_imports)]\n#![allow(unused_mut)]\n\n";

    let error_code = prettyplease::unparse(
        &syn::parse2::<syn::File>(quote::quote! {
            use serde::{Deserialize, Serialize};
            use serde_json;
            use time;
            use uuid;
            use rust_decimal;
            use std::net::IpAddr;

            #payload_structs

            #error_enum
        })
        .expect("error type code to parse"),
    );

    Ok(Some(warning_ignores.to_string() + &error_code))
}

fn generate_mod_file(schema_files: &HashMap<String, String>) -> String {
    let mut sorted_schemas: Vec<_> = schema_files.keys().collect();
    sorted_schemas.sort();

    let mod_declarations: Vec<String> = sorted_schemas
        .iter()
        .map(|schema| {
            let schema_name =
                crate::ident::sql_to_rs_ident(schema, crate::ident::CaseType::Snake).to_string();
            format!("pub mod {};", schema_name)
        })
        .collect();

    mod_declarations.join("\n") + "\n"
}

/// Build view nullability cache by analyzing all views in the database
fn build_view_nullability_cache(
    db: &mut postgres::Client,
    rel_index: &RelIndex,
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
            array_agg(a.attname ORDER BY a.attnum) as column_names
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        WHERE c.relkind IN ('v', 'm')
        GROUP BY n.nspname, c.relname, c.oid
    "#;

    let rows = db.query(view_query, &[])?;

    // Collect all views
    let mut views: HashMap<(Option<String>, String), (String, Vec<String>)> = HashMap::new();

    for row in rows {
        let schema: String = row.get(0);
        let view_name: String = row.get(1);
        let view_def: String = row.get(2);
        let column_names: Vec<String> = row.get(3);

        let schema_opt = if schema == "public" {
            None
        } else {
            Some(schema)
        };

        views.insert((schema_opt, view_name), (view_def, column_names));
    }

    log::info!("Found {} views to analyze", views.len());

    if views.is_empty() {
        return Ok(view_nullability::ViewNullabilityCache::new());
    }

    // Build dependency graph
    let mut graph = DiGraph::new();
    let mut node_map: HashMap<(Option<String>, String), NodeIndex> = HashMap::new();
    let mut node_to_view: HashMap<NodeIndex, (Option<String>, String)> = HashMap::new();

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
                .filter(|dep| views.contains_key(dep))
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
            log::warn!("Circular dependency detected in views, falling back to unordered analysis");
            node_to_view.keys().cloned().collect()
        }
    };

    // Analyze views in topological order
    let mut nullability_cache = view_nullability::ViewNullabilityCache::new();

    for node in sorted_nodes {
        if let Some(view_key) = node_to_view.get(&node) {
            if let Some((view_def, column_names)) = views.get(view_key) {
                log::info!("Analyzing view {:?} in topological order", view_key);

                let mut analyzer =
                    view_nullability::ViewNullabilityAnalyzer::new(rel_index, &nullability_cache);

                match analyzer.analyze_view(view_def, column_names) {
                    Ok(nullability_map) => {
                        log::info!(
                            "View nullability analysis results for {:?}: {:?}",
                            view_key,
                            nullability_map
                        );

                        // Store in cache for dependent views
                        nullability_cache.insert(view_key.clone(), nullability_map);
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
    let warning_ignores = "#![allow(dead_code)]\n#![allow(unused_variables)]\n#![allow(unused_imports)]\n#![allow(unused_mut)]\n\n";

    let query_code_tokens = codegen::codegen_queries(query_index, ty_index, rel_index, config);

    let query_code = prettyplease::unparse(
        &syn::parse2::<syn::File>(quote! {
            use postgres_types::private::BytesMut;
            use postgres_types::{IsNull, ToSql, Type};
            use rust_decimal::Decimal;

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

            #query_code_tokens
        })
        .expect("query code to parse"),
    );

    Ok(warning_ignores.to_string() + &query_code)
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
