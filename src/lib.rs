#![feature(iterator_try_collect)]

use crate::codegen::codegen_split;
use crate::config::{Config, TaskQueueConfig};
use crate::db::Db;
use crate::fn_index::FunctionIndex;
use crate::rel_index::RelIndex;
use crate::ty_index::TypeIndex;
use crate::task_index::{TaskIndex, generate_task_enum};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use proc_macro2::TokenStream;
use quote::quote;

mod types;
mod config;
mod db;
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
mod rel_index;
mod sql_state;
mod tests;
mod trigger_index;
mod ty_index;
mod unified_error;
pub(crate) mod task_index;
mod codegen;

/// Builder for configuring and running pgrpc code generation
pub struct PgrpcBuilder {
    connection_string: Option<String>,
    schemas: Vec<String>,
    types: HashMap<String, String>,
    exceptions: HashMap<String, String>,
    output_path: Option<PathBuf>,
    task_queue: Option<TaskQueueConfig>,
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
    pub fn type_mapping(mut self, pg_type: impl Into<String>, rust_type: impl Into<String>) -> Self {
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
        })
    }


    /// Generate and write the code to the specified output directory
    pub fn build(&self) -> anyhow::Result<()> {
        // Initialize env_logger if not already initialized
        let _ = env_logger::try_init();
        
        let output_path = self.output_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Output path is required. Specify either -o in CLI or output_path in pgrpc.toml"))?;
        
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
        };

        let mut db = Db::new(&connection_string)?;
        let rel_index = RelIndex::new(&mut db.client)?;
        let trigger_index = trigger_index::TriggerIndex::new(&mut db.client, &rel_index, &config.schemas)?;
        let fn_index = FunctionIndex::new(&mut db.client, &rel_index, &trigger_index, &config.schemas)?;
        
        // Collect type OIDs from both functions and task types
        let mut type_oids = fn_index.get_type_oids();
        
        // If task queue is configured, collect task type OIDs as well
        if let Some(task_config) = &config.task_queue {
            let task_type_oids = task_index::collect_task_type_oids(&mut db.client, task_config)?;
            type_oids.extend(task_type_oids);
        }
        
        let ty_index = TypeIndex::new(&mut db.client, type_oids.as_slice())?;
        
        let schema_files = codegen_split(&fn_index, &ty_index, &rel_index, &config)?;
        
        // Create output directory if it doesn't exist
        fs::create_dir_all(output_path)?;
        
        // Track how many files were written vs unchanged
        let mut files_written = 0;
        let mut files_unchanged = 0;
        
        // Write each schema file
        for (schema, code) in &schema_files {
            let schema_name = crate::ident::sql_to_rs_ident(schema, crate::ident::CaseType::Snake).to_string();
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
            format!("{} files updated, {} unchanged", files_written, files_unchanged)
        };
        
        println!(
            "✅  PgRPC: {} ({} total) in {:.2}s",
            update_message,
            total_files,
            duration
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
            
            #task_enum_code
        })
        .expect("task enum code to parse"),
    );
    
    Ok(Some(warning_ignores.to_string() + &task_code))
}

fn generate_mod_file(schema_files: &HashMap<String, String>) -> String {
    let mut sorted_schemas: Vec<_> = schema_files.keys().collect();
    sorted_schemas.sort();
    
    let mod_declarations: Vec<String> = sorted_schemas
        .iter()
        .map(|schema| {
            let schema_name = crate::ident::sql_to_rs_ident(schema, crate::ident::CaseType::Snake).to_string();
            format!("pub mod {};", schema_name)
        })
        .collect();
    
    mod_declarations.join("\n") + "\n"
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

