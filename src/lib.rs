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
        let ty_index = TypeIndex::new(&mut db.client, fn_index.get_type_oids().as_slice())?;
        
        let schema_files = codegen_split(&fn_index, &ty_index, &rel_index, &config)?;
        
        // Create output directory if it doesn't exist
        fs::create_dir_all(output_path)?;
        
        // Write each schema file
        for (schema, code) in &schema_files {
            let schema_name = crate::ident::sql_to_rs_ident(schema, crate::ident::CaseType::Snake).to_string();
            let file_path = output_path.join(format!("{}.rs", schema_name));
            fs::write(&file_path, code)?;
        }
        
        // Generate task enum if task queue is configured
        let mut final_schema_files = schema_files.clone();
        if let Some(task_config) = &config.task_queue {
            match generate_task_code(&mut db.client, task_config, &ty_index, &config) {
                Ok(Some(task_code)) => {
                    let task_file_path = output_path.join("tasks.rs");
                    fs::write(&task_file_path, &task_code)?;
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
        fs::write(output_path.join("mod.rs"), mod_content)?;
        
        let duration = start.elapsed().as_secs_f64();
        println!(
            "✅  PgRPC functions written to {} ({} files) in {:.2}s",
            output_path.display(),
            final_schema_files.len() + 1, // +1 for mod.rs
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
    
    let task_enum_code = generate_task_enum(&task_index, ty_index, config);
    let warning_ignores = "#![allow(dead_code)]\n#![allow(unused_variables)]\n#![allow(unused_imports)]\n#![allow(unused_mut)]\n\n";
    
    let task_code = prettyplease::unparse(
        &syn::parse2::<syn::File>(quote::quote! {
            use serde_json;
            use chrono;
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

