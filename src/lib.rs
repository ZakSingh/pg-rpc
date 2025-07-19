#![feature(iterator_try_collect)]

use crate::codegen::codegen_split;
use crate::config::Config;
use crate::db::Db;
use crate::fn_index::FunctionIndex;
use crate::rel_index::RelIndex;
use crate::ty_index::TypeIndex;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

mod codegen;
mod config;
mod db;
mod exceptions;
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
mod ty_index;

/// Builder for configuring and running pgrpc code generation
pub struct PgrpcBuilder {
    connection_string: Option<String>,
    schemas: Vec<String>,
    types: HashMap<String, String>,
    exceptions: HashMap<String, String>,
    output_path: Option<PathBuf>,
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
            connection_string: Some(config.connection_string),
            schemas: config.schemas,
            types: config.types,
            exceptions: config.exceptions,
            output_path: None,
        })
    }


    /// Generate and write the code to the specified output directory
    pub fn build(&self) -> anyhow::Result<()> {
        let output_path = self.output_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Output path is required"))?;
        
        let start = Instant::now();
        
        let connection_string = self.connection_string
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Connection string is required"))?;
        
        if self.schemas.is_empty() {
            return Err(anyhow::anyhow!("At least one schema must be specified"));
        }

        let config = Config {
            connection_string: connection_string.clone(),
            schemas: self.schemas.clone(),
            types: self.types.clone(),
            exceptions: self.exceptions.clone(),
        };

        let mut db = Db::new(&config.connection_string)?;
        let rel_index = RelIndex::new(&mut db.client)?;
        let fn_index = FunctionIndex::new(&mut db.client, &rel_index, &config.schemas)?;
        let ty_index = TypeIndex::new(&mut db.client, fn_index.get_type_oids().as_slice())?;
        
        let schema_files = codegen_split(&fn_index, &ty_index, &config)?;
        
        // Create output directory if it doesn't exist
        fs::create_dir_all(output_path)?;
        
        // Write each schema file
        for (schema, code) in &schema_files {
            let schema_name = crate::ident::sql_to_rs_ident(schema, crate::ident::CaseType::Snake).to_string();
            let file_path = output_path.join(format!("{}.rs", schema_name));
            fs::write(&file_path, code)?;
        }
        
        // Generate and write mod.rs
        let mod_content = generate_mod_file(&schema_files);
        fs::write(output_path.join("mod.rs"), mod_content)?;
        
        let duration = start.elapsed().as_secs_f64();
        println!(
            "✅  PgRPC functions written to {} ({} files) in {:.2}s",
            output_path.display(),
            schema_files.len() + 1, // +1 for mod.rs
            duration
        );

        Ok(())
    }
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

