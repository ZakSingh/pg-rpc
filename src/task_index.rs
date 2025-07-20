use crate::codegen::OID;
use crate::config::{Config, TaskQueueConfig};
use crate::ty_index::TypeIndex;
use crate::ident::{sql_to_rs_string, CaseType};
use anyhow::Context;
use itertools::Itertools;
use postgres::Client;
use proc_macro2::TokenStream;
use quote::{quote, format_ident};
use serde_json::Value;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

const TASK_INTROSPECTION_QUERY: &'static str = include_str!("./queries/task_introspection.sql");

#[derive(Debug, Clone)]
pub struct TaskType {
    pub task_name: String,
    pub type_oid: OID,
    pub fields: Vec<TaskField>,
}

#[derive(Debug, Clone)]
pub struct TaskField {
    pub name: String,
    pub type_oid: OID,
    pub postgres_type: String,
    pub position: i32,
    pub not_null: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Default)]
pub struct TaskIndex(HashMap<String, TaskType>);

impl Deref for TaskIndex {
    type Target = HashMap<String, TaskType>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TaskIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TaskIndex {
    /// Construct the task index by introspecting the configured task schema
    pub fn new(db: &mut Client, task_config: &TaskQueueConfig) -> anyhow::Result<Self> {
        let task_types = db
            .query(TASK_INTROSPECTION_QUERY, &[&task_config.schema])
            .context("Task introspection query failed")?
            .into_iter()
            .map(|row| {
                let task_name: String = row.try_get("task_name")?;
                let type_oid: u32 = row.try_get("type_oid")?;
                let fields_json: Value = row.try_get("fields")?;
                
                let fields: Vec<TaskField> = serde_json::from_value::<Vec<Value>>(fields_json)
                    .context("Failed to parse fields JSON")?
                    .into_iter()
                    .map(|field_json: Value| -> Result<TaskField, serde_json::Error> {
                        Ok(TaskField {
                            name: field_json["name"].as_str().unwrap().to_string(),
                            type_oid: field_json["type_oid"].as_u64().unwrap() as u32,
                            postgres_type: field_json["postgres_type"].as_str().unwrap().to_string(),
                            position: field_json["position"].as_i64().unwrap() as i32,
                            not_null: field_json["not_null"].as_bool().unwrap(),
                            comment: field_json["comment"].as_str().map(|s| s.to_string()),
                        })
                    })
                    .try_collect()?;

                Ok::<_, anyhow::Error>((
                    task_name.clone(),
                    TaskType {
                        task_name,
                        type_oid,
                        fields,
                    }
                ))
            })
            .try_collect()?;

        Ok(Self(task_types))
    }
    
    /// Get all task names in the index
    pub fn task_names(&self) -> Vec<String> {
        self.0.keys().cloned().collect()
    }
    
    /// Check if the index is empty (no task types found)
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Generate task payload enum from task index
pub fn generate_task_enum(
    task_index: &TaskIndex,
    type_index: &TypeIndex,
    config: &Config,
) -> TokenStream {
    if task_index.is_empty() {
        return TokenStream::new();
    }

    let task_config = config.task_queue.as_ref().expect("Task queue config should be present");
    
    // Generate enum variants
    let variants: Vec<TokenStream> = task_index
        .values()
        .map(|task_type| {
            generate_task_variant(task_type, type_index, config)
        })
        .collect();

    // Generate match arms for from_database_row
    let from_arms: Vec<TokenStream> = task_index
        .values()
        .map(|task_type| {
            let task_name = &task_type.task_name;
            let variant_name = format_ident!("{}", sql_to_rs_string(task_name, CaseType::Pascal));
            
            quote! {
                #task_name => serde_json::from_value(payload).map(TaskPayload::#variant_name)
            }
        })
        .collect();

    // Generate match arms for task_name method
    let name_arms: Vec<TokenStream> = task_index
        .values()
        .map(|task_type| {
            let task_name = &task_type.task_name;
            let variant_name = format_ident!("{}", sql_to_rs_string(task_name, CaseType::Pascal));
            
            quote! {
                TaskPayload::#variant_name { .. } => #task_name
            }
        })
        .collect();

    let task_name_column = &task_config.task_name_column;
    let payload_column = &task_config.payload_column;
    let full_table_name = task_config.get_full_table_name();
    let task_schema = &task_config.schema;

    let enum_doc = format!(
        "Task payload enum generated from PostgreSQL composite types in the '{}' schema.\n\nThis enum is designed to work with a task queue table '{}' with the following structure:\n- {}: TEXT (task type identifier)\n- {}: JSONB (task payload data)\n\nTask types are defined as composite types in the '{}' schema and automatically\nconverted to strongly-typed Rust enum variants.",
        task_schema, full_table_name, task_name_column, payload_column, task_schema
    );

    quote! {
        #[doc = #enum_doc]
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        #[serde(tag = #task_name_column, content = #payload_column)]
        pub enum TaskPayload {
            #(#variants),*
        }

        impl TaskPayload {
            /// Get the task type name for this payload
            pub fn task_name(&self) -> &'static str {
                match self {
                    #(#name_arms),*
                }
            }

            /// Deserialize a task payload from database row data
            pub fn from_database_row(task_name: &str, payload: serde_json::Value) -> Result<Self, serde_json::Error> {
                match task_name {
                    #(#from_arms),*
                    _ => Err(serde_json::Error::custom(format!("Unknown task: {}", task_name))),
                }
            }
            
            /// Get the configured task queue table name
            pub const fn table_name() -> &'static str {
                #full_table_name
            }
            
            /// Get the configured task name column
            pub const fn task_name_column() -> &'static str {
                #task_name_column
            }
            
            /// Get the configured payload column
            pub const fn payload_column() -> &'static str {
                #payload_column
            }
        }
    }
}

/// Generate a single task variant
fn generate_task_variant(
    task_type: &TaskType,
    type_index: &TypeIndex,
    _config: &Config,
) -> TokenStream {
    let variant_name = format_ident!("{}", sql_to_rs_string(&task_type.task_name, CaseType::Pascal));
    let task_name = &task_type.task_name;
    
    let fields: Vec<TokenStream> = task_type.fields
        .iter()
        .map(|field| {
            let field_name = format_ident!("{}", sql_to_rs_string(&field.name, CaseType::Snake));
            
            // Look up the PostgreSQL type in our type index to get the Rust type
            let rust_type = if let Some(pg_type) = type_index.get(&field.type_oid) {
                pg_type.to_rust_ident(type_index)
            } else {
                // Fallback to a basic type mapping
                map_postgres_type_to_rust(&field.postgres_type)
            };
            
            quote! { pub #field_name: #rust_type }
        })
        .collect();

    quote! {
        #[serde(rename = #task_name)]
        #variant_name {
            #(#fields),*
        }
    }
}

/// Basic PostgreSQL to Rust type mapping for task fields
fn map_postgres_type_to_rust(postgres_type: &str) -> TokenStream {
    match postgres_type {
        "bigint" | "int8" => quote! { i64 },
        "integer" | "int4" => quote! { i32 },
        "smallint" | "int2" => quote! { i16 },
        "text" | "varchar" | "char" => quote! { String },
        "boolean" | "bool" => quote! { bool },
        "real" | "float4" => quote! { f32 },
        "double precision" | "float8" => quote! { f64 },
        "numeric" | "decimal" => quote! { rust_decimal::Decimal },
        "json" | "jsonb" => quote! { serde_json::Value },
        "uuid" => quote! { uuid::Uuid },
        "timestamp" | "timestamptz" => quote! { chrono::DateTime<chrono::Utc> },
        "date" => quote! { chrono::NaiveDate },
        "time" => quote! { chrono::NaiveTime },
        "bytea" => quote! { Vec<u8> },
        "inet" => quote! { std::net::IpAddr },
        _ => {
            // For complex types, fall back to String and let serde handle it
            quote! { String }
        }
    }
}