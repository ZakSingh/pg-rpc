use crate::codegen::OID;
use crate::config::{Config, TaskQueueConfig};
use crate::ident::{sql_to_rs_string, CaseType};
use crate::pg_type::PgType;
use crate::ty_index::TypeIndex;
use anyhow::Context;
use postgres::Client;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};

const TASK_INTROSPECTION_QUERY: &'static str = include_str!("./queries/task_introspection.sql");

#[derive(Debug, Clone)]
pub struct TaskType {
    pub task_name: String,
    pub type_oid: OID,
    pub fields: Vec<TaskField>,
    pub comment: Option<String>,
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
        eprintln!(
            "[PGRPC] Introspecting task types from schema: {}",
            task_config.schema
        );

        // First, let's log the actual SQL query being executed
        eprintln!(
            "[PGRPC] Executing task introspection query with schema parameter: '{}'",
            task_config.schema
        );

        let query_result = db
            .query(TASK_INTROSPECTION_QUERY, &[&task_config.schema])
            .context("Task introspection query failed")?;

        eprintln!(
            "[PGRPC] Task introspection query returned {} rows",
            query_result.len()
        );

        let task_types: HashMap<String, TaskType> = query_result
            .into_iter()
            .enumerate()
            .map(|(row_idx, row)| {
                eprintln!("[PGRPC] Processing row {}", row_idx);

                let task_name: String = row.try_get("task_name")?;
                let type_oid: u32 = row.try_get("type_oid")?;
                let type_comment: Option<String> = row.try_get("type_comment")?;
                let fields_json: Value = row.try_get("fields")?;

                eprintln!("[PGRPC] Row {}: Processing task type '{}' (OID: {})", row_idx, task_name, type_oid);
                eprintln!("[PGRPC] Row {}: Raw fields JSON: {}", row_idx, fields_json);

                // Check if fields_json is null or empty array
                let fields_vec = if fields_json.is_null() {
                    eprintln!("[PGRPC] Row {}: Fields JSON is null for task '{}'", row_idx, task_name);
                    Vec::new()
                } else {
                    match serde_json::from_value::<Vec<Value>>(fields_json.clone()) {
                        Ok(vec) => {
                            eprintln!("[PGRPC] Row {}: Successfully parsed {} field entries for task '{}'", row_idx, vec.len(), task_name);
                            for (field_idx, field_json) in vec.iter().enumerate() {
                                eprintln!("[PGRPC] Row {}: Field {}: {}", row_idx, field_idx, field_json);
                            }
                            vec
                        }
                        Err(e) => {
                            eprintln!("[PGRPC] Row {}: Failed to parse fields JSON for task '{}': {}", row_idx, task_name, e);
                            eprintln!("[PGRPC] Row {}: Problematic JSON: {}", row_idx, fields_json);
                            return Err(e.into());
                        }
                    }
                };

                let fields: Vec<TaskField> = fields_vec
                    .into_iter()
                    .filter_map(|field_json: Value| {
                        // Extract all required fields, logging and skipping if any are missing
                        let name = field_json["name"].as_str();
                        // Handle type_oid as either number or string
                        let type_oid = field_json["type_oid"].as_u64()
                            .or_else(|| field_json["type_oid"].as_str().and_then(|s| s.parse().ok()));
                        let postgres_type = field_json["postgres_type"].as_str();
                        let position = field_json["position"].as_i64();
                        let not_null = field_json["not_null"].as_bool();

                        match (name, type_oid, postgres_type, position, not_null) {
                            (Some(n), Some(t), Some(pt), Some(p), Some(nn)) => {
                                eprintln!("[PGRPC]   Field '{}': {} (OID: {})", n, pt, t);
                                Some(TaskField {
                                    name: n.to_string(),
                                    type_oid: t as u32,
                                    postgres_type: pt.to_string(),
                                    position: p as i32,
                                    not_null: nn,
                                    comment: field_json["comment"].as_str().map(|s| s.to_string()),
                                })
                            }
                            _ => {
                                eprintln!("[PGRPC] Skipping malformed task field JSON: {:?}", field_json);
                                None
                            }
                        }
                    })
                    .collect();

                eprintln!("[PGRPC] Task '{}' has {} fields after filtering", task_name, fields.len());

                Ok::<_, anyhow::Error>((
                    task_name.clone(),
                    TaskType {
                        task_name,
                        type_oid,
                        fields,
                        comment: type_comment,
                    }
                ))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        eprintln!(
            "[PGRPC] Found {} task types in schema '{}'",
            task_types.len(),
            task_config.schema
        );
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

    /// Collect all type OIDs referenced by task types
    pub fn collect_type_oids(&self) -> Vec<OID> {
        let mut oids = HashSet::new();

        for task_type in self.0.values() {
            // Add the composite type itself
            oids.insert(task_type.type_oid);

            // Add all field type OIDs
            for field in &task_type.fields {
                oids.insert(field.type_oid);
            }
        }

        oids.into_iter().collect()
    }
}

/// Collect all type OIDs from task types in the database
pub fn collect_task_type_oids(
    db_client: &mut postgres::Client,
    task_config: &TaskQueueConfig,
) -> anyhow::Result<Vec<OID>> {
    let mut oids = HashSet::new();

    // Get all composite types in the task schema AND their field types
    let query = r#"
        WITH task_types AS (
            SELECT
                t.oid as type_oid,
                t.typrelid
            FROM pg_type t
            WHERE t.typnamespace = (
                SELECT oid FROM pg_namespace WHERE nspname = $1
            )
              AND t.typtype = 'c'  -- composite types only
        )
        SELECT DISTINCT oid FROM (
            -- Include the composite type OIDs
            SELECT type_oid as oid FROM task_types

            UNION

            -- Include all field type OIDs
            SELECT a.atttypid as oid
            FROM task_types tt
            JOIN pg_attribute a ON a.attrelid = tt.typrelid
            WHERE a.attnum > 0 AND NOT a.attisdropped
        ) AS all_oids
    "#;

    let rows = db_client.query(query, &[&task_config.schema])?;

    eprintln!(
        "[PGRPC] collect_task_type_oids: Query returned {} rows",
        rows.len()
    );

    for row in rows {
        let type_oid: OID = row.try_get("oid")?;
        eprintln!("[PGRPC] collect_task_type_oids: Adding OID {}", type_oid);
        oids.insert(type_oid);
    }

    eprintln!(
        "[PGRPC] collect_task_type_oids: Collected {} type OIDs from task schema '{}'",
        oids.len(),
        task_config.schema
    );

    Ok(oids.into_iter().collect())
}

/// Generate task payload enum from task index
pub fn generate_task_enum(
    task_index: &TaskIndex,
    type_index: &TypeIndex,
    config: &Config,
) -> (TokenStream, HashSet<String>) {
    let mut referenced_schemas = HashSet::new();

    if task_index.is_empty() {
        return (TokenStream::new(), referenced_schemas);
    }

    let task_config = config
        .task_queue
        .as_ref()
        .expect("Task queue config should be present");

    // First, generate all payload structs
    let payload_structs: Vec<TokenStream> = task_index
        .values()
        .map(|task_type| {
            let (struct_def, schemas) = generate_payload_struct(task_type, type_index, config);
            referenced_schemas.extend(schemas);
            struct_def
        })
        .collect();

    // Then generate enum variants (which now reference the payload structs)
    let variants: Vec<TokenStream> = task_index
        .values()
        .map(|task_type| generate_task_variant(task_type, type_index, config))
        .collect();

    let task_name_column = &task_config.task_name_column;
    let payload_column = &task_config.payload_column;

    // Generate match arms for from_database_row
    let from_arms: Vec<TokenStream> = task_index
        .values()
        .map(|task_type| {
            let task_name = &task_type.task_name;

            quote! {
                #task_name => {
                    let tagged_value = serde_json::json!({
                        #task_name_column: task_name,
                        #payload_column: payload
                    });
                    serde_json::from_value(tagged_value)
                }
            }
        })
        .collect();

    // Generate match arms for task_name method (now handling tuple variants)
    let name_arms: Vec<TokenStream> = task_index
        .values()
        .map(|task_type| {
            let task_name = &task_type.task_name;
            let variant_name = format_ident!("{}", sql_to_rs_string(task_name, CaseType::Pascal));

            quote! {
                TaskPayload::#variant_name(_) => #task_name
            }
        })
        .collect();
    let full_table_name = task_config.get_full_table_name();
    let task_schema = &task_config.schema;

    let enum_doc = format!(
        "Task payload enum generated from PostgreSQL composite types in the '{}' schema.\n\nThis enum is designed to work with a task queue table '{}' with the following structure:\n- {}: TEXT (task type identifier)\n- {}: JSONB (task payload data)\n\nTask types are defined as composite types in the '{}' schema and automatically\nconverted to strongly-typed Rust enum variants.",
        task_schema, full_table_name, task_name_column, payload_column, task_schema
    );

    let enum_code = quote! {
        // Generate all payload structs first
        #(#payload_structs)*

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
                    #(#from_arms),*,
                    _ => {
                        // Create a JSON object that will fail to deserialize into TaskPayload
                        let error_json = serde_json::json!({
                            #task_name_column: format!("__unknown_task_{}", task_name),
                            #payload_column: {}
                        });
                        serde_json::from_value(error_json)
                    }
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
    };

    (enum_code, referenced_schemas)
}

/// Generate a payload struct for a task type
fn generate_payload_struct(
    task_type: &TaskType,
    type_index: &TypeIndex,
    config: &Config,
) -> (TokenStream, std::collections::HashSet<String>) {
    let mut referenced_schemas = std::collections::HashSet::new();
    let struct_name = format_ident!(
        "{}Payload",
        sql_to_rs_string(&task_type.task_name, CaseType::Pascal)
    );
    let task_name = &task_type.task_name;

    eprintln!(
        "[PGRPC] Generating payload struct for task '{}' with {} fields",
        task_name,
        task_type.fields.len()
    );

    // Parse bulk not null annotations from type-level comment
    let bulk_not_null_columns = crate::pg_type::parse_bulk_not_null_columns(&task_type.comment);
    eprintln!("[PGRPC] Task type comment: {:?}", task_type.comment);
    eprintln!("[PGRPC] Bulk not null columns: {:?}", bulk_not_null_columns);

    // Debug: print all fields
    for (idx, field) in task_type.fields.iter().enumerate() {
        eprintln!("[PGRPC]   Field {}: name='{}', type_oid={}, postgres_type='{}', not_null={}, comment={:?}",
                  idx, field.name, field.type_oid, field.postgres_type, field.not_null, field.comment);
    }

    let fields: Vec<TokenStream> = task_type.fields
        .iter()
        .map(|field| {
            let field_name = format_ident!("{}", sql_to_rs_string(&field.name, CaseType::Snake));

            // Generate field type with proper nullability handling
            let rust_type = if let Some(pg_type) = type_index.get(&field.type_oid) {
                eprintln!("[PGRPC]     Field '{}' (OID {}) found in TypeIndex", field.name, field.type_oid);
                let (type_tokens, schemas) = pg_type.to_rust_ident_with_schemas(type_index);
                referenced_schemas.extend(schemas);

                // Apply nullability logic consistent with existing composite types
                // Fields are nullable by default, unless they have:
                // 1. @pgrpc_not_null in their field-level comment OR
                // 2. Their name is in the bulk_not_null_columns set from type-level comment
                let is_nullable = !field.comment
                    .as_ref()
                    .is_some_and(|c| c.contains("@pgrpc_not_null"))
                    && !bulk_not_null_columns.contains(&field.name);

                if is_nullable {
                    quote! { Option<#type_tokens> }
                } else {
                    type_tokens
                }
            } else {
                // Use fallback mapping with proper nullability handling
                eprintln!("[PGRPC]     Field '{}' (OID {}) NOT found in TypeIndex, using fallback mapping for '{}'",
                         field.name, field.type_oid, field.postgres_type);
                generate_task_field_type(field, type_index, &bulk_not_null_columns)
            };

            quote! { pub #field_name: #rust_type }
        })
        .collect();

    eprintln!("[PGRPC]   Generated {} field tokens", fields.len());

    // Get the task queue schema from config
    let task_schema = config
        .task_queue
        .as_ref()
        .map(|tc| tc.schema.as_str())
        .unwrap_or("tasks");

    // Generate struct name in format that matches the pattern
    let full_type_name = format!("{}.{}", task_schema, struct_name.to_string());
    
    let deserialize_derive = if config.should_disable_deserialize(task_schema, &struct_name.to_string()) {
        quote! {}
    } else {
        quote! { serde::Deserialize, }
    };

    let payload_struct = quote! {
        #[derive(Debug, Clone, serde::Serialize, #deserialize_derive)]
        pub struct #struct_name {
            #(#fields),*
        }
    };

    (payload_struct, referenced_schemas)
}

/// Generate a single task variant (now using tuple syntax)
fn generate_task_variant(
    task_type: &TaskType,
    _type_index: &TypeIndex,
    _config: &Config,
) -> TokenStream {
    let variant_name = format_ident!(
        "{}",
        sql_to_rs_string(&task_type.task_name, CaseType::Pascal)
    );
    let payload_struct_name = format_ident!(
        "{}Payload",
        sql_to_rs_string(&task_type.task_name, CaseType::Pascal)
    );
    let task_name = &task_type.task_name;

    log::debug!("Generating tuple variant for task '{}'", task_name);

    // For empty payloads, we still generate a struct, but it will be empty
    quote! {
        #[serde(rename = #task_name)]
        #variant_name(#payload_struct_name)
    }
}

/// Generate Rust type for a task field, properly handling nullability and @pgrpc_not_null annotations
fn generate_task_field_type(field: &TaskField, type_index: &HashMap<OID, PgType>, bulk_not_null_columns: &HashSet<String>) -> TokenStream {
    // Determine if field should be nullable
    // Fields are nullable by default in PostgreSQL composite types
    // They become NOT nullable if:
    // 1. They have @pgrpc_not_null comment OR
    // 2. Their name is in the bulk_not_null_columns set from type-level comment
    let is_nullable = !field
        .comment
        .as_ref()
        .is_some_and(|c| c.contains("@pgrpc_not_null"))
        && !bulk_not_null_columns.contains(&field.name);

    // Get the base Rust type
    let base_type = if let Some(pg_type) = type_index.get(&field.type_oid) {
        pg_type.to_rust_ident(type_index)
    } else {
        // Fallback to basic type mapping
        map_postgres_type_to_rust(&field.postgres_type)
    };

    // Wrap in Option if nullable
    if is_nullable {
        quote! { Option<#base_type> }
    } else {
        base_type
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
        "timestamp" | "timestamptz" => quote! { time::OffsetDateTime },
        "date" => quote! { time::Date },
        "time" => quote! { time::Time },
        "bytea" => quote! { Vec<u8> },
        "inet" => quote! { std::net::IpAddr },
        _ => {
            // For complex types, fall back to String and let serde handle it
            quote! { String }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, TaskQueueConfig};
    use crate::pg_type::PgType;
    use serde_json::json;

    fn create_test_task_index() -> TaskIndex {
        let mut task_index = TaskIndex(HashMap::new());

        // Create a task type with fields (like send_verification_code)
        let task_with_fields = TaskType {
            task_name: "send_verification_code".to_string(),
            type_oid: 123456,
            fields: vec![
                TaskField {
                    name: "email".to_string(),
                    type_oid: 1001,
                    postgres_type: "email".to_string(),
                    position: 1,
                    not_null: true,
                    comment: None,
                },
                TaskField {
                    name: "code".to_string(),
                    type_oid: 1002,
                    postgres_type: "verification_code".to_string(),
                    position: 2,
                    not_null: true,
                    comment: None,
                },
            ],
            comment: None,
        };

        // Create a task type without fields (current problematic case)
        let task_without_fields = TaskType {
            task_name: "shipment_created".to_string(),
            type_oid: 123457,
            fields: vec![], // This is what's currently happening
            comment: None,
        };

        task_index.insert("send_verification_code".to_string(), task_with_fields);
        task_index.insert("shipment_created".to_string(), task_without_fields);

        task_index
    }

    fn create_test_type_index() -> HashMap<OID, PgType> {
        let mut type_map = HashMap::new();

        // Just add basic types - we don't need domain types for our test
        type_map.insert(25, PgType::Text);

        type_map
    }

    fn create_test_config() -> Config {
        Config {
            schemas: vec!["api".to_string()],
            task_queue: Some(TaskQueueConfig {
                schema: "tasks".to_string(),
                table_schema: Some("mq".to_string()),
                table_name: Some("task".to_string()),
                task_name_column: "task_name".to_string(),
                payload_column: "payload".to_string(),
            }),
            ..Default::default()
        }
    }

    // Test the type mapping logic that's used for fallback
    #[test]
    fn test_postgres_type_mapping() {
        // Test the basic type mappings used when type lookup fails
        let email_mapping = map_postgres_type_to_rust("email");
        let text_mapping = map_postgres_type_to_rust("text");
        let integer_mapping = map_postgres_type_to_rust("integer");

        // Domain types should fall back to String
        assert_eq!(email_mapping.to_string(), "String");
        assert_eq!(text_mapping.to_string(), "String");
        assert_eq!(integer_mapping.to_string(), "i32");
    }

    // Test the field generation logic directly
    #[test]
    fn test_field_generation_logic() {
        let fields = vec![
            TaskField {
                name: "email".to_string(),
                type_oid: 1001,
                postgres_type: "email".to_string(),
                position: 1,
                not_null: true,
                comment: None,
            },
            TaskField {
                name: "code".to_string(),
                type_oid: 1002,
                postgres_type: "verification_code".to_string(),
                position: 2,
                not_null: true,
                comment: None,
            },
        ];

        // Test that fields are not empty - this should demonstrate the issue
        assert_eq!(fields.len(), 2, "Should have 2 fields");
        assert_eq!(fields[0].name, "email", "First field should be email");
        assert_eq!(fields[1].name, "code", "Second field should be code");

        // Test the TokenStream generation for field names
        let field_tokens: Vec<TokenStream> = fields
            .iter()
            .map(|field| {
                let field_name =
                    format_ident!("{}", sql_to_rs_string(&field.name, CaseType::Snake));
                let rust_type = map_postgres_type_to_rust(&field.postgres_type);
                quote! { pub #field_name: #rust_type }
            })
            .collect();

        assert_eq!(field_tokens.len(), 2, "Should generate 2 field tokens");

        // Check the generated field code
        let first_field = field_tokens[0].to_string();
        let second_field = field_tokens[1].to_string();

        assert!(
            first_field.contains("pub email"),
            "First field should contain 'pub email'"
        );
        assert!(
            first_field.contains("String"),
            "First field should be String type"
        );
        assert!(
            second_field.contains("pub code"),
            "Second field should contain 'pub code'"
        );
        assert!(
            second_field.contains("String"),
            "Second field should be String type"
        );
    }

    // Test that empty fields generates empty struct
    #[test]
    fn test_empty_fields_generates_empty_struct() {
        let empty_fields: Vec<TaskField> = vec![];

        // Generate tokens for empty fields
        let field_tokens: Vec<TokenStream> = empty_fields
            .iter()
            .map(|field| {
                let field_name =
                    format_ident!("{}", sql_to_rs_string(&field.name, CaseType::Snake));
                let rust_type = map_postgres_type_to_rust(&field.postgres_type);
                quote! { pub #field_name: #rust_type }
            })
            .collect();

        // Should result in no field tokens
        assert_eq!(
            field_tokens.len(),
            0,
            "Empty fields should generate no field tokens"
        );

        // Simulate the payload struct generation with empty fields
        let struct_name = format_ident!("TestTaskPayload");
        let payload_struct = quote! {
            #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
            pub struct #struct_name {
                #(#field_tokens),*
            }
        };

        let generated_struct = payload_struct.to_string();
        println!("Empty payload struct generated: {}", generated_struct);

        // Should generate an empty struct
        assert!(
            generated_struct.contains("TestTaskPayload {"),
            "Should contain struct with empty braces"
        );

        // Now test the enum variant generation
        let variant_name = format_ident!("TestTask");
        let variant = quote! {
            #[serde(rename = "test_task")]
            #variant_name(#struct_name)
        };

        let generated_variant = variant.to_string();
        println!("Tuple variant generated: {}", generated_variant);

        // Should generate a tuple variant
        assert!(
            generated_variant.contains("TestTask")
                && generated_variant.contains("(")
                && generated_variant.contains("TestTaskPayload)"),
            "Should contain tuple variant"
        );
    }

    // Test the new payload struct generation with fallback type mapping
    #[test]
    fn test_payload_struct_generation_with_fallback() {
        let task_type = TaskType {
            task_name: "send_verification_code".to_string(),
            type_oid: 123456,
            fields: vec![
                TaskField {
                    name: "email".to_string(),
                    type_oid: 9999, // Not in type index, will use fallback
                    postgres_type: "email".to_string(),
                    position: 1,
                    not_null: true,
                    comment: None,
                },
                TaskField {
                    name: "code".to_string(),
                    type_oid: 9998, // Not in type index, will use fallback
                    postgres_type: "text".to_string(),
                    position: 2,
                    not_null: true,
                    comment: None,
                },
            ],
            comment: None,
        };

        // Test with minimal type_index - fields will use fallback mapping
        let fields: Vec<TokenStream> = task_type
            .fields
            .iter()
            .map(|field| {
                let field_name =
                    format_ident!("{}", sql_to_rs_string(&field.name, CaseType::Snake));
                let rust_type = map_postgres_type_to_rust(&field.postgres_type);
                quote! { pub #field_name: #rust_type }
            })
            .collect();

        let struct_name = format_ident!(
            "{}Payload",
            sql_to_rs_string(&task_type.task_name, CaseType::Pascal)
        );
        let payload_struct = quote! {
            #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
            pub struct #struct_name {
                #(#fields),*
            }
        };

        let generated = payload_struct.to_string();
        println!("Generated payload struct:\n{}", generated);

        // Should generate a struct named SendVerificationCodePayload
        assert!(
            generated.contains("SendVerificationCodePayload"),
            "Should contain payload struct name"
        );
        assert!(
            generated.contains("pub email : String"),
            "Should contain email field as String (fallback)"
        );
        assert!(
            generated.contains("pub code : String"),
            "Should contain code field as String"
        );
    }

    // Test the new variant generation with tuple syntax
    #[test]
    fn test_generate_task_variant_tuple_syntax() {
        let task_type = TaskType {
            task_name: "send_verification_code".to_string(),
            type_oid: 123456,
            fields: vec![], // Fields don't matter for variant generation anymore
            comment: None,
        };

        // Directly test the variant generation
        let variant_name = format_ident!(
            "{}",
            sql_to_rs_string(&task_type.task_name, CaseType::Pascal)
        );
        let payload_struct_name = format_ident!(
            "{}Payload",
            sql_to_rs_string(&task_type.task_name, CaseType::Pascal)
        );
        let task_name = &task_type.task_name;

        let variant = quote! {
            #[serde(rename = #task_name)]
            #variant_name(#payload_struct_name)
        };

        let generated = variant.to_string();
        println!("Generated tuple variant:\n{}", generated);

        // Should generate a tuple variant
        assert!(
            generated.contains("SendVerificationCode")
                && generated.contains("(")
                && generated.contains("SendVerificationCodePayload)"),
            "Should contain tuple variant with payload struct"
        );
        assert!(
            generated.contains("serde")
                && generated.contains("rename")
                && generated.contains("\"send_verification_code\""),
            "Should have serde rename attribute"
        );
    }

    #[test]
    fn test_task_field_processing_from_json() {
        // Test the JSON processing logic that converts database results to TaskField structs
        let fields_json = json!([
            {
                "name": "email",
                "type_oid": 1001,
                "postgres_type": "email",
                "position": 1,
                "not_null": true,
                "comment": null
            },
            {
                "name": "code",
                "type_oid": 1002,
                "postgres_type": "verification_code",
                "position": 2,
                "not_null": true,
                "comment": null
            }
        ]);

        // This simulates what happens in TaskIndex::new() when processing the JSON
        let fields_vec: Vec<serde_json::Value> = serde_json::from_value(fields_json).unwrap();

        let fields: Vec<TaskField> = fields_vec
            .into_iter()
            .filter_map(|field_json: serde_json::Value| {
                let name = field_json["name"].as_str();
                let type_oid = field_json["type_oid"].as_u64();
                let postgres_type = field_json["postgres_type"].as_str();
                let position = field_json["position"].as_i64();
                let not_null = field_json["not_null"].as_bool();

                match (name, type_oid, postgres_type, position, not_null) {
                    (Some(n), Some(t), Some(pt), Some(p), Some(nn)) => Some(TaskField {
                        name: n.to_string(),
                        type_oid: t as u32,
                        postgres_type: pt.to_string(),
                        position: p as i32,
                        not_null: nn,
                        comment: field_json["comment"].as_str().map(|s| s.to_string()),
                    }),
                    _ => None,
                }
            })
            .collect();

        assert_eq!(fields.len(), 2, "Should parse 2 fields from JSON");
        assert_eq!(fields[0].name, "email", "First field should be email");
        assert_eq!(fields[1].name, "code", "Second field should be code");
        assert_eq!(
            fields[0].type_oid, 1001,
            "First field should have correct type OID"
        );
        assert_eq!(
            fields[1].type_oid, 1002,
            "Second field should have correct type OID"
        );
    }

    #[test]
    fn test_empty_fields_json_processing() {
        // Test what happens when we get empty JSON array (current problem case)
        let empty_fields_json = json!([]);

        let fields_vec: Vec<serde_json::Value> = serde_json::from_value(empty_fields_json).unwrap();
        assert_eq!(
            fields_vec.len(),
            0,
            "Empty JSON array should result in empty vec"
        );

        // Test what happens with null JSON
        let null_fields_json = serde_json::Value::Null;
        assert!(
            null_fields_json.is_null(),
            "Null JSON should be detected as null"
        );
    }

    #[test]
    fn test_bulk_not_null_annotation_for_tasks() {
        let task_type = TaskType {
            task_name: "create_authorization".to_string(),
            type_oid: 123458,
            fields: vec![
                TaskField {
                    name: "payment_method_id".to_string(),
                    type_oid: 25, // text
                    postgres_type: "text".to_string(),
                    position: 1,
                    not_null: false,
                    comment: None,
                },
                TaskField {
                    name: "stripe_customer_id".to_string(),
                    type_oid: 25, // text
                    postgres_type: "text".to_string(),
                    position: 2,
                    not_null: false,
                    comment: None,
                },
                TaskField {
                    name: "optional_field".to_string(),
                    type_oid: 25, // text
                    postgres_type: "text".to_string(),
                    position: 3,
                    not_null: false,
                    comment: None,
                },
            ],
            comment: Some("Task payload for creating payment authorization when setup intent succeeds... @pgrpc_not_null(payment_method_id, stripe_customer_id)".to_string()),
        };

        // Parse bulk not null columns
        let bulk_not_null_columns = crate::pg_type::parse_bulk_not_null_columns(&task_type.comment);
        assert!(bulk_not_null_columns.contains("payment_method_id"));
        assert!(bulk_not_null_columns.contains("stripe_customer_id"));
        assert!(!bulk_not_null_columns.contains("optional_field"));

        // Test field nullability logic
        let type_index = create_test_type_index();
        for field in &task_type.fields {
            let rust_type = generate_task_field_type(field, &type_index, &bulk_not_null_columns);
            let type_str = rust_type.to_string();
            
            if field.name == "payment_method_id" || field.name == "stripe_customer_id" {
                assert!(!type_str.contains("Option"), 
                    "Field {} should not be Option because it's in bulk_not_null_columns", field.name);
            } else {
                assert!(type_str.contains("Option"), 
                    "Field {} should be Option because it's not in bulk_not_null_columns", field.name);
            }
        }
    }
}
