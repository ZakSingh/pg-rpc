use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
#[serde(default)]
pub struct Config {
    pub connection_string: Option<String>,
    pub output_path: Option<String>,
    pub types: HashMap<String, String>,
    pub exceptions: HashMap<String, String>,
    pub schemas: Vec<String>,
    pub task_queue: Option<TaskQueueConfig>,
    pub errors: Option<ErrorsConfig>,
    pub infer_view_nullability: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TaskQueueConfig {
    pub schema: String,
    pub task_name_column: String,
    pub payload_column: String,
    pub table_schema: Option<String>,
    pub table_name: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ErrorsConfig {
    pub schema: String,
    pub raise_function: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connection_string: None,
            output_path: None,
            types: HashMap::new(),
            exceptions: HashMap::new(),
            schemas: Vec::new(),
            task_queue: None,
            errors: None,
            infer_view_nullability: true,
        }
    }
}

impl TaskQueueConfig {
    /// Get the table schema, using default if not specified
    pub fn get_table_schema(&self) -> &str {
        self.table_schema.as_deref().unwrap_or("mq")
    }

    /// Get the table name, using default if not specified
    pub fn get_table_name(&self) -> &str {
        self.table_name.as_deref().unwrap_or("task")
    }

    /// Get the full table name (schema.table)
    pub fn get_full_table_name(&self) -> String {
        format!("{}.{}", self.get_table_schema(), self.get_table_name())
    }
}

impl Default for TaskQueueConfig {
    fn default() -> Self {
        Self {
            schema: "tasks".to_string(),
            task_name_column: "task_name".to_string(),
            payload_column: "payload".to_string(),
            table_schema: Some("mq".to_string()),
            table_name: Some("task".to_string()),
        }
    }
}

impl ErrorsConfig {
    /// Get the raise error function name, defaulting to core.raise_error
    pub fn get_raise_function(&self) -> &str {
        self.raise_function.as_deref().unwrap_or("core.raise_error")
    }
}

impl Default for ErrorsConfig {
    fn default() -> Self {
        Self {
            schema: "errors".to_string(),
            raise_function: Some("core.raise_error".to_string()),
        }
    }
}
