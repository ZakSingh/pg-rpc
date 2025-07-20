use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
#[serde(default)]
pub struct Config {
    pub connection_string: Option<String>,
    pub output_path: Option<String>,
    pub types: HashMap<String, String>,
    pub exceptions: HashMap<String, String>,
    pub schemas: Vec<String>
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connection_string: None,
            output_path: None,
            types: HashMap::new(),
            exceptions: HashMap::new(),
            schemas: Vec::new(),
        }
    }
}
