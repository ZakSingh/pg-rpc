use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub connection_string: String,
    pub types: HashMap<String, String>,
    pub exceptions: HashMap<String, String>,
    pub schemas: Vec<String>
}
