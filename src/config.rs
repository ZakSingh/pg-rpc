use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub types: HashMap<String, String>,
    pub exceptions: HashMap<String, String>,
}
