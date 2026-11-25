use serde::Deserialize;
use ustr::{ustr, Ustr};

/// Namespaced identifier (e.g. `schema.function`)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
pub struct PgId(Ustr, Ustr);

impl PgId {
    pub fn new(schema_name: Option<Ustr>, name: Ustr) -> Self {
        Self(schema_name.unwrap_or("public".into()), name)
    }

    pub fn schema(&self) -> &str {
        &self.0
    }

    pub fn name(&self) -> &str {
        &self.1
    }
}

impl From<String> for PgId {
    fn from(s: String) -> Self {
        let (schema, name) = s
            .split_once(".")
            .map(|(s, n)| (Some(ustr(s)), ustr(n)))
            .unwrap_or((None, ustr(s.as_ref())));

        Self::new(schema, name)
    }
}
