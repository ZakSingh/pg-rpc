use pg_query::protobuf::RangeVar;
use serde::Deserialize;

/// Namespaced identifier (e.g. `schema.function`)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct PgId(String, String);

impl PgId {
    pub fn new(schema_name: Option<String>, name: String) -> Self {
        Self(schema_name.unwrap_or("public".to_string()), name)
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
            .map(|(s, n)| (Some(s.to_string()), n.to_string()))
            .unwrap_or((None, s));

        Self::new(schema, name)
    }
}

// for pg_query
impl From<RangeVar> for PgId {
    fn from(r: RangeVar) -> Self {
        Self::new(
            (!r.schemaname.is_empty()).then_some(r.schemaname),
            r.relname,
        )
    }
}

impl From<&RangeVar> for PgId {
    fn from(r: &RangeVar) -> Self {
        Self::new(
            (!r.schemaname.is_empty()).then_some(r.schemaname.to_owned()),
            r.relname.to_owned(),
        )
    }
}
