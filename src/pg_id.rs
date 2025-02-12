use pg_query::protobuf::RangeVar;
use ustr::{ustr, Ustr};

/// Namespaced identifier (e.g. `schema.function`)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PgId(Ustr, Ustr);

impl PgId {
    pub fn new(schema_name: Option<impl Into<Ustr>>, name: impl Into<Ustr>) -> Self {
        Self(
            schema_name.map(|s| s.into()).unwrap_or("public".into()),
            name.into(),
        )
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
