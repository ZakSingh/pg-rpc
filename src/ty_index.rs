use crate::codegen::OID;
use crate::pg_type::PgType;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use tokio_postgres::Client;

const TYPES_INTROSPECTION_QUERY: &'static str = include_str!("./queries/type_introspection.sql");

#[derive(Debug)]
pub struct TypeIndex(HashMap<OID, PgType>);

impl Deref for TypeIndex {
    type Target = HashMap<OID, PgType>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TypeIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TypeIndex {
    pub async fn new(db: &Client, type_oids: &[OID]) -> anyhow::Result<Self> {
        let types = db
            .query(TYPES_INTROSPECTION_QUERY, &[&Vec::from_iter(type_oids)])
            .await?
            .into_iter()
            .map(|row| (row.get("oid"), PgType::try_from(row).unwrap()))
            .collect();

        Ok(Self(types))
    }
}
