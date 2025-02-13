use crate::codegen::OID;
use crate::pg_id::PgId;
use crate::pg_rel::PgRel;
use anyhow::Context;
use itertools::Itertools;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use tokio_postgres::Client;

const RELATION_INTROSPECTION_QUERY: &'static str =
    include_str!("./queries/relation_introspection.sql");

#[derive(Debug, Default)]
pub struct RelIndex(HashMap<OID, PgRel>);

impl Deref for RelIndex {
    type Target = HashMap<OID, PgRel>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RelIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl RelIndex {
    /// Construct the relation index.
    pub async fn new(db: &Client) -> anyhow::Result<Self> {
        let relations = db
            .query(RELATION_INTROSPECTION_QUERY, &[])
            .await
            .context("Relation introspection query failed")?
            .into_iter()
            .map(|row| {
                Ok::<_, tokio_postgres::Error>((
                    row.try_get::<_, u32>("oid")?,
                    PgRel::try_from(row)?,
                ))
            })
            .try_collect()?;

        Ok(Self(relations))
    }

    pub fn get_by_id(&self, id: &PgId) -> Option<&PgRel> {
        self.0.values().find(|rel| rel.id == *id)
    }

    pub fn id_to_oid(&self, id: &PgId) -> Option<OID> {
        self.get_by_id(id).map(|rel| rel.oid)
    }
}
