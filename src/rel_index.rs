use crate::codegen::OID;
use crate::pg_id::PgId;
use crate::pg_rel::PgRel;
use anyhow::Context;
use itertools::Itertools;
use postgres::Client;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

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
    pub fn new(db: &mut Client) -> anyhow::Result<Self> {
        let relations = db
            .query(RELATION_INTROSPECTION_QUERY, &[])
            .context("Relation introspection query failed")?
            .into_iter()
            .map(|row| {
                Ok::<_, postgres::Error>((row.try_get::<_, u32>("oid")?, PgRel::try_from(row)?))
            })
            .try_collect()?;

        Ok(Self(relations))
    }

    pub fn get_by_id(&self, id: &PgId) -> Option<&PgRel> {
        self.0.values().find(|rel| rel.id == *id)
    }

    /// Get type OIDs for all views and materialized views
    /// Note: Returns type OIDs, not relation OIDs
    pub fn get_view_type_oids(&self, client: &mut postgres::Client) -> anyhow::Result<Vec<OID>> {
        let view_names: Vec<(String, String)> = self
            .0
            .iter()
            .filter_map(|(_, rel)| match &rel.kind {
                crate::pg_rel::PgRelKind::View { .. } => {
                    Some((rel.id.schema().to_string(), rel.id.name().to_string()))
                }
                _ => None,
            })
            .collect();

        log::info!("Found {} views in RelIndex", view_names.len());
        for (schema, name) in &view_names {
            log::info!("  View: {}.{}", schema, name);
        }

        if view_names.is_empty() {
            return Ok(Vec::new());
        }

        // Query to get type OIDs for views
        let query = r#"
            SELECT t.oid 
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE (n.nspname, t.typname) IN (
                SELECT unnest($1::text[]), unnest($2::text[])
            )
            AND t.typtype = 'c'
        "#;

        let schemas: Vec<String> = view_names.iter().map(|(s, _)| s.clone()).collect();
        let names: Vec<String> = view_names.iter().map(|(_, n)| n.clone()).collect();

        let rows = client.query(query, &[&schemas, &names])?;
        let oids: Vec<OID> = rows.iter().map(|row| row.get(0)).collect();

        log::info!("Found {} view type OIDs", oids.len());

        Ok(oids)
    }

    pub fn id_to_oid(&self, id: &PgId) -> Option<OID> {
        self.get_by_id(id).map(|rel| rel.oid)
    }
}
