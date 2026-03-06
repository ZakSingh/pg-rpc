use crate::codegen::OID;
use crate::parse_check_enum::parse_check_as_enum;
use crate::pg_constraint::Constraint;
use crate::pg_id::PgId;
use crate::pg_rel::PgRel;
use anyhow::Context;
use postgres::Client;
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

/// Information about an enum type inferred from a CHECK constraint
#[derive(Debug, Clone)]
pub struct CheckEnumTypeInfo {
    pub schema: String,
    pub table_name: String,
    pub column_name: String,
    pub constraint_name: String,
    pub variants: Vec<String>,
}

const RELATION_INTROSPECTION_QUERY: &'static str =
    include_str!("./queries/relation_introspection.sql");

#[derive(Debug, Default)]
pub struct RelIndex(BTreeMap<OID, PgRel>);

impl Deref for RelIndex {
    type Target = BTreeMap<OID, PgRel>;

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
        let relations: BTreeMap<OID, PgRel> = db
            .query(RELATION_INTROSPECTION_QUERY, &[])
            .context("Relation introspection query failed")?
            .into_iter()
            .map(|row| {
                Ok::<_, postgres::Error>((row.try_get::<_, u32>("oid")?, PgRel::try_from(row)?))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;

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

    /// Get the type OID for a column in a relation by table and column name.
    /// This preserves domain types (unlike PostgreSQL's parameter type inference).
    pub fn get_column_type(&self, table_name: &str, column_name: &str) -> Option<OID> {
        self.values()
            .find(|rel| rel.id.name() == table_name)
            .and_then(|rel| {
                rel.columns
                    .iter()
                    .position(|c| c.as_str() == column_name)
                    .and_then(|idx| rel.column_types.get(idx).copied())
            })
    }

    /// Extract CHECK constraint enum information from all relations.
    /// Returns enum info for single-column CHECK constraints that match
    /// patterns like `col IN ('a', 'b', 'c')` or `col = 'a' OR col = 'b'`.
    pub fn get_check_enum_infos(&self) -> Vec<CheckEnumTypeInfo> {
        let mut infos = Vec::new();

        for rel in self.values() {
            let schema = rel.id.schema().to_string();
            let table_name = rel.id.name().to_string();

            for constraint in &rel.constraints {
                if let Constraint::Check(check) = constraint {
                    // Only process single-column CHECK constraints
                    if check.columns.len() != 1 {
                        continue;
                    }

                    if let Some(ref check_expr) = check.check_expression {
                        let column_refs: Vec<&str> =
                            check.columns.iter().map(|c| c.as_str()).collect();

                        if let Some(enum_info) = parse_check_as_enum(check_expr, &column_refs) {
                            infos.push(CheckEnumTypeInfo {
                                schema: schema.clone(),
                                table_name: table_name.clone(),
                                column_name: enum_info.column,
                                constraint_name: check.name.to_string(),
                                variants: enum_info.variants,
                            });
                        }
                    }
                }
            }
        }

        infos
    }
}
