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

    /// Look up a column's type OID within a single relation.
    fn column_type_in_rel(rel: &PgRel, column_name: &str) -> Option<OID> {
        rel.columns
            .iter()
            .position(|c| c.as_str() == column_name)
            .and_then(|idx| rel.column_types.get(idx).copied())
    }

    /// Get the type OID for a column in a relation by table and column name.
    /// This preserves domain types (unlike PostgreSQL's parameter type inference).
    ///
    /// Matching by unqualified name alone is ambiguous when two schemas contain
    /// a table of the same name: `find` would return whichever relation sorts
    /// first by OID, which is catalog-state-dependent. If those same-named
    /// tables disagree on the column's type (e.g. `text` in one, `text[]` in
    /// another), the wrong answer silently corrupts the generated Rust type —
    /// a scalar column can pick up a `Vec<…>` wrapper from an unrelated table.
    /// To stay correct, an ambiguous lookup with conflicting types returns
    /// `None` (falling back to the authoritative prepared-statement type) rather
    /// than guessing. Use [`get_column_type_in_schema`] when the schema is known.
    pub fn get_column_type(&self, table_name: &str, column_name: &str) -> Option<OID> {
        let mut found: Option<OID> = None;
        for rel in self.values().filter(|rel| rel.id.name() == table_name) {
            if let Some(oid) = Self::column_type_in_rel(rel, column_name) {
                match found {
                    None => found = Some(oid),
                    // Ambiguous across schemas with conflicting types — refuse
                    // to guess; the caller falls back to the prepared-statement
                    // type, which postgres reports correctly.
                    Some(prev) if prev != oid => return None,
                    Some(_) => {}
                }
            }
        }
        found
    }

    /// Get a column's type OID, preferring an exact (schema, table) match.
    ///
    /// When `schema` is `Some`, only the relation in that schema is consulted —
    /// this avoids the cross-schema ambiguity described on [`get_column_type`].
    /// When `schema` is `None`, falls back to the disambiguating by-name lookup.
    pub fn get_column_type_in_schema(
        &self,
        schema: Option<&str>,
        table_name: &str,
        column_name: &str,
    ) -> Option<OID> {
        match schema {
            Some(schema) => self
                .values()
                .find(|rel| rel.id.schema() == schema && rel.id.name() == table_name)
                .and_then(|rel| Self::column_type_in_rel(rel, column_name)),
            None => self.get_column_type(table_name, column_name),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_rel::PgRelKind;
    use ustr::ustr;

    /// OIDs for `text` and `text[]` (`_text`) — the pair from the bug report.
    const TEXT_OID: OID = 25;
    const TEXT_ARRAY_OID: OID = 1009;

    fn rel(oid: OID, schema: &str, name: &str, cols: &[(&str, OID)]) -> PgRel {
        PgRel {
            oid,
            id: PgId::new(Some(ustr(schema)), ustr(name)),
            kind: PgRelKind::Table,
            constraints: vec![],
            columns: cols.iter().map(|(c, _)| ustr(c)).collect(),
            column_types: cols.iter().map(|(_, t)| *t).collect(),
        }
    }

    fn index(rels: Vec<PgRel>) -> RelIndex {
        RelIndex(rels.into_iter().map(|r| (r.oid, r)).collect())
    }

    /// Two tables named `widget` in different schemas disagree on `label`'s
    /// type. A by-name lookup must not silently pick one (which one wins is
    /// catalog-OID-dependent); it returns `None` so the caller falls back to the
    /// authoritative prepared-statement type. This is the core of the array-bleed
    /// bug: `core.widget.label` (text) was getting `other.widget.label`'s text[].
    #[test]
    fn ambiguous_cross_schema_lookup_with_conflict_returns_none() {
        // `other.widget` sorts first by OID, exactly as in the failing CI case.
        let idx = index(vec![
            rel(100, "other", "widget", &[("id", 23), ("label", TEXT_ARRAY_OID)]),
            rel(200, "core", "widget", &[("id", 23), ("label", TEXT_OID)]),
        ]);

        assert_eq!(idx.get_column_type("widget", "label"), None);

        // With the schema known, the exact match resolves correctly either way.
        assert_eq!(
            idx.get_column_type_in_schema(Some("core"), "widget", "label"),
            Some(TEXT_OID)
        );
        assert_eq!(
            idx.get_column_type_in_schema(Some("other"), "widget", "label"),
            Some(TEXT_ARRAY_OID)
        );
    }

    /// When same-named tables agree on the column's type, the by-name lookup
    /// still resolves it (no spurious `None`).
    #[test]
    fn ambiguous_cross_schema_lookup_without_conflict_resolves() {
        let idx = index(vec![
            rel(100, "other", "widget", &[("label", TEXT_OID)]),
            rel(200, "core", "widget", &[("label", TEXT_OID)]),
        ]);

        assert_eq!(idx.get_column_type("widget", "label"), Some(TEXT_OID));
    }

    /// The unambiguous single-table case (the overwhelmingly common one) is
    /// unaffected.
    #[test]
    fn unambiguous_lookup_resolves() {
        let idx = index(vec![rel(
            100,
            "public",
            "t",
            &[("id", 23), ("name", TEXT_OID), ("tags", TEXT_ARRAY_OID)],
        )]);

        assert_eq!(idx.get_column_type("t", "name"), Some(TEXT_OID));
        assert_eq!(idx.get_column_type("t", "tags"), Some(TEXT_ARRAY_OID));
        // `schema = None` falls back to the by-name path.
        assert_eq!(
            idx.get_column_type_in_schema(None, "t", "name"),
            Some(TEXT_OID)
        );
        // A wrong schema yields no match (no cross-schema bleed).
        assert_eq!(
            idx.get_column_type_in_schema(Some("nonexistent"), "t", "name"),
            None
        );
    }
}
