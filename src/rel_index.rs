use crate::codegen::OID;
use crate::parse_check_enum::parse_check_as_enum;
use crate::pg_constraint::Constraint;
use crate::pg_rel::PgRel;
use anyhow::Context;
use postgres::Client;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::OnceLock;

/// Information about an enum type inferred from a CHECK constraint
#[derive(Debug, Clone)]
pub struct CheckEnumTypeInfo {
    pub schema: String,
    pub table_name: String,
    pub column_name: String,
    pub constraint_name: String,
    pub variants: Vec<String>,
}

const RELATION_INTROSPECTION_QUERY: &str = include_str!("./queries/relation_introspection.sql");

/// Schemas the relation introspection query leaves out. A name found in one
/// of them while walking the search path is a relation we have no entry for.
const UNINDEXED_SCHEMAS: &[&str] = &["pg_catalog", "information_schema"];

#[derive(Debug)]
pub struct RelIndex {
    relations: BTreeMap<OID, PgRel>,
    /// Parent relation -> its partitions / inheritance children.
    children: BTreeMap<OID, Vec<OID>>,
    /// The connection's effective search path, in resolution order
    /// (`current_schemas(true)`, so implicit schemas such as `pg_catalog`
    /// are included). `["public"]` for an index built by hand.
    search_path: Vec<String>,
    /// `(schema, name)` of relations in search-path schemas that the index
    /// does not hold (system catalogs). They shadow same-named user relations
    /// further down the path, so an unqualified name that hits one resolves
    /// to nothing rather than to the wrong relation.
    unindexed: HashSet<(String, String)>,
    /// Relation name -> `(schema, OID)` of every relation with that name.
    /// Built on first use and dropped on any mutable access to `relations`,
    /// so relations inserted through `DerefMut` are always found.
    by_name: OnceLock<HashMap<String, Vec<(String, OID)>>>,
}

impl Default for RelIndex {
    fn default() -> Self {
        Self {
            relations: BTreeMap::new(),
            children: BTreeMap::new(),
            search_path: vec!["public".to_string()],
            unindexed: HashSet::new(),
            by_name: OnceLock::new(),
        }
    }
}

impl Deref for RelIndex {
    type Target = BTreeMap<OID, PgRel>;

    fn deref(&self) -> &Self::Target {
        &self.relations
    }
}

impl DerefMut for RelIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Whatever the caller changes, the name index is rebuilt afterwards.
        self.by_name = OnceLock::new();
        &mut self.relations
    }
}

impl RelIndex {
    /// Construct the relation index.
    pub fn new(db: &mut Client) -> anyhow::Result<Self> {
        let mut relations = BTreeMap::new();
        let mut children = BTreeMap::new();
        for row in db
            .query(RELATION_INTROSPECTION_QUERY, &[])
            .context("Relation introspection query failed")?
        {
            let oid = row.try_get::<_, u32>("oid")?;
            if let Some(child_oids) = row.try_get::<_, Option<Vec<u32>>>("child_oids")? {
                children.insert(oid, child_oids);
            }
            let rel = PgRel::try_from(row).context("Failed to index relation")?;
            relations.insert(oid, rel);
        }

        // Effective search path, in resolution order — what Postgres uses to
        // resolve an unqualified relation name on this connection.
        let search_path: Vec<String> = db
            .query_one("SELECT current_schemas(true) AS path", &[])
            .context("Failed to read search_path")?
            .get("path");

        let unindexed_on_path: Vec<&str> = UNINDEXED_SCHEMAS
            .iter()
            .copied()
            .filter(|schema| search_path.iter().any(|s| s == schema))
            .collect();
        let mut unindexed = HashSet::new();
        if !unindexed_on_path.is_empty() {
            for row in db
                .query(
                    "SELECT n.nspname AS schema, c.relname AS name \
                     FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = ANY($1::text[]) \
                       AND c.relkind IN ('r', 'v', 'm', 'f', 'p')",
                    &[&unindexed_on_path],
                )
                .context("System relation name query failed")?
            {
                unindexed.insert((row.try_get("schema")?, row.try_get("name")?));
            }
        }

        Ok(Self {
            relations,
            children,
            search_path,
            unindexed,
            by_name: OnceLock::new(),
        })
    }

    /// Replace the search path. For indexes built by hand (tests); a real
    /// index reads the connection's.
    pub fn with_search_path(mut self, search_path: &[&str]) -> Self {
        self.search_path = search_path.iter().map(|s| s.to_string()).collect();
        self
    }

    /// The search path unqualified names are resolved along.
    pub fn search_path(&self) -> &[String] {
        &self.search_path
    }

    fn by_name(&self) -> &HashMap<String, Vec<(String, OID)>> {
        self.by_name.get_or_init(|| {
            let mut by_name: HashMap<String, Vec<(String, OID)>> = HashMap::new();
            for (oid, rel) in &self.relations {
                by_name
                    .entry(rel.id.name().to_string())
                    .or_default()
                    .push((rel.id.schema().to_string(), *oid));
            }
            by_name
        })
    }

    /// The partitions (or classic inheritance children) of a relation. Rows
    /// written through the parent land in these, and it is the child's
    /// constraint and table names that a violation reports.
    pub fn children_of(&self, oid: OID) -> &[OID] {
        self.children.get(&oid).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Record `child` as a partition / inheritance child of `parent`.
    pub fn add_child(&mut self, parent: OID, child: OID) {
        self.children.entry(parent).or_default().push(child);
    }

    /// Get type OIDs for all views and materialized views
    /// Note: Returns type OIDs, not relation OIDs
    pub fn get_view_type_oids(&self, client: &mut postgres::Client) -> anyhow::Result<Vec<OID>> {
        let view_names: Vec<(String, String)> = self
            .relations
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

    /// Find the relation a name written in a query refers to, the way
    /// Postgres does on this connection.
    ///
    /// With a schema, the match is exact. Without one, the name is looked up
    /// in each schema of the search path in order and the first hit wins. A
    /// name that is in no schema of the path does not resolve — Postgres
    /// would reject the query — even when exactly one relation elsewhere has
    /// it. Nor does a name that a system catalog earlier on the path shadows:
    /// the index has no entry for that relation, and handing out the user
    /// table behind it would attribute one relation's columns to another.
    /// `None` in both cases, so callers fall back to not knowing.
    pub fn resolve(&self, schema: Option<&str>, name: &str) -> Option<&PgRel> {
        let candidates = self.by_name().get(name)?;
        let in_schema = |schema: &str| {
            candidates
                .iter()
                .find(|(s, _)| s == schema)
                .and_then(|(_, oid)| self.relations.get(oid))
        };
        match schema {
            Some(schema) => in_schema(schema),
            None => {
                for schema in &self.search_path {
                    if let Some(rel) = in_schema(schema) {
                        return Some(rel);
                    }
                    if UNINDEXED_SCHEMAS.contains(&schema.as_str())
                        && self.unindexed.contains(&(schema.clone(), name.to_string()))
                    {
                        return None;
                    }
                }
                None
            }
        }
    }

    /// [`Self::resolve`] for a parsed relation reference.
    pub fn resolve_range_var(&self, range_var: &pg_query::protobuf::RangeVar) -> Option<&PgRel> {
        let schema = (!range_var.schemaname.is_empty()).then_some(range_var.schemaname.as_str());
        self.resolve(schema, &range_var.relname)
    }

    /// Look up a column's type OID within a single relation.
    fn column_type_in_rel(rel: &PgRel, column_name: &str) -> Option<OID> {
        rel.columns
            .iter()
            .position(|c| c.as_str() == column_name)
            .and_then(|idx| rel.column_types.get(idx).copied())
    }

    /// Declared type of column `column` of relation `rel_oid` (domains are
    /// preserved, unlike in prepared-statement metadata).
    pub fn column_type(&self, rel_oid: OID, column: &str) -> Option<OID> {
        Self::column_type_in_rel(self.relations.get(&rel_oid)?, column)
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
    use crate::pg_id::PgId;
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
        let mut index = RelIndex::default();
        for r in rels {
            index.insert(r.oid, r);
        }
        index
    }

    /// Two tables named `widget` in different schemas disagree on `label`'s
    /// type. Nothing may attribute one's column to the other (which one a
    /// by-name scan finds is catalog-OID-dependent — the array-bleed bug:
    /// `core.widget.label` (text) was getting `other.widget.label`'s text[]).
    /// A relation is resolved first, by schema or search path, and the column
    /// type is then read from exactly that relation.
    #[test]
    fn column_type_is_read_from_the_resolved_relation_only() {
        // `other.widget` sorts first by OID, exactly as in the failing CI case.
        let idx = index(vec![
            rel(100, "other", "widget", &[("id", 23), ("label", TEXT_ARRAY_OID)]),
            rel(200, "core", "widget", &[("id", 23), ("label", TEXT_OID)]),
        ]);
        let label_type = |idx: &RelIndex, schema: Option<&str>| {
            idx.resolve(schema, "widget")
                .and_then(|rel| idx.column_type(rel.oid, "label"))
        };

        assert_eq!(label_type(&idx, Some("core")), Some(TEXT_OID));
        assert_eq!(label_type(&idx, Some("other")), Some(TEXT_ARRAY_OID));
        // Neither schema is on the path: no relation, no type.
        assert_eq!(label_type(&idx, None), None);
        assert_eq!(label_type(&idx, Some("nonexistent")), None);

        let idx = idx.with_search_path(&["core", "other"]);
        assert_eq!(label_type(&idx, None), Some(TEXT_OID));
        let idx = idx.with_search_path(&["other", "core"]);
        assert_eq!(label_type(&idx, None), Some(TEXT_ARRAY_OID));

        assert_eq!(idx.column_type(200, "missing"), None);
        assert_eq!(idx.column_type(999, "label"), None);
    }

    /// `resolve` matches a schema-qualified name exactly, and an unqualified
    /// one through the search path.
    #[test]
    fn resolve_is_exact_with_schema_and_follows_the_search_path_without() {
        let idx = index(vec![
            rel(100, "other", "widget", &[("id", 23)]),
            rel(200, "core", "widget", &[("id", 23)]),
            rel(300, "core", "gadget", &[("id", 23)]),
            rel(400, "hidden", "gizmo", &[("id", 23)]),
        ])
        .with_search_path(&["core", "other"]);

        assert_eq!(idx.resolve(Some("core"), "widget").map(|r| r.oid), Some(200));
        assert_eq!(idx.resolve(Some("other"), "widget").map(|r| r.oid), Some(100));
        // First schema on the path that has the name.
        assert_eq!(idx.resolve(None, "widget").map(|r| r.oid), Some(200));
        assert_eq!(idx.resolve(None, "gadget").map(|r| r.oid), Some(300));
        assert!(idx.resolve(Some("other"), "gadget").is_none());
        assert!(idx.resolve(None, "missing").is_none());
        // Unique across the whole index, but not on the path: Postgres would
        // reject the query, so it resolves to nothing. Qualified, it is found.
        assert!(idx.resolve(None, "gizmo").is_none());
        assert_eq!(idx.resolve(Some("hidden"), "gizmo").map(|r| r.oid), Some(400));

        // The same relations under the reverse path: the other `widget` wins.
        let idx = idx.with_search_path(&["other", "core"]);
        assert_eq!(idx.resolve(None, "widget").map(|r| r.oid), Some(100));
        assert_eq!(idx.resolve(None, "gadget").map(|r| r.oid), Some(300));
    }

    /// A hand-built index resolves unqualified names in `public` only.
    #[test]
    fn default_search_path_is_public() {
        let idx = index(vec![
            rel(100, "public", "t", &[("id", 23)]),
            rel(200, "app", "t", &[("id", 23)]),
            rel(300, "app", "u", &[("id", 23)]),
        ]);
        assert_eq!(idx.search_path(), ["public".to_string()]);
        assert_eq!(idx.resolve(None, "t").map(|r| r.oid), Some(100));
        assert!(idx.resolve(None, "u").is_none());
    }

    /// Relations added through `DerefMut` after a lookup are still found, and
    /// removed ones no longer are.
    #[test]
    fn name_index_follows_later_inserts_and_removals() {
        let mut idx = index(vec![rel(100, "public", "t", &[("id", 23)])]);
        assert!(idx.resolve(None, "u").is_none());
        idx.insert(200, rel(200, "public", "u", &[("id", 23)]));
        assert_eq!(idx.resolve(None, "u").map(|r| r.oid), Some(200));
        assert_eq!(idx.resolve(Some("public"), "u").map(|r| r.oid), Some(200));
        idx.remove(&100);
        assert!(idx.resolve(None, "t").is_none());
    }

    /// A system catalog earlier on the path shadows a user table of the same
    /// name; the index has no entry for the catalog, so nothing resolves.
    #[test]
    fn system_relation_on_the_path_shadows_a_user_table() {
        let mut idx = index(vec![rel(100, "public", "pg_class", &[("id", 23)])])
            .with_search_path(&["pg_catalog", "public"]);
        idx.unindexed
            .insert(("pg_catalog".to_string(), "pg_class".to_string()));
        assert!(idx.resolve(None, "pg_class").is_none());
        assert_eq!(idx.resolve(Some("public"), "pg_class").map(|r| r.oid), Some(100));
        // With `public` first, the user table wins as it would in Postgres.
        let idx = idx.with_search_path(&["public", "pg_catalog"]);
        assert_eq!(idx.resolve(None, "pg_class").map(|r| r.oid), Some(100));
    }
}
