use crate::codegen::OID;
use crate::pg_constraint::Constraint;
use crate::pg_id::PgId;
use postgres::Row;
use ustr::Ustr;

#[derive(Debug, Clone)]
pub enum PgRelKind {
    Table,
    View { definition: String },
}

#[derive(Debug, Clone)]
pub struct PgRel {
    pub oid: OID,
    pub id: PgId,
    pub kind: PgRelKind,
    pub constraints: Vec<Constraint>,
    pub columns: Vec<Ustr>,
    pub column_types: Vec<OID>,
}

impl PgRelKind {
    /// Map a `pg_class.relkind` to the kind of relation it is for analysis
    /// purposes. Partitioned (`p`) and foreign (`f`) tables are written and
    /// constrained like ordinary tables. `None` for kinds the introspection
    /// query should never return.
    fn from_relkind(relkind: &str, view_definition: Option<String>) -> Option<Self> {
        match relkind {
            "r" | "p" | "f" => Some(PgRelKind::Table),
            "v" | "m" => Some(PgRelKind::View {
                definition: view_definition.unwrap_or_default(),
            }),
            _ => None,
        }
    }
}

impl TryFrom<Row> for PgRel {
    type Error = anyhow::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        // The introspection query aggregates constraints without an ORDER BY,
        // so their order is whatever the executor produced that session. Sort
        // so generated match arms don't reorder between runs.
        let mut constraints = row.try_get::<_, Vec<Constraint>>("constraints")?;
        constraints.sort_by(|a, b| a.name().as_str().cmp(b.name().as_str()));

        let schema = row.try_get::<_, Option<&str>>("schema")?;
        let name = row.try_get::<_, &str>("name")?;
        let relkind = row.try_get::<_, &str>("kind")?;
        let kind = PgRelKind::from_relkind(relkind, row.try_get("view_definition")?)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "relation {}.{} has unsupported relkind '{}'",
                    schema.unwrap_or("public"),
                    name,
                    relkind
                )
            })?;

        Ok(Self {
            oid: row.try_get::<_, u32>("oid")?,
            kind,
            id: PgId::new(schema.map(|s| s.into()), name.into()),
            constraints,
            columns: row
                .try_get::<_, Vec<&str>>("column_names")?
                .into_iter()
                .map(|s| s.into())
                .collect(),
            column_types: row
                .try_get::<_, Option<Vec<u32>>>("column_types")?
                .unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitioned_and_foreign_tables_are_tables() {
        for relkind in ["r", "p", "f"] {
            assert!(
                matches!(PgRelKind::from_relkind(relkind, None), Some(PgRelKind::Table)),
                "{relkind}"
            );
        }
    }

    #[test]
    fn views_keep_their_definition() {
        for relkind in ["v", "m"] {
            match PgRelKind::from_relkind(relkind, Some("select 1".into())) {
                Some(PgRelKind::View { definition }) => assert_eq!(definition, "select 1"),
                other => panic!("{relkind}: {other:?}"),
            }
        }
    }

    #[test]
    fn unknown_relkind_is_reported_not_panicked() {
        assert!(PgRelKind::from_relkind("S", None).is_none());
    }
}
