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

impl TryFrom<Row> for PgRel {
    type Error = postgres::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let constraints = row.try_get::<_, Vec<Constraint>>("constraints")?;

        Ok(Self {
            oid: row.try_get::<_, u32>("oid")?,
            kind: match row.try_get::<_, &str>("kind")?.into() {
                "r" => PgRelKind::Table,
                "v" | "m" => PgRelKind::View {
                    definition: row.try_get("view_definition")?,
                },
                value => unimplemented!("unknown relation kind {}", value),
            },
            id: PgId::new(
                row.try_get::<_, Option<&str>>("schema")?.map(|s| s.into()),
                row.try_get::<_, &str>("name")?.into(),
            ),
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
