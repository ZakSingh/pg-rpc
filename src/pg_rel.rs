use crate::codegen::OID;
use crate::pg_constraint::Constraint;
use crate::pg_id::PgId;
use tokio_postgres::Row;
use ustr::Ustr;

#[derive(Debug, Clone)]
pub struct PgRel {
    pub oid: OID,
    pub id: PgId,
    pub constraints: Vec<Constraint>,
    pub columns: Vec<Ustr>,
}

impl TryFrom<Row> for PgRel {
    type Error = tokio_postgres::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let constraints = row.try_get::<_, Vec<Constraint>>("constraints")?;

        Ok(Self {
            oid: row.try_get::<_, u32>("oid")?,
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
        })
    }
}
