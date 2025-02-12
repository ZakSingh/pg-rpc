use crate::codegen::OID;
use crate::pg_constraint::{
    CheckConstraint, Constraint, ForeignKeyConstraint, NotNullConstraint, OnDelete,
    PrimaryKeyConstraint, UniqueConstraint,
};
use crate::pg_id::PgId;
use itertools::{izip, Itertools};
use tokio_postgres::Row;
use ustr::Ustr;

#[derive(Debug, Copy, Clone)]
pub struct Column {
    pub name: Ustr,
    pub has_default: bool,
    pub not_null: bool,
}

impl Column {
    pub fn new(name: impl Into<Ustr>, has_default: bool, not_null: bool) -> Self {
        Self {
            name: name.into(),
            has_default,
            not_null,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PgRel {
    pub oid: OID,
    pub id: PgId,
    pub constraints: Vec<Constraint>,
    pub columns: Vec<Column>,
}

impl PgRel {
    pub fn column_names(&self) -> Vec<Ustr> {
        self.columns.iter().map(|c| c.name).collect()
    }
}

impl TryFrom<Row> for PgRel {
    type Error = tokio_postgres::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        // Get the constraints on the relation
        let constraint_names = row.try_get::<_, Vec<&str>>("constraint_names")?;
        let constraint_types = row.try_get::<_, Vec<&str>>("constraint_types")?;
        let constraints_columns = row.try_get::<_, Vec<Vec<&str>>>("constraints_columns")?;

        let constraints: Vec<Constraint> =
            izip!(constraint_types, constraint_names, constraints_columns)
                .map(|(kind, name, cols)| match kind {
                    "c" => Constraint::Check(CheckConstraint {
                        name: name.into(),
                        columns: cols.iter().map_into().collect(),
                    }),
                    "f" => Constraint::ForeignKey(ForeignKeyConstraint {
                        name: name.into(),
                        columns: cols.iter().map_into().collect(),
                        on_delete: OnDelete::Nothing,
                    }),
                    "p" => Constraint::PrimaryKey(PrimaryKeyConstraint {
                        name: name.into(),
                        columns: cols.iter().map_into().collect(),
                    }),
                    "u" => Constraint::Unique(UniqueConstraint {
                        name: name.into(),
                        columns: Default::default(),
                    }),
                    "n" => Constraint::NotNull(NotNullConstraint {
                        name: name.into(),
                        column: Default::default(),
                    }),
                    _ => unimplemented!("Unsupported constraint kind: {}", kind),
                })
                .collect();

        let col_names = row.try_get::<_, Vec<&str>>("col_names")?;
        let col_defaults = row.try_get::<_, Vec<bool>>("col_has_defaults")?;
        let col_not_nulls = row.try_get::<_, Vec<bool>>("col_not_nulls")?;

        let columns: Vec<Column> = izip!(col_names, col_defaults, col_not_nulls)
            .map(|(name, default, not_null)| Column::new(name, default, not_null))
            .collect();

        Ok(Self {
            oid: row.try_get::<_, u32>("oid")?,
            id: PgId::new(
                row.try_get::<_, Option<&str>>("schema")?,
                row.try_get::<_, &str>("name")?,
            ),
            constraints,
            columns,
        })
    }
}
