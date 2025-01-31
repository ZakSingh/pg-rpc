use crate::codegen::OID;
use crate::ident::sql_to_rs_ident;
use crate::ident::CaseType::Pascal;
use crate::pg_fn::Cmd;
use crate::pg_id::PgId;
use anyhow::Context;
use heck::ToPascalCase;
use itertools::{izip, Itertools};
use quote::__private::TokenStream;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Row};

const RELATION_INTROSPECTION_QUERY: &'static str =
    include_str!("./queries/relation_introspection.sql");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConstraintKind {
    Check,
    ForeignKey,
    PrimaryKey,
    Unique,
    NotNull,
}

impl From<&str> for ConstraintKind {
    fn from(s: &str) -> Self {
        match s {
            "c" => ConstraintKind::Check,
            "f" => ConstraintKind::ForeignKey,
            "p" => ConstraintKind::PrimaryKey,
            "u" => ConstraintKind::Unique,
            "n" => ConstraintKind::NotNull,
            _ => unimplemented!("Unknown constraint kind: {}", s),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Constraint {
    name: String,
    kind: ConstraintKind,
}

impl From<ConstraintKind> for SqlState {
    fn from(c: ConstraintKind) -> Self {
        match c {
            ConstraintKind::Check => SqlState::CHECK_VIOLATION,
            ConstraintKind::ForeignKey => SqlState::FOREIGN_KEY_VIOLATION,
            ConstraintKind::PrimaryKey => SqlState::UNIQUE_VIOLATION,
            ConstraintKind::Unique => SqlState::UNIQUE_VIOLATION,
            ConstraintKind::NotNull => SqlState::NOT_NULL_VIOLATION,
        }
    }
}

impl Constraint {
    pub fn rs_name(&self) -> TokenStream {
        sql_to_rs_ident(&self.name, Pascal)
    }
}

#[derive(Debug, Clone)]
pub struct PgRel {
    pub(crate) oid: OID,
    pub(crate) id: PgId,
    pub(crate) constraints: Vec<Constraint>,
}

impl TryFrom<Row> for PgRel {
    type Error = tokio_postgres::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let constraint_names = row.try_get::<_, Vec<&str>>("constraint_names")?;
        let constraint_types = row.try_get::<_, Vec<&str>>("constraint_types")?;

        let constraints: Vec<Constraint> = izip!(constraint_names, constraint_types)
            .map(|(name, kind)| Constraint {
                name: name.to_string(),
                kind: ConstraintKind::from(kind),
            })
            .collect();

        Ok(Self {
            oid: row.try_get("oid")?,
            id: PgId::new(row.try_get("schema")?, row.try_get("name")?),
            constraints,
        })
    }
}

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

    pub fn get_by_oid(&self, oid: &OID) -> Option<&PgRel> {
        self.0.get(oid)
    }

    pub fn id_to_oid(&self, id: &PgId) -> Option<OID> {
        self.get_by_id(id).map(|rel| rel.oid)
    }
}
