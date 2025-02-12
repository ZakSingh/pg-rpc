use crate::ident::sql_to_rs_ident;
use crate::ident::CaseType::Pascal;
use enum_dispatch::enum_dispatch;
use postgres_types::{FromSql, Type};
use quote::ToTokens;
use quote::__private::TokenStream;
use serde::Deserialize;
use smallvec::SmallVec;
use std::error::Error;
use tokio_postgres::error::SqlState;
use ustr::Ustr;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub enum OnDelete {
    #[serde(rename = "r")]
    Restrict,
    #[serde(rename = "c")]
    Cascade,
    #[serde(rename = "a")]
    NoAction,
    #[serde(rename = "n")]
    SetNull,
    #[serde(rename = "d")]
    SetDefault,
}

#[enum_dispatch]
trait RsName {
    fn rs_name(&self) -> TokenStream;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct CheckConstraint {
    pub(crate) name: Ustr,
    pub(crate) columns: SmallVec<Ustr, 2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct ForeignKeyConstraint {
    pub(crate) name: Ustr,
    pub(crate) columns: SmallVec<Ustr, 2>,
    pub(crate) on_delete: OnDelete,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct PrimaryKeyConstraint {
    pub(crate) name: Ustr,
    pub(crate) columns: SmallVec<Ustr, 2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct UniqueConstraint {
    pub(crate) name: Ustr,
    pub columns: SmallVec<Ustr, 2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct NotNullConstraint {
    pub(crate) name: Ustr,
    pub column: Ustr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct DefaultConstraint {
    pub(crate) name: Ustr,
    pub column: Ustr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(tag = "type")]
pub enum Constraint {
    #[serde(rename = "c")]
    Check(CheckConstraint),
    #[serde(rename = "f")]
    ForeignKey(ForeignKeyConstraint),
    #[serde(rename = "p")]
    PrimaryKey(PrimaryKeyConstraint),
    #[serde(rename = "u")]
    Unique(UniqueConstraint),
    #[serde(rename = "n")]
    NotNull(NotNullConstraint),
    #[serde(rename = "d")]
    Default(DefaultConstraint),
}

impl FromSql<'_> for Constraint {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if *ty == Type::JSON || *ty == Type::JSONB {
            let json_str = std::str::from_utf8(raw)?;
            let parsed: Constraint = serde_json::from_str(json_str)?;
            Ok(parsed)
        } else {
            Err("unexpected datatype for Constraint".into())
        }
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSON || *ty == Type::JSONB
    }
}

impl Constraint {
    pub fn name(&self) -> Ustr {
        match self {
            Constraint::Check(check) => check.name,
            Constraint::ForeignKey(foreign_key) => foreign_key.name,
            Constraint::PrimaryKey(primary_key) => primary_key.name,
            Constraint::Unique(unique) => unique.name,
            Constraint::NotNull(not_null) => not_null.name,
            Constraint::Default(default) => default.name,
        }
    }

    /// Given a vec of column names, check whether any of those column names are present in the constraint.
    pub fn contains_columns(&self, cols: &Vec<Ustr>) -> bool {
        match self {
            Constraint::Check(c) => cols.iter().any(|item| c.columns.contains(item)),
            Constraint::ForeignKey(f) => cols.iter().any(|item| f.columns.contains(item)),
            Constraint::PrimaryKey(p) => cols.iter().any(|item| p.columns.contains(item)),
            Constraint::Unique(u) => cols.iter().any(|item| u.columns.contains(item)),
            Constraint::NotNull(n) => cols.contains(&n.column),
            Constraint::Default(d) => cols.contains(&d.column),
        }
    }
}

impl From<Constraint> for SqlState {
    fn from(c: Constraint) -> Self {
        match c {
            Constraint::Check(..) => SqlState::CHECK_VIOLATION,
            Constraint::ForeignKey(..) => SqlState::FOREIGN_KEY_VIOLATION,
            Constraint::PrimaryKey(..) => SqlState::UNIQUE_VIOLATION,
            Constraint::Unique(..) => SqlState::UNIQUE_VIOLATION,
            Constraint::NotNull(..) => SqlState::NOT_NULL_VIOLATION,
            _ => unimplemented!("No SQLSTATE for DEFAULT constraint"),
        }
    }
}

impl ToTokens for Constraint {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(sql_to_rs_ident(&self.name(), Pascal))
    }
}
