use crate::ident::sql_to_rs_ident;
use crate::ident::CaseType::Pascal;
use enum_dispatch::enum_dispatch;
use quote::ToTokens;
use quote::__private::TokenStream;
use smallvec::SmallVec;
use tokio_postgres::error::SqlState;
use ustr::Ustr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OnDelete {
    Restrict,
    Cascade,
    Nothing,
    SetNull,
    SetDefault,
}

#[enum_dispatch]
trait RsName {
    fn rs_name(&self) -> TokenStream;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CheckConstraint {
    pub(crate) name: Ustr,
    pub(crate) columns: SmallVec<Ustr, 2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ForeignKeyConstraint {
    pub(crate) name: Ustr,
    pub(crate) columns: SmallVec<Ustr, 2>,
    pub(crate) on_delete: OnDelete,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrimaryKeyConstraint {
    pub(crate) name: Ustr,
    pub(crate) columns: SmallVec<Ustr, 2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UniqueConstraint {
    pub(crate) name: Ustr,
    pub columns: SmallVec<Ustr, 2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NotNullConstraint {
    pub(crate) name: Ustr,
    pub column: Ustr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Constraint {
    Check(CheckConstraint),
    ForeignKey(ForeignKeyConstraint),
    PrimaryKey(PrimaryKeyConstraint),
    Unique(UniqueConstraint),
    NotNull(NotNullConstraint),
}

impl Constraint {
    pub fn name(&self) -> Ustr {
        match self {
            Constraint::Check(check) => check.name,
            Constraint::ForeignKey(foreign_key) => foreign_key.name,
            Constraint::PrimaryKey(primary_key) => primary_key.name,
            Constraint::Unique(unique) => unique.name,
            Constraint::NotNull(not_null) => not_null.name,
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
        }
    }
}

impl ToTokens for Constraint {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(sql_to_rs_ident(&self.name(), Pascal))
    }
}
