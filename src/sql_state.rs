use crate::ident::sql_to_rs_ident;
use crate::ident::CaseType::Pascal;
use crate::pg_constraint::Constraint;
use phf::phf_map;
use quote::ToTokens;
use quote::__private::TokenStream;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

#[derive(Clone, Eq, Debug)]
pub struct SqlState(tokio_postgres::error::SqlState);

impl Default for SqlState {
    fn default() -> Self {
        Self(tokio_postgres::error::SqlState::RAISE_EXCEPTION)
    }
}

impl PartialEq for SqlState {
    fn eq(&self, other: &Self) -> bool {
        self.0.code() == other.0.code()
    }
}

impl Hash for SqlState {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.code().hash(state); // Hashing only the string code
    }
}

impl Deref for SqlState {
    type Target = tokio_postgres::error::SqlState;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SqlState {
    pub fn from_code(code: &str) -> Self {
        Self(tokio_postgres::error::SqlState::from_code(code))
    }
}

impl ToTokens for SqlState {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(sql_to_rs_ident(&self.code(), Pascal))
    }
}

// Convert SQL state codes to names
// TODO: fill in the rest
pub static SYM_SQL_STATE_TO_CODE: phf::Map<&'static str, SqlState> = phf_map! {
    "unique_violation" => SqlState(tokio_postgres::error::SqlState::UNIQUE_VIOLATION),
    "foreign_key_violation" => SqlState(tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION),
    "not_null_violation" => SqlState(tokio_postgres::error::SqlState::NOT_NULL_VIOLATION),
    "check_violation" => SqlState(tokio_postgres::error::SqlState::CHECK_VIOLATION),
    "invalid_transaction_state" => {
        SqlState(tokio_postgres::error::SqlState::INVALID_TRANSACTION_STATE)
    },
    "active_sql_transaction" => SqlState(tokio_postgres::error::SqlState::ACTIVE_SQL_TRANSACTION),
    "invalid_cursor_name" => SqlState(tokio_postgres::error::SqlState::INVALID_CURSOR_NAME),
    "invalid_catalog_name" => SqlState(tokio_postgres::error::SqlState::INVALID_CATALOG_NAME),
    "syntax_error" => SqlState(tokio_postgres::error::SqlState::SYNTAX_ERROR),
    "insufficient_privilege" => SqlState(tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE),
    "division_by_zero" => SqlState(tokio_postgres::error::SqlState::DIVISION_BY_ZERO),
};
