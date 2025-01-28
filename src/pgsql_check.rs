use crate::infer_types::OID;
use ariadne::{Report, ReportKind};
use postgres_types::{FromSql, Type};
use serde::Deserialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use std::error::Error;

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct PgSqlStatement {
    #[serde(rename = "lineNumber")]
    #[serde_as(as = "DisplayFromStr")]
    line_number: u32,
    text: String,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct PgSqlQuery {
    #[serde(rename = "lineNumber")]
    #[serde_as(as = "DisplayFromStr")]
    position: u32,
    text: String,
}

#[derive(Debug, Deserialize, Copy, Clone)]
pub enum PgSqlLevel {
    #[serde(rename = "warning extra")]
    Warning,
    #[serde(rename = "error")]
    Error,
}

#[derive(Debug, Deserialize)]
pub struct PgSqlIssue {
    level: PgSqlLevel,
    message: String,
    #[serde(rename = "sqlState")]
    sql_state: String,
    statement: Option<PgSqlStatement>,
    query: Option<PgSqlQuery>,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct PgSqlReport {
    #[serde_as(as = "DisplayFromStr")]
    function: OID,
    pub issues: Vec<PgSqlIssue>,
}

impl FromSql<'_> for PgSqlReport {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if *ty == Type::JSON || *ty == Type::JSONB {
            // Parse the raw bytes as a JSON string
            let json_str = std::str::from_utf8(raw)?;

            // Use serde to deserialize the JSON into our struct
            let parsed: PgSqlReport = serde_json::from_str(json_str)?;
            Ok(parsed)
        } else {
            Err("unexpected datatype for MyJsonData".into())
        }
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSON || *ty == Type::JSONB
    }
}

impl From<PgSqlLevel> for ReportKind<'_> {
    fn from(value: PgSqlLevel) -> Self {
        match value {
            PgSqlLevel::Warning => ReportKind::Warning,
            PgSqlLevel::Error => ReportKind::Error,
        }
    }
}

impl PgSqlIssue {
    pub fn display(&self) -> String {
        String::from("hello")
        // Report::build(self.level.into())
    }
}
