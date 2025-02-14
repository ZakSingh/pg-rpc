use ariadne::ReportKind;
use postgres_types::{FromSql, Type};
use serde::{Deserialize, Deserializer};
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use std::error::Error;
use std::str::FromStr;

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct PgSqlStatement {
    #[serde(rename = "lineNumber")]
    #[serde_as(as = "DisplayFromStr")]
    pub line_number: usize,
    pub text: String,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct PgSqlQuery {
    #[serde_as(as = "DisplayFromStr")]
    pub position: usize,
    pub text: String,
}

#[derive(Debug, Copy, Clone)]
pub enum PgSqlLevel {
    Warning,
    Error,
}


impl FromStr for PgSqlLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "warning" | "warning extra" => Ok(PgSqlLevel::Warning),
            "error" => Ok(PgSqlLevel::Error),
            s => Err(format!("Unknown PgSqlLevel: {}", s))
        }
    }
}

impl<'de> Deserialize<'de> for PgSqlLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
      D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse::<PgSqlLevel>().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Deserialize)]
pub struct PgSqlIssue {
    pub level: PgSqlLevel,
    pub message: String,
    #[serde(rename = "sqlState")]
    pub sql_state: String,
    pub statement: Option<PgSqlStatement>,
    pub query: Option<PgSqlQuery>,
    pub detail: Option<String>,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct PgSqlReport {
    pub issues: Vec<PgSqlIssue>,
}

impl FromSql<'_> for PgSqlReport {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if *ty == Type::JSON || *ty == Type::JSONB {
            let json_str = std::str::from_utf8(raw)?;
            let parsed: PgSqlReport = serde_json::from_str(json_str)?;
            Ok(parsed)
        } else {
            Err("unexpected datatype for PgSqlReport".into())
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

pub fn line_to_span(content: &str, line_number: usize) -> Option<std::ops::Range<usize>> {
    if line_number == 0 {
        return None;
    }

    let mut current_line = 1;
    let mut line_start = 0;

    // Find the start of our target line
    for (idx, c) in content.char_indices() {
        if current_line == line_number {
            line_start = idx;
            break;
        }
        if c == '\n' {
            current_line += 1;
        }
    }

    // If we never reached our line number, it's out of bounds
    if current_line != line_number {
        return None;
    }

    // Find the end of the line (next newline or end of string)
    let line_end = content[line_start..]
        .char_indices()
        .find(|(_, c)| *c == '\n')
        .map(|(idx, _)| line_start + idx)
        .unwrap_or(content.len());

    Some(line_start..line_end)
}
