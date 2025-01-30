use crate::PgFnBody;
use serde::de::{self, MapAccess, Visitor};
use serde::Deserialize;
use serde_json::Value;
use serde_json_path::JsonPath;
use std::fmt;
use tokio_postgres::error::SqlState;

/// Extract all explicit raised exceptions from a plpgsql function body
pub fn get_explicit_exceptions(body: &PgFnBody) -> Result<Vec<StmtRaise>, serde_json::Error> {
    match body {
        PgFnBody::SQL(_) => Ok(Vec::new()), // SQL queries can't do explicit raises
        PgFnBody::PgSQL(json) => JsonPath::parse("$..PLpgSQL_stmt_raise")
            .unwrap()
            .query(json)
            .into_iter()
            .cloned()
            .filter(is_exception)
            .map(serde_json::from_value)
            .collect(),
    }
}

/// Postgres exceptions have elog_level == 21.
fn is_exception(raise_json: &Value) -> bool {
    raise_json
        .get("elog_level")
        .and_then(|l| l.as_i64())
        .map_or(false, |v| v == 21)
}

#[derive(Debug)]
pub struct StmtRaise {
    pub elog_level: i32,
    pub sql_state: SqlState,
    pub message: Option<String>,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub column: Option<String>,
    pub constraint: Option<String>,
    pub data_type: Option<String>,
    pub table: Option<String>,
    pub schema: Option<String>,
}

// Custom deserializer for StmtRaise
impl<'de> Deserialize<'de> for StmtRaise {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            ElogLevel,
            Message,
            #[serde(rename = "condname")]
            CondName,
            Options,
            #[serde(rename = "lineno")]
            Lineno,
            Params,
        }

        struct StmtRaiseVisitor;

        impl<'de> Visitor<'de> for StmtRaiseVisitor {
            type Value = StmtRaise;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct StmtRaise")
            }

            fn visit_map<V>(self, mut map: V) -> Result<StmtRaise, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut elog_level = None;
                let mut message = None;
                let mut sql_state = SqlState::RAISE_EXCEPTION;
                let mut detail = None;
                let mut hint = None;
                let mut column = None;
                let mut constraint = None;
                let mut data_type = None;
                let mut table = None;
                let mut schema = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::ElogLevel => {
                            elog_level = Some(map.next_value()?);
                        }
                        Field::Message => {
                            message = Some(map.next_value()?);
                        }
                        Field::Lineno => {
                            let _: i32 = map.next_value()?;
                        }
                        Field::CondName => {
                            let value: String = map.next_value()?;
                            // First try to parse as a SQL state code
                            sql_state = if value.len() == 5 {
                                SqlState::from_code(&value)
                            } else {
                                // Otherwise treat as a condition name
                                match value.as_str() {
                                    "unique_violation" => SqlState::UNIQUE_VIOLATION,
                                    "foreign_key_violation" => SqlState::FOREIGN_KEY_VIOLATION,
                                    "not_null_violation" => SqlState::NOT_NULL_VIOLATION,
                                    "check_violation" => SqlState::CHECK_VIOLATION,
                                    "invalid_transaction_state" => {
                                        SqlState::INVALID_TRANSACTION_STATE
                                    }
                                    "active_sql_transaction" => SqlState::ACTIVE_SQL_TRANSACTION,
                                    "invalid_cursor_name" => SqlState::INVALID_CURSOR_NAME,
                                    "invalid_catalog_name" => SqlState::INVALID_CATALOG_NAME,
                                    "syntax_error" => SqlState::SYNTAX_ERROR,
                                    "insufficient_privilege" => SqlState::INSUFFICIENT_PRIVILEGE,
                                    "division_by_zero" => SqlState::DIVISION_BY_ZERO,
                                    c => {
                                        return Err(de::Error::custom(format!(
                                            "Unknown condition name: {c}"
                                        )))
                                    }
                                }
                            };
                        }
                        Field::Options => {
                            let options: Vec<serde_json::Value> = map.next_value()?; // Parse options as raw JSON values
                            for option in options {
                                let option = option.get("PLpgSQL_raise_option").unwrap();
                                if let Some(opt_type) =
                                    option.get("opt_type").and_then(|v| v.as_i64())
                                {
                                    if let Some(expr) = option
                                        .get("expr")
                                        .and_then(|e| e.get("PLpgSQL_expr"))
                                        .and_then(|e| e.get("query"))
                                        .and_then(|q| q.as_str())
                                    {
                                        match opt_type {
                                            0 => sql_state = SqlState::from_code(expr),
                                            1 => message = Some(expr.to_string()),
                                            2 => detail = Some(expr.to_string()),
                                            3 => hint = Some(expr.to_string()),
                                            4 => column = Some(expr.to_string()),
                                            5 => constraint = Some(expr.to_string()),
                                            6 => data_type = Some(expr.to_string()),
                                            7 => table = Some(expr.to_string()),
                                            8 => schema = Some(expr.to_string()),
                                            _ => {
                                                return Err(de::Error::custom(format!(
                                                    "Unknown opt_type: {}",
                                                    opt_type
                                                )))
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Field::Params => {
                            // String parameters used in the message
                            // We don't need these
                        }
                    }
                }

                Ok(StmtRaise {
                    elog_level: elog_level.ok_or_else(|| de::Error::missing_field("elog_level"))?,
                    message,
                    sql_state,
                    detail,
                    hint,
                    column,
                    constraint,
                    data_type,
                    table,
                    schema,
                })
            }
        }

        const FIELDS: &[&str] = &["elog_level", "message", "condname", "options"];
        deserializer.deserialize_struct("StmtRaise", FIELDS, StmtRaiseVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_with_sql_state_code() {
        let json_str = r#"{
            "elog_level": 21,
            "condname": "23505"
        }"#;

        let stmt: StmtRaise = serde_json::from_str(json_str).unwrap();
        assert_eq!(stmt.sql_state.code(), "23505");
    }

    #[test]
    fn test_deserialize_with_sql_state_name() {
        let json_str = r#"{
            "elog_level": 21,
            "condname": "unique_violation"
        }"#;

        let stmt: StmtRaise = serde_json::from_str(json_str).unwrap();
        assert_eq!(stmt.sql_state.code(), "23505");
    }

    #[test]
    fn test_deserialize_with_option_sql_state() {
        let json_str = r#"{
            "elog_level": 21,
            "options": [
                {
                    "PLpgSQL_raise_option": {
                        "expr": {
                            "PLpgSQL_expr": {
                                "parseMode": 2,
                                "query": "23505"
                            }
                        },
                        "opt_type": 0
                    }
                }
            ]
        }"#;

        let stmt: StmtRaise = serde_json::from_str(json_str).unwrap();
        assert_eq!(stmt.sql_state.code(), "23505");
    }

    #[test]
    fn test_deserialize_option_with_details() {
        let json_str = r#"{
            "elog_level": 21,
            "options": [
                {
                    "PLpgSQL_raise_option": {
                        "expr": {
                            "PLpgSQL_expr": {
                                "parseMode": 2,
                                "query": "detail message"
                            }
                        },
                        "opt_type": 2
                    }
                }
            ]
        }"#;

        let stmt: StmtRaise = serde_json::from_str(json_str).unwrap();
        assert_eq!(stmt.detail, Some("detail message".to_string()));
    }

    #[test]
    fn test_deserialize_with_mixed_sql_states() {
        let json_str = r#"{
            "elog_level": 21,
            "condname": "unique_violation",
            "options": [
                {
                    "PLpgSQL_raise_option": {
                        "expr": {
                            "PLpgSQL_expr": {
                                "parseMode": 2,
                                "query": "23503"
                            }
                        },
                        "opt_type": 0
                    }
                }
            ]
        }"#;

        let stmt: StmtRaise = serde_json::from_str(json_str).unwrap();
        // Option should override field
        assert_eq!(stmt.sql_state.code(), "23503");
    }
}
