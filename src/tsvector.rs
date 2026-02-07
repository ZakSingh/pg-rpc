//! PostgreSQL full-text search types with proper binary protocol support.
//!
//! This module provides Rust types for PostgreSQL's `tsvector` and `tsquery` types
//! with proper `FromSql`/`ToSql` implementations that handle the binary wire protocol.

use postgres_types::private::BytesMut;
use postgres_types::{FromSql, IsNull, ToSql, Type};
use std::error::Error;

/// PostgreSQL tsvector type - a sorted list of distinct lexemes with optional positions/weights.
///
/// A tsvector value is a sorted list of distinct lexemes, which are words that have been
/// normalized to merge different variants of the same word. Lexemes can have optional
/// position and weight information for ranking purposes.
///
/// # Example
/// ```ignore
/// // In PostgreSQL: to_tsvector('english', 'The quick brown fox')
/// // Produces: 'brown':3 'fox':4 'quick':2
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TsVector(pub String);

/// PostgreSQL tsquery type - a search query for full-text search.
///
/// A tsquery value stores lexemes that are to be searched for, and can combine them
/// using Boolean operators & (AND), | (OR), and ! (NOT), as well as the phrase search
/// operator <-> (FOLLOWED BY).
///
/// # Example
/// ```ignore
/// // In PostgreSQL: to_tsquery('english', 'quick & fox')
/// // Produces: 'quick' & 'fox'
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TsQuery(pub String);

impl<'a> FromSql<'a> for TsVector {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(TsVector(parse_tsvector_binary(raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "tsvector"
    }
}

impl<'a> FromSql<'a> for TsQuery {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(TsQuery(parse_tsquery_binary(raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "tsquery"
    }
}

// ToSql implementations - send as text representation, PostgreSQL will parse
impl ToSql for TsVector {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // Send as text - PostgreSQL will parse it
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "tsvector"
    }

    postgres_types::to_sql_checked!();
}

impl ToSql for TsQuery {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // Send as text - PostgreSQL will parse it
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "tsquery"
    }

    postgres_types::to_sql_checked!();
}

impl std::fmt::Display for TsVector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for TsQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for TsVector {
    fn from(s: String) -> Self {
        TsVector(s)
    }
}

impl From<&str> for TsVector {
    fn from(s: &str) -> Self {
        TsVector(s.to_string())
    }
}

impl From<String> for TsQuery {
    fn from(s: String) -> Self {
        TsQuery(s)
    }
}

impl From<&str> for TsQuery {
    fn from(s: &str) -> Self {
        TsQuery(s.to_string())
    }
}

/// Parse PostgreSQL tsvector binary format.
///
/// Binary format:
/// - 4 bytes: number of lexemes (int32, big-endian)
/// - For each lexeme:
///   - null-terminated string (the lexeme text)
///   - 2 bytes: number of positions (int16, big-endian)
///   - For each position:
///     - 2 bytes: position with weight in high bits (int16, big-endian)
fn parse_tsvector_binary(raw: &[u8]) -> Result<String, Box<dyn Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err("tsvector binary data too short".into());
    }

    let num_lexemes = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let mut pos = 4;
    let mut lexemes = Vec::with_capacity(num_lexemes);

    for _ in 0..num_lexemes {
        // Read null-terminated lexeme string
        let lexeme_start = pos;
        while pos < raw.len() && raw[pos] != 0 {
            pos += 1;
        }
        if pos >= raw.len() {
            return Err("tsvector binary data truncated in lexeme".into());
        }

        let lexeme = std::str::from_utf8(&raw[lexeme_start..pos])
            .map_err(|e| format!("invalid UTF-8 in tsvector lexeme: {}", e))?;
        pos += 1; // Skip null terminator

        // Read number of positions
        if pos + 2 > raw.len() {
            return Err("tsvector binary data truncated in position count".into());
        }
        let num_positions = u16::from_be_bytes([raw[pos], raw[pos + 1]]) as usize;
        pos += 2;

        // Read positions
        let mut positions = Vec::with_capacity(num_positions);
        for _ in 0..num_positions {
            if pos + 2 > raw.len() {
                return Err("tsvector binary data truncated in positions".into());
            }
            let pos_weight = u16::from_be_bytes([raw[pos], raw[pos + 1]]);
            pos += 2;

            // Extract position (lower 14 bits) and weight (upper 2 bits)
            let position = pos_weight & 0x3FFF;
            let weight = (pos_weight >> 14) & 0x03;

            let weight_char = match weight {
                3 => "A",
                2 => "B",
                1 => "C",
                _ => "", // D weight is default, not shown
            };

            if weight_char.is_empty() {
                positions.push(format!("{}", position));
            } else {
                positions.push(format!("{}{}", position, weight_char));
            }
        }

        // Format lexeme with positions
        if positions.is_empty() {
            lexemes.push(format!("'{}'", lexeme));
        } else {
            lexemes.push(format!("'{}':{}",lexeme, positions.join(",")));
        }
    }

    Ok(lexemes.join(" "))
}

/// Parse PostgreSQL tsquery binary format.
///
/// Binary format uses a stack-based representation:
/// - 4 bytes: number of items (int32, big-endian)
/// - For each item:
///   - 1 byte: item type (1=VAL, 2=OPR)
///   - 1 byte: weight (for VAL) or operator type (for OPR)
///   - 1 byte: prefix flag (for VAL only)
///   - null-terminated string (for VAL only)
fn parse_tsquery_binary(raw: &[u8]) -> Result<String, Box<dyn Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err("tsquery binary data too short".into());
    }

    let num_items = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;

    if num_items == 0 {
        return Ok(String::new());
    }

    let mut pos = 4;
    let mut stack: Vec<String> = Vec::new();

    for _ in 0..num_items {
        if pos >= raw.len() {
            return Err("tsquery binary data truncated".into());
        }

        let item_type = raw[pos];
        pos += 1;

        match item_type {
            1 => {
                // QI_VAL: lexeme value
                if pos + 2 > raw.len() {
                    return Err("tsquery binary data truncated in VAL".into());
                }

                let weight = raw[pos];
                let prefix = raw[pos + 1];
                pos += 2;

                // Read null-terminated lexeme
                let lexeme_start = pos;
                while pos < raw.len() && raw[pos] != 0 {
                    pos += 1;
                }
                if pos >= raw.len() {
                    return Err("tsquery binary data truncated in lexeme".into());
                }

                let lexeme = std::str::from_utf8(&raw[lexeme_start..pos])
                    .map_err(|e| format!("invalid UTF-8 in tsquery lexeme: {}", e))?;
                pos += 1; // Skip null terminator

                // Build lexeme string with optional weight and prefix
                let mut lexeme_str = format!("'{}'", lexeme);

                // Add prefix marker if set
                if prefix != 0 {
                    lexeme_str.push_str(":*");
                }

                // Add weight if not default (all weights)
                if weight != 0 && weight != 0x0F {
                    let mut weights = String::new();
                    if weight & 0x08 != 0 {
                        weights.push('A');
                    }
                    if weight & 0x04 != 0 {
                        weights.push('B');
                    }
                    if weight & 0x02 != 0 {
                        weights.push('C');
                    }
                    if weight & 0x01 != 0 {
                        weights.push('D');
                    }
                    if !weights.is_empty() {
                        if prefix != 0 {
                            // Already has :*, append weights
                            lexeme_str = format!("'{}':{}", lexeme, weights);
                            lexeme_str.push('*');
                        } else {
                            lexeme_str = format!("'{}':{}", lexeme, weights);
                        }
                    }
                }

                stack.push(lexeme_str);
            }
            2 => {
                // QI_OPR: operator
                if pos >= raw.len() {
                    return Err("tsquery binary data truncated in OPR".into());
                }

                let oper = raw[pos];
                pos += 1;

                match oper {
                    1 => {
                        // OP_NOT: unary operator
                        if let Some(operand) = stack.pop() {
                            stack.push(format!("!{}", operand));
                        }
                    }
                    2 => {
                        // OP_AND: binary operator
                        if stack.len() >= 2 {
                            let right = stack.pop().unwrap();
                            let left = stack.pop().unwrap();
                            stack.push(format!("{} & {}", left, right));
                        }
                    }
                    3 => {
                        // OP_OR: binary operator
                        if stack.len() >= 2 {
                            let right = stack.pop().unwrap();
                            let left = stack.pop().unwrap();
                            stack.push(format!("( {} | {} )", left, right));
                        }
                    }
                    4 => {
                        // OP_PHRASE: phrase operator with distance
                        if pos + 2 > raw.len() {
                            return Err("tsquery binary data truncated in PHRASE distance".into());
                        }
                        let distance = u16::from_be_bytes([raw[pos], raw[pos + 1]]);
                        pos += 2;

                        if stack.len() >= 2 {
                            let right = stack.pop().unwrap();
                            let left = stack.pop().unwrap();
                            if distance == 1 {
                                stack.push(format!("{} <-> {}", left, right));
                            } else {
                                stack.push(format!("{} <{}> {}", left, distance, right));
                            }
                        }
                    }
                    _ => {
                        return Err(format!("unknown tsquery operator: {}", oper).into());
                    }
                }
            }
            _ => {
                return Err(format!("unknown tsquery item type: {}", item_type).into());
            }
        }
    }

    // The final result should be on the stack
    Ok(stack.pop().unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tsvector_display() {
        let tv = TsVector("'quick':2 'fox':4".to_string());
        assert_eq!(tv.to_string(), "'quick':2 'fox':4");
    }

    #[test]
    fn test_tsquery_display() {
        let tq = TsQuery("'quick' & 'fox'".to_string());
        assert_eq!(tq.to_string(), "'quick' & 'fox'");
    }

    #[test]
    fn test_tsvector_from_string() {
        let tv = TsVector::from("test vector");
        assert_eq!(tv.0, "test vector");
    }

    #[test]
    fn test_tsquery_from_string() {
        let tq = TsQuery::from("test query");
        assert_eq!(tq.0, "test query");
    }

    #[test]
    fn test_tsvector_serde() {
        let tv = TsVector("'test':1".to_string());
        let json = serde_json::to_string(&tv).unwrap();
        assert_eq!(json, "\"'test':1\"");

        let parsed: TsVector = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, tv);
    }

    #[test]
    fn test_tsquery_serde() {
        let tq = TsQuery("'a' & 'b'".to_string());
        let json = serde_json::to_string(&tq).unwrap();
        assert_eq!(json, "\"'a' & 'b'\"");

        let parsed: TsQuery = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, tq);
    }
}
