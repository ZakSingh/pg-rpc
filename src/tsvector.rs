//! PostgreSQL full-text search types with binary protocol support.
//!
//! `TsVector` and `TsQuery` hold the text form of a `tsvector` / `tsquery`
//! value, the same text Postgres prints and accepts in a cast. Parameters and
//! results travel in the binary wire format, so the text is parsed when a
//! value is sent and rendered when one is received. As with a cast (and
//! unlike `to_tsvector` / `to_tsquery`), nothing is stemmed or normalized.
//!
//! This file is also emitted, verbatim, as the shared `tsvector` module of
//! the generated code, so it must not depend on anything else in the crate.

use postgres_types::private::BytesMut;
use postgres_types::{FromSql, IsNull, ToSql, Type};
use std::error::Error;

type BoxError = Box<dyn Error + Sync + Send>;

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
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TsQuery(pub String);

impl<'a> FromSql<'a> for TsVector {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, BoxError> {
        Ok(TsVector(parse_tsvector_binary(raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "tsvector"
    }
}

impl<'a> FromSql<'a> for TsQuery {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, BoxError> {
        Ok(TsQuery(parse_tsquery_binary(raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "tsquery"
    }
}

impl ToSql for TsVector {
    fn to_sql(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, BoxError> {
        write_tsvector_binary(&self.0, out)?;
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "tsvector"
    }

    postgres_types::to_sql_checked!();
}

impl ToSql for TsQuery {
    fn to_sql(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, BoxError> {
        write_tsquery_binary(&self.0, out)?;
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

/// Highest position a tsvector can record; larger ones are clamped to it.
const MAX_POSITION: u16 = (1 << 14) - 1;
/// Most positions kept per lexeme.
const MAX_POSITIONS: usize = 256;
/// Longest lexeme, in bytes.
const MAX_LEXEME_LEN: usize = (1 << 11) - 1;
/// Largest distance of a phrase operator.
const MAX_DISTANCE: u16 = 1 << 14;

/// `'lexeme'`, with quotes and backslashes doubled as Postgres prints them.
fn push_quoted(out: &mut String, lexeme: &str) {
    out.push('\'');
    for c in lexeme.chars() {
        if c == '\'' || c == '\\' {
            out.push(c);
        }
        out.push(c);
    }
    out.push('\'');
}

/// A NUL-terminated string starting at `*pos`, which is left after the NUL.
fn read_cstr<'a>(raw: &'a [u8], pos: &mut usize, what: &str) -> Result<&'a str, BoxError> {
    let rest = raw.get(*pos..).unwrap_or_default();
    let len = rest
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| format!("{what} binary data truncated in lexeme"))?;
    let lexeme = std::str::from_utf8(&rest[..len])
        .map_err(|e| format!("invalid UTF-8 in {what} lexeme: {e}"))?;
    *pos += len + 1;
    Ok(lexeme)
}

fn read_u16(raw: &[u8], pos: &mut usize, what: &str) -> Result<u16, BoxError> {
    let bytes = raw
        .get(*pos..*pos + 2)
        .ok_or_else(|| format!("{what} binary data truncated"))?;
    *pos += 2;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u8(raw: &[u8], pos: &mut usize, what: &str) -> Result<u8, BoxError> {
    let byte = *raw
        .get(*pos)
        .ok_or_else(|| format!("{what} binary data truncated"))?;
    *pos += 1;
    Ok(byte)
}

fn read_count(raw: &[u8], what: &str) -> Result<usize, BoxError> {
    let bytes = raw
        .get(..4)
        .ok_or_else(|| format!("{what} binary data too short"))?;
    let count = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    usize::try_from(count).map_err(|_| format!("invalid {what} size {count}").into())
}

/// A lexeme of a tsquery or tsvector in text form, starting at `chars[*pos]`:
/// quoted (`'two words'`, with `''` or `\'` for a quote) or a bare run up to
/// whitespace or one of `stop`. A backslash takes the next character
/// literally in both forms.
fn read_lexeme(chars: &[char], pos: &mut usize, stop: &[char]) -> Result<String, BoxError> {
    let mut lexeme = String::new();
    if chars.get(*pos) == Some(&'\'') {
        *pos += 1;
        loop {
            match chars.get(*pos) {
                None => return Err("unterminated quoted lexeme".into()),
                Some('\'') if chars.get(*pos + 1) == Some(&'\'') => {
                    lexeme.push('\'');
                    *pos += 2;
                }
                Some('\'') => {
                    *pos += 1;
                    break;
                }
                Some('\\') => {
                    let escaped = chars.get(*pos + 1).ok_or("dangling backslash in lexeme")?;
                    lexeme.push(*escaped);
                    *pos += 2;
                }
                Some(c) => {
                    lexeme.push(*c);
                    *pos += 1;
                }
            }
        }
    } else {
        while let Some(&c) = chars.get(*pos) {
            if c.is_whitespace() || stop.contains(&c) {
                break;
            }
            if c == '\\' {
                let escaped = chars.get(*pos + 1).ok_or("dangling backslash in lexeme")?;
                lexeme.push(*escaped);
                *pos += 2;
            } else {
                lexeme.push(c);
                *pos += 1;
            }
        }
    }
    if lexeme.is_empty() {
        return Err(format!("expected a lexeme at character {}", *pos + 1).into());
    }
    if lexeme.len() > MAX_LEXEME_LEN {
        return Err(format!("lexeme is too long ({} bytes, max {MAX_LEXEME_LEN})", lexeme.len()).into());
    }
    Ok(lexeme)
}

fn skip_whitespace(chars: &[char], pos: &mut usize) {
    while chars.get(*pos).is_some_and(|c| c.is_whitespace()) {
        *pos += 1;
    }
}

/// Parse PostgreSQL tsvector binary format into its text form.
///
/// Binary format:
/// - 4 bytes: number of lexemes (int32, big-endian)
/// - For each lexeme:
///   - null-terminated string (the lexeme text)
///   - 2 bytes: number of positions (int16, big-endian)
///   - For each position:
///     - 2 bytes: weight in the high 2 bits, position in the low 14
fn parse_tsvector_binary(raw: &[u8]) -> Result<String, BoxError> {
    let num_lexemes = read_count(raw, "tsvector")?;
    let mut pos = 4;
    let mut text = String::new();

    for i in 0..num_lexemes {
        let lexeme = read_cstr(raw, &mut pos, "tsvector")?;
        if i > 0 {
            text.push(' ');
        }
        push_quoted(&mut text, lexeme);

        let num_positions = read_u16(raw, &mut pos, "tsvector")?;
        for j in 0..num_positions {
            let entry = read_u16(raw, &mut pos, "tsvector")?;
            text.push(if j == 0 { ':' } else { ',' });
            text.push_str(&(entry & MAX_POSITION).to_string());
            // Weight D is the default and is not shown.
            match entry >> 14 {
                3 => text.push('A'),
                2 => text.push('B'),
                1 => text.push('C'),
                _ => {}
            }
        }
    }

    Ok(text)
}

/// Encode the text form of a tsvector (`'fat':1A,3 cat 'two words':2`) in the
/// binary format. Lexemes are sorted and merged and positions sorted, as the
/// server requires of binary input.
fn write_tsvector_binary(text: &str, out: &mut BytesMut) -> Result<(), BoxError> {
    let chars: Vec<char> = text.chars().collect();
    let mut pos = 0;
    // lexeme -> (position, weight 0..=3)
    let mut lexemes: std::collections::BTreeMap<Vec<u8>, Vec<(u16, u16)>> =
        std::collections::BTreeMap::new();

    loop {
        skip_whitespace(&chars, &mut pos);
        if pos >= chars.len() {
            break;
        }
        let lexeme = read_lexeme(&chars, &mut pos, &[':'])
            .map_err(|e| format!("invalid tsvector {text:?}: {e}"))?;
        let positions = lexemes.entry(lexeme.into_bytes()).or_default();

        if chars.get(pos) != Some(&':') {
            continue;
        }
        pos += 1;
        loop {
            let digits_start = pos;
            while chars.get(pos).is_some_and(|c| c.is_ascii_digit()) {
                pos += 1;
            }
            let digits: String = chars[digits_start..pos].iter().collect();
            let position = match digits.parse::<u32>() {
                Ok(0) | Err(_) => {
                    return Err(format!(
                        "invalid tsvector {text:?}: expected a position at character {}",
                        digits_start + 1
                    )
                    .into())
                }
                Ok(position) => position.min(u32::from(MAX_POSITION)) as u16,
            };
            let weight = match chars.get(pos).map(|c| c.to_ascii_uppercase()) {
                Some('A') => Some(3),
                Some('B') => Some(2),
                Some('C') => Some(1),
                Some('D') => Some(0),
                _ => None,
            };
            if weight.is_some() {
                pos += 1;
            }
            positions.push((position, weight.unwrap_or(0)));
            if chars.get(pos) == Some(&',') {
                pos += 1;
            } else {
                break;
            }
        }
        if chars.get(pos).is_some_and(|c| !c.is_whitespace()) {
            return Err(format!(
                "invalid tsvector {text:?}: unexpected character at {}",
                pos + 1
            )
            .into());
        }
    }

    let count = i32::try_from(lexemes.len()).map_err(|_| "tsvector has too many lexemes")?;
    out.extend_from_slice(&count.to_be_bytes());
    for (lexeme, mut positions) in lexemes {
        // One entry per position, the heaviest weight winning.
        positions.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        positions.dedup_by_key(|entry| entry.0);
        positions.truncate(MAX_POSITIONS);

        out.extend_from_slice(&lexeme);
        out.extend_from_slice(&[0]);
        out.extend_from_slice(&(positions.len() as u16).to_be_bytes());
        for (position, weight) in positions {
            out.extend_from_slice(&((weight << 14) | position).to_be_bytes());
        }
    }
    Ok(())
}

/// A tsquery as a tree. Operator codes and priorities are Postgres's.
enum QueryNode {
    Value {
        lexeme: String,
        /// Bit 3 = A, bit 2 = B, bit 1 = C, bit 0 = D; 0 for any weight.
        weight: u8,
        prefix: bool,
    },
    Not(Box<QueryNode>),
    And(Box<QueryNode>, Box<QueryNode>),
    Or(Box<QueryNode>, Box<QueryNode>),
    /// `left <distance> right`; `<->` is distance 1.
    Phrase(Box<QueryNode>, Box<QueryNode>, u16),
}

const ITEM_VALUE: u8 = 1;
const ITEM_OPERATOR: u8 = 2;
const OP_NOT: u8 = 1;
const OP_AND: u8 = 2;
const OP_OR: u8 = 3;
const OP_PHRASE: u8 = 4;

impl QueryNode {
    /// Binding strength, as in Postgres: `|` < `&` < `<->` < `!`.
    fn priority(&self) -> i32 {
        match self {
            QueryNode::Or(..) => 1,
            QueryNode::And(..) => 2,
            QueryNode::Phrase(..) => 3,
            QueryNode::Not(..) => 4,
            QueryNode::Value { .. } => 5,
        }
    }

    /// Number of items this tree occupies on the wire.
    fn len(&self) -> usize {
        match self {
            QueryNode::Value { .. } => 1,
            QueryNode::Not(operand) => 1 + operand.len(),
            QueryNode::And(left, right)
            | QueryNode::Or(left, right)
            | QueryNode::Phrase(left, right, _) => 1 + left.len() + right.len(),
        }
    }

    /// Write the items in the order the server stores and sends them: an
    /// operator, then its right operand's items, then its left operand's.
    fn write(&self, out: &mut BytesMut) {
        match self {
            QueryNode::Value {
                lexeme,
                weight,
                prefix,
            } => {
                out.extend_from_slice(&[ITEM_VALUE, *weight, u8::from(*prefix)]);
                out.extend_from_slice(lexeme.as_bytes());
                out.extend_from_slice(&[0]);
            }
            QueryNode::Not(operand) => {
                out.extend_from_slice(&[ITEM_OPERATOR, OP_NOT]);
                operand.write(out);
            }
            QueryNode::And(left, right) => {
                out.extend_from_slice(&[ITEM_OPERATOR, OP_AND]);
                right.write(out);
                left.write(out);
            }
            QueryNode::Or(left, right) => {
                out.extend_from_slice(&[ITEM_OPERATOR, OP_OR]);
                right.write(out);
                left.write(out);
            }
            QueryNode::Phrase(left, right, distance) => {
                out.extend_from_slice(&[ITEM_OPERATOR, OP_PHRASE]);
                out.extend_from_slice(&distance.to_be_bytes());
                right.write(out);
                left.write(out);
            }
        }
    }

    /// Read one tree from the item list (see [`QueryNode::write`] for the order).
    fn read(raw: &[u8], pos: &mut usize, remaining: &mut usize) -> Result<QueryNode, BoxError> {
        if *remaining == 0 {
            return Err("tsquery binary data has an operator without its operand".into());
        }
        *remaining -= 1;

        match read_u8(raw, pos, "tsquery")? {
            ITEM_VALUE => {
                let weight = read_u8(raw, pos, "tsquery")?;
                let prefix = read_u8(raw, pos, "tsquery")? != 0;
                let lexeme = read_cstr(raw, pos, "tsquery")?.to_string();
                Ok(QueryNode::Value {
                    lexeme,
                    weight,
                    prefix,
                })
            }
            ITEM_OPERATOR => match read_u8(raw, pos, "tsquery")? {
                OP_NOT => Ok(QueryNode::Not(Box::new(Self::read(raw, pos, remaining)?))),
                operator @ (OP_AND | OP_OR | OP_PHRASE) => {
                    let distance = if operator == OP_PHRASE {
                        read_u16(raw, pos, "tsquery")?
                    } else {
                        0
                    };
                    let right = Box::new(Self::read(raw, pos, remaining)?);
                    let left = Box::new(Self::read(raw, pos, remaining)?);
                    Ok(match operator {
                        OP_AND => QueryNode::And(left, right),
                        OP_OR => QueryNode::Or(left, right),
                        _ => QueryNode::Phrase(left, right, distance),
                    })
                }
                other => Err(format!("unknown tsquery operator: {other}").into()),
            },
            other => Err(format!("unknown tsquery item type: {other}").into()),
        }
    }

    /// Append the text form, parenthesized the way Postgres prints it: an
    /// operand that binds less tightly than its parent is wrapped, and so is
    /// a phrase on the right of a phrase (phrases do not associate).
    fn render(&self, parent_priority: i32, right_of_phrase: bool, out: &mut String) {
        let priority = self.priority();
        match self {
            QueryNode::Value {
                lexeme,
                weight,
                prefix,
            } => {
                push_quoted(out, lexeme);
                if *prefix || *weight != 0 {
                    out.push(':');
                    if *prefix {
                        out.push('*');
                    }
                    for (bit, letter) in [(8, 'A'), (4, 'B'), (2, 'C'), (1, 'D')] {
                        if weight & bit != 0 {
                            out.push(letter);
                        }
                    }
                }
            }
            QueryNode::Not(operand) => {
                out.push('!');
                operand.render(priority, false, out);
            }
            QueryNode::And(left, right)
            | QueryNode::Or(left, right)
            | QueryNode::Phrase(left, right, _) => {
                let is_phrase = matches!(self, QueryNode::Phrase(..));
                let parenthesize = priority < parent_priority || (is_phrase && right_of_phrase);
                if parenthesize {
                    out.push_str("( ");
                }
                left.render(priority, false, out);
                match self {
                    QueryNode::And(..) => out.push_str(" & "),
                    QueryNode::Or(..) => out.push_str(" | "),
                    QueryNode::Phrase(_, _, 1) => out.push_str(" <-> "),
                    QueryNode::Phrase(_, _, distance) => {
                        out.push_str(&format!(" <{distance}> "));
                    }
                    _ => {}
                }
                right.render(priority, is_phrase, out);
                if parenthesize {
                    out.push_str(" )");
                }
            }
        }
    }
}

/// Recursive-descent parser for the text form of a tsquery.
struct QueryParser<'a> {
    chars: &'a [char],
    pos: usize,
}

impl QueryParser<'_> {
    fn peek(&mut self) -> Option<char> {
        skip_whitespace(self.chars, &mut self.pos);
        self.chars.get(self.pos).copied()
    }

    fn error(&self, message: &str) -> BoxError {
        format!("{message} at character {}", self.pos + 1).into()
    }

    /// `and ( '|' and )*`
    fn or(&mut self) -> Result<QueryNode, BoxError> {
        let mut left = self.and()?;
        while self.peek() == Some('|') {
            self.pos += 1;
            left = QueryNode::Or(Box::new(left), Box::new(self.and()?));
        }
        Ok(left)
    }

    /// `phrase ( '&' phrase )*`
    fn and(&mut self) -> Result<QueryNode, BoxError> {
        let mut left = self.phrase()?;
        while self.peek() == Some('&') {
            self.pos += 1;
            left = QueryNode::And(Box::new(left), Box::new(self.phrase()?));
        }
        Ok(left)
    }

    /// `unary ( ( '<->' | '<N>' ) unary )*`
    fn phrase(&mut self) -> Result<QueryNode, BoxError> {
        let mut left = self.unary()?;
        while self.peek() == Some('<') {
            self.pos += 1;
            let start = self.pos;
            while self.chars.get(self.pos).is_some_and(|c| *c != '>') {
                self.pos += 1;
            }
            if self.pos >= self.chars.len() {
                return Err(self.error("unterminated phrase operator"));
            }
            let inner: String = self.chars[start..self.pos].iter().collect();
            self.pos += 1;
            let distance = match inner.trim() {
                "-" => 1,
                digits => digits
                    .parse::<u16>()
                    .ok()
                    .filter(|d| *d <= MAX_DISTANCE)
                    .ok_or_else(|| self.error("invalid phrase operator distance"))?,
            };
            left = QueryNode::Phrase(Box::new(left), Box::new(self.unary()?), distance);
        }
        Ok(left)
    }

    /// `'!' unary | '(' or ')' | lexeme [ ':' flags ]`
    fn unary(&mut self) -> Result<QueryNode, BoxError> {
        match self.peek() {
            Some('!') => {
                self.pos += 1;
                Ok(QueryNode::Not(Box::new(self.unary()?)))
            }
            Some('(') => {
                self.pos += 1;
                let inner = self.or()?;
                if self.peek() != Some(')') {
                    return Err(self.error("expected `)`"));
                }
                self.pos += 1;
                Ok(inner)
            }
            Some(_) => {
                let lexeme = read_lexeme(self.chars, &mut self.pos, &['&', '|', '!', '(', ')', '<', ':'])?;
                let mut weight = 0;
                let mut prefix = false;
                if self.chars.get(self.pos) == Some(&':') {
                    self.pos += 1;
                    while let Some(flag) = self.chars.get(self.pos) {
                        match flag.to_ascii_uppercase() {
                            '*' => prefix = true,
                            'A' => weight |= 8,
                            'B' => weight |= 4,
                            'C' => weight |= 2,
                            'D' => weight |= 1,
                            _ => break,
                        }
                        self.pos += 1;
                    }
                }
                Ok(QueryNode::Value {
                    lexeme,
                    weight,
                    prefix,
                })
            }
            None => Err(self.error("expected an operand")),
        }
    }
}

/// Parse the text form of a tsquery; `None` for an empty query.
fn parse_tsquery_text(text: &str) -> Result<Option<QueryNode>, BoxError> {
    let chars: Vec<char> = text.chars().collect();
    let mut parser = QueryParser {
        chars: &chars,
        pos: 0,
    };
    if parser.peek().is_none() {
        return Ok(None);
    }
    let query = parser
        .or()
        .map_err(|e| format!("invalid tsquery {text:?}: {e}"))?;
    if parser.peek().is_some() {
        return Err(format!(
            "invalid tsquery {text:?}: unexpected character at {}",
            parser.pos + 1
        )
        .into());
    }
    Ok(Some(query))
}

/// Parse PostgreSQL tsquery binary format into its text form.
///
/// Binary format:
/// - 4 bytes: number of items (int32, big-endian)
/// - The items, in prefix order with the RIGHT operand first: an operator is
///   followed by all the items of its right operand, then those of its left.
///   - value: type 1, weight bitmask, prefix flag, null-terminated lexeme
///   - operator: type 2, operator (1 = NOT, 2 = AND, 3 = OR, 4 = PHRASE),
///     and for PHRASE a 2-byte distance
fn parse_tsquery_binary(raw: &[u8]) -> Result<String, BoxError> {
    let mut remaining = read_count(raw, "tsquery")?;
    if remaining == 0 {
        return Ok(String::new());
    }

    let mut pos = 4;
    let query = QueryNode::read(raw, &mut pos, &mut remaining)?;
    if remaining != 0 {
        return Err("tsquery binary data has items left over".into());
    }

    let mut text = String::new();
    query.render(-1, false, &mut text);
    Ok(text)
}

/// Encode the text form of a tsquery (`'fat' & ( 'cat' | rat:*A ) <-> !dog`)
/// in the binary format.
fn write_tsquery_binary(text: &str, out: &mut BytesMut) -> Result<(), BoxError> {
    match parse_tsquery_text(text)? {
        None => out.extend_from_slice(&0i32.to_be_bytes()),
        Some(query) => {
            let count = i32::try_from(query.len()).map_err(|_| "tsquery has too many items")?;
            out.extend_from_slice(&count.to_be_bytes());
            query.write(out);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query_round_trip(text: &str) -> String {
        let mut out = BytesMut::new();
        write_tsquery_binary(text, &mut out).unwrap();
        parse_tsquery_binary(&out).unwrap()
    }

    fn vector_round_trip(text: &str) -> String {
        let mut out = BytesMut::new();
        write_tsvector_binary(text, &mut out).unwrap();
        parse_tsvector_binary(&out).unwrap()
    }

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

    #[test]
    fn tsquery_items_are_prefix_with_the_right_operand_first() {
        // 'a' & 'b' as the server sends it: AND, then 'b', then 'a'.
        let raw = [
            &3i32.to_be_bytes()[..],
            &[ITEM_OPERATOR, OP_AND],
            &[ITEM_VALUE, 0, 0, b'b', 0],
            &[ITEM_VALUE, 0, 0, b'a', 0],
        ]
        .concat();
        assert_eq!(parse_tsquery_binary(&raw).unwrap(), "'a' & 'b'");

        let mut out = BytesMut::new();
        write_tsquery_binary("a & b", &mut out).unwrap();
        assert_eq!(&out[..], &raw[..]);
    }

    #[test]
    fn tsquery_text_round_trips_in_postgres_form() {
        for (input, expected) in [
            ("cat", "'cat'"),
            ("cat & dog | bird", "'cat' & 'dog' | 'bird'"),
            ("cat & (dog | bird)", "'cat' & ( 'dog' | 'bird' )"),
            ("!(cat | dog)", "!( 'cat' | 'dog' )"),
            ("!cat & dog", "!'cat' & 'dog'"),
            ("cat <-> dog <2> bird", "'cat' <-> 'dog' <2> 'bird'"),
            ("cat <-> (dog <-> bird)", "'cat' <-> ( 'dog' <-> 'bird' )"),
            ("(cat | dog) <-> bird", "( 'cat' | 'dog' ) <-> 'bird'"),
            ("sup:*", "'sup':*"),
            ("cat:ba* & dog:D", "'cat':*AB & 'dog':D"),
            ("'two words' & 'it''s'", "'two words' & 'it''s'"),
            ("", ""),
        ] {
            assert_eq!(query_round_trip(input), expected, "{input}");
            // Postgres's own output is a fixed point.
            assert_eq!(query_round_trip(expected), expected, "{expected}");
        }
    }

    #[test]
    fn malformed_tsquery_text_is_an_error() {
        for input in ["cat &", "& cat", "(cat", "cat dog", "cat <x> dog", "'open"] {
            let mut out = BytesMut::new();
            assert!(write_tsquery_binary(input, &mut out).is_err(), "{input}");
        }
    }

    #[test]
    fn tsvector_text_round_trips_sorted_and_merged() {
        for (input, expected) in [
            ("cat", "'cat'"),
            ("the quick brown fox", "'brown' 'fox' 'quick' 'the'"),
            ("fox:4 quick:7,2A brown:3B,9c", "'brown':3B,9C 'fox':4 'quick':2A,7"),
            ("b a b a:3", "'a':3 'b'"),
            ("'two words':1 'it''s':2", "'it''s':2 'two words':1"),
            ("a:99999", "'a':16383"),
            ("", ""),
        ] {
            assert_eq!(vector_round_trip(input), expected, "{input}");
            assert_eq!(vector_round_trip(expected), expected, "{expected}");
        }
    }

    #[test]
    fn malformed_tsvector_text_is_an_error() {
        for input in ["cat:", "cat:0", "cat:x", "cat:1,", "'open"] {
            let mut out = BytesMut::new();
            assert!(write_tsvector_binary(input, &mut out).is_err(), "{input}");
        }
    }
}
