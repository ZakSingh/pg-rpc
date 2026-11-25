use crate::annotations;
use anyhow::{anyhow, Result};
use pg_query::protobuf::{KeywordKind, ScanToken, Token};
use regex::Regex;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QueryType {
    /// Returns Result<T, Error> - expects exactly one row, panics if none
    One,
    /// Returns Result<Option<T>, Error> - zero or one row
    Opt,
    /// Returns Result<Vec<T>, Error> - multiple rows
    Many,
    /// Returns Result<(), Error> - execute, no return value
    Exec,
    /// Returns Result<u64, Error> - execute, returns row count
    ExecRows,
}

impl FromStr for QueryType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "one" => Ok(QueryType::One),
            "opt" => Ok(QueryType::Opt),
            "many" => Ok(QueryType::Many),
            "exec" => Ok(QueryType::Exec),
            "execrows" => Ok(QueryType::ExecRows),
            _ => Err(anyhow!("Unknown query type: {}", s)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ParameterSpec {
    /// Named parameter: :param_name (use @pgrpc_nullable annotation for nullable params)
    Named { name: String, position: usize, nullable: bool },
    /// Positional parameter: $N or $N? (nullable)
    Positional { position: usize, nullable: bool },
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub name: String,
    /// Explicitly specified query type (None if it is to be inferred)
    pub explicit_query_type: Option<QueryType>,
    /// Original SQL as written in file
    pub original_sql: String,
    /// SQL transformed for PostgreSQL ($1, $2, ...)
    pub postgres_sql: String,
    /// Parameter specifications
    pub parameters: Vec<ParameterSpec>,
    /// File path where query was found
    pub file_path: PathBuf,
    /// Line number in file
    pub line_number: usize,
    /// Column names annotated as NOT NULL with @pgrpc_not_null(col1, col2)
    pub not_null_annotations: HashSet<String>,
    /// Parameter names annotated as nullable with @pgrpc_nullable(param1, param2)
    pub nullable_param_annotations: HashSet<String>,
}

pub struct SqlParser {
    /// Regex for matching -- name: FunctionName :type
    annotation_regex: Regex,
}

/// PostgreSQL's wire protocol carries the parameter count in 16 bits.
const MAX_PARAMETERS: usize = 65535;

/// Whether `text` is usable as a `:name` parameter name.
fn is_param_name(text: &str) -> bool {
    let mut chars = text.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Whether a token can be the last token of an expression. A `:` directly
/// after one is an operator between two expressions (an array slice bound,
/// `arr[lo:hi]`, or a JSON constructor's `key : value`), not the start of a
/// `:name` parameter.
///
/// Keywords are the ambiguous case: `ORDER BY :sort` and `arr[start:n]` both
/// put an unreserved keyword in front of the colon. Inside a subscript a
/// non-reserved keyword is taken to be a column name (so the colon is a slice
/// separator); elsewhere it is taken to be syntax.
fn ends_expression(token: &ScanToken, in_subscript: bool) -> bool {
    match token.token() {
        Token::Ident
        | Token::Uident
        | Token::Sconst
        | Token::Usconst
        | Token::Iconst
        | Token::Fconst
        | Token::Bconst
        | Token::Xconst
        | Token::Param
        | Token::Ascii41
        | Token::Ascii93
        | Token::NullP
        | Token::TrueP
        | Token::FalseP
        | Token::EndP
        | Token::CurrentCatalog
        | Token::CurrentDate
        | Token::CurrentRole
        | Token::CurrentSchema
        | Token::CurrentTime
        | Token::CurrentTimestamp
        | Token::CurrentUser
        | Token::Localtime
        | Token::Localtimestamp
        | Token::SessionUser
        | Token::User => true,
        _ => {
            in_subscript
                && matches!(
                    token.keyword_kind(),
                    KeywordKind::UnreservedKeyword
                        | KeywordKind::ColNameKeyword
                        | KeywordKind::TypeFuncNameKeyword
                )
        }
    }
}

impl SqlParser {
    pub fn new() -> Self {
        Self {
            // Query type is now optional - (?::([a-z]+))? makes the :type part optional
            annotation_regex: Regex::new(r"^--\s*name:\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(?::([a-z]+))?\s*$")
                .unwrap(),
        }
    }

    /// Parse all SQL files matching the given glob patterns
    pub fn parse_files(&self, patterns: &[String]) -> Result<Vec<ParsedQuery>> {
        use rayon::prelude::*;

        // Collect all file paths first
        let mut paths = Vec::new();
        for pattern in patterns {
            let glob_paths = glob::glob(pattern)
                .map_err(|e| anyhow!("Invalid glob pattern '{}': {}", pattern, e))?;
            for path_result in glob_paths {
                let path = path_result
                    .map_err(|e| anyhow!("Error reading path from glob: {}", e))?;
                if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                    paths.push(path);
                }
            }
        }

        // Parse files in parallel
        let results: Vec<Result<Vec<ParsedQuery>>> = paths
            .par_iter()
            .map(|path| self.parse_file(path))
            .collect();

        let mut all_queries = Vec::new();
        for result in results {
            all_queries.extend(result?);
        }

        Ok(all_queries)
    }

    /// Parse a single SQL file
    pub fn parse_file(&self, path: &Path) -> Result<Vec<ParsedQuery>> {
        let content = fs::read_to_string(path)
            .map_err(|e| anyhow!("Failed to read file {:?}: {}", path, e))?;

        self.parse_content(&content, path.to_path_buf())
    }

    /// Parse SQL content from a string
    pub fn parse_content(&self, content: &str, file_path: PathBuf) -> Result<Vec<ParsedQuery>> {
        let mut queries = Vec::new();
        let lines: Vec<&str> = content.lines().collect();
        let mut i = 0;

        while i < lines.len() {
            let line = lines[i].trim();

            // Check if this line is an annotation
            if let Some(captures) = self.annotation_regex.captures(line) {
                let name = captures.get(1).unwrap().as_str().to_string();

                // Query type is now optional - if not specified, it will be inferred later
                let explicit_query_type = if let Some(type_match) = captures.get(2) {
                    Some(QueryType::from_str(type_match.as_str())
                        .map_err(|e| anyhow!("Error at {}:{}: {}", file_path.display(), i + 1, e))?)
                } else {
                    None
                };

                let line_number = i + 1;

                // Collect comment lines (for @pgrpc annotations) and SQL query lines
                i += 1;
                let mut sql_lines = Vec::new();
                let mut comment_lines = Vec::new();

                while i < lines.len() {
                    let sql_line = lines[i];
                    let trimmed = sql_line.trim();

                    // Stop if we hit another name annotation
                    if self.annotation_regex.is_match(trimmed) {
                        break;
                    }

                    // Collect comment lines that might contain annotations
                    if trimmed.starts_with("--") {
                        comment_lines.push(trimmed);
                    }

                    sql_lines.push(sql_line);
                    i += 1;
                }

                let original_sql = sql_lines.join("\n").trim().to_string();

                if original_sql.is_empty() {
                    return Err(anyhow!(
                        "Empty query for '{}' at {}:{}",
                        name,
                        file_path.display(),
                        line_number
                    ));
                }

                // Parse @pgrpc_not_null and @pgrpc_nullable annotations from comments
                let (not_null_annotations, nullable_param_annotations) = self
                    .parse_annotations(&comment_lines)
                    .map_err(|e| {
                        anyhow!("Query '{}' at {}:{}: {}", name, file_path.display(), line_number, e)
                    })?;

                // Transform parameters and extract specifications
                let (postgres_sql, parameters) = self
                    .transform_parameters(&original_sql, &nullable_param_annotations)
                    .map_err(|e| {
                        anyhow!(
                            "Query '{}' at {}:{}: {}",
                            name,
                            file_path.display(),
                            line_number,
                            e
                        )
                    })?;

                queries.push(ParsedQuery {
                    name,
                    explicit_query_type,
                    original_sql,
                    postgres_sql,
                    parameters,
                    file_path: file_path.clone(),
                    line_number,
                    not_null_annotations,
                    nullable_param_annotations,
                });
            } else {
                i += 1;
            }
        }

        Ok(queries)
    }

    /// Parse the `@pgrpc_not_null(cols)` and `@pgrpc_nullable(params)`
    /// annotations in a query's comment lines. Anything else tagged `@pgrpc_`
    /// is an error.
    fn parse_annotations(
        &self,
        comment_lines: &[&str],
    ) -> Result<(HashSet<String>, HashSet<String>)> {
        let comment = comment_lines.join("\n");
        let (parsed, mut problems) = annotations::parse(&comment);
        problems.extend(parsed.misplaced(annotations::Site::Query));
        if !problems.is_empty() {
            return Err(anyhow!("{}", problems.join("; ")));
        }
        Ok((
            parsed.not_null.into_iter().collect(),
            parsed.nullable.into_iter().collect(),
        ))
    }

    /// Transform named parameters (:param) to positional ($N) and extract parameter specs.
    /// Parameters in `nullable_annotations` will be marked as nullable.
    ///
    /// Works on the token stream rather than the raw text, so that only real
    /// parameters are rewritten: a `:word` or `$N` inside a string literal
    /// (`'HH24:MI:SS'`), a quoted identifier, a dollar-quoted body or a comment
    /// is left alone, `::` is a cast, and `arr[1:n]` is a slice.
    ///
    /// Named parameters are numbered in order of first appearance. `$N` and
    /// `$N?` (nullable) are kept as written; a query may mix the two styles
    /// only if the explicit numbers don't land on a number a named parameter
    /// was given.
    fn transform_parameters(
        &self,
        sql: &str,
        nullable_annotations: &HashSet<String>,
    ) -> Result<(String, Vec<ParameterSpec>)> {
        let scan = pg_query::scan(sql).map_err(|e| anyhow!("failed to tokenize SQL: {}", e))?;
        // Comments carry no meaning for what precedes or follows a token.
        let tokens: Vec<&ScanToken> = scan
            .tokens
            .iter()
            .filter(|t| !matches!(t.token(), Token::SqlComment | Token::CComment))
            .collect();
        let text = |t: &ScanToken| &sql[t.start as usize..t.end as usize];

        // Named parameters in order of first appearance; position = index + 1.
        let mut named: Vec<String> = Vec::new();
        // Explicit `$N` parameters -> whether any occurrence was `$N?`.
        let mut positional: BTreeMap<usize, bool> = BTreeMap::new();

        let mut out = String::with_capacity(sql.len());
        let mut copied = 0usize; // bytes of `sql` already written to `out`
        let mut open: Vec<Token> = Vec::new(); // unclosed `(` / `[`

        let mut i = 0;
        while i < tokens.len() {
            let token = tokens[i];
            let next = tokens
                .get(i + 1)
                .filter(|next| next.start == token.end)
                .copied();

            match token.token() {
                Token::Ascii40 | Token::Ascii91 => open.push(token.token()),
                Token::Ascii41 | Token::Ascii93 => {
                    open.pop();
                }
                Token::Ascii58 => {
                    let in_subscript = open.last() == Some(&Token::Ascii91);
                    let after_expression = i
                        .checked_sub(1)
                        .is_some_and(|prev| ends_expression(tokens[prev], in_subscript));
                    if let Some(name_token) = next.filter(|n| is_param_name(text(n))) {
                        if !after_expression {
                            let name = text(name_token);
                            let position = match named.iter().position(|n| n == name) {
                                Some(index) => index + 1,
                                None => {
                                    named.push(name.to_string());
                                    named.len()
                                }
                            };
                            out.push_str(&sql[copied..token.start as usize]);
                            out.push_str(&format!("${}", position));
                            copied = name_token.end as usize;
                            i += 2;
                            continue;
                        }
                    }
                }
                Token::Param => {
                    let written = text(token);
                    let number = written[1..]
                        .parse::<usize>()
                        .ok()
                        .filter(|n| (1..=MAX_PARAMETERS).contains(n))
                        .ok_or_else(|| {
                            anyhow!(
                                "invalid parameter '{}': parameter numbers run from $1 to ${}",
                                written,
                                MAX_PARAMETERS
                            )
                        })?;
                    // `$N?` marks the parameter nullable; the `?` is ours, not SQL.
                    let marker = next.filter(|n| n.token() == Token::Op && text(n) == "?");
                    *positional.entry(number).or_insert(false) |= marker.is_some();
                    if let Some(marker) = marker {
                        out.push_str(&sql[copied..marker.start as usize]);
                        copied = marker.end as usize;
                        i += 2;
                        continue;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        out.push_str(&sql[copied..]);

        // A named parameter was numbered without regard to the explicit `$N`s.
        // If one of those has the same number, the two would silently become
        // one parameter.
        if let Some((&number, _)) = positional.iter().find(|(&n, _)| n <= named.len()) {
            return Err(anyhow!(
                "positional parameter ${} collides with named parameter :{}, which is also \
                 numbered ${}. Use only named or only positional parameters, or number the \
                 positional ones after the {} named one(s)",
                number,
                named[number - 1],
                number,
                named.len()
            ));
        }

        if let Some(unknown) = nullable_annotations.iter().find(|n| !named.contains(n)) {
            return Err(anyhow!(
                "@pgrpc_nullable names `{}`, which is not a parameter of the query",
                unknown
            ));
        }

        let count = positional
            .keys()
            .next_back()
            .copied()
            .unwrap_or(0)
            .max(named.len());
        let parameters = (1..=count)
            .map(|position| match named.get(position - 1) {
                Some(name) => ParameterSpec::Named {
                    name: name.clone(),
                    position,
                    nullable: nullable_annotations.contains(name),
                },
                // A number that never appears still occupies its position.
                None => ParameterSpec::Positional {
                    position,
                    nullable: positional.get(&position).copied().unwrap_or(false),
                },
            })
            .collect();

        Ok((out, parameters))
    }
}

impl Default for SqlParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_annotation() {
        let parser = SqlParser::new();

        let content = r#"
-- name: GetAuthor :one
SELECT * FROM authors WHERE id = :author_id;

-- name: ListAuthors :many
SELECT * FROM authors ORDER BY name;

-- name: DeleteAuthor :exec
DELETE FROM authors WHERE id = :author_id;
        "#;

        let queries = parser.parse_content(content, PathBuf::from("test.sql")).unwrap();
        assert_eq!(queries.len(), 3);

        assert_eq!(queries[0].name, "GetAuthor");
        assert_eq!(queries[0].explicit_query_type, Some(QueryType::One));
        assert_eq!(queries[1].name, "ListAuthors");
        assert_eq!(queries[1].explicit_query_type, Some(QueryType::Many));
        assert_eq!(queries[2].name, "DeleteAuthor");
        assert_eq!(queries[2].explicit_query_type, Some(QueryType::Exec));
    }

    #[test]
    fn test_named_parameters() {
        let parser = SqlParser::new();

        let sql = "SELECT * FROM users WHERE id = :user_id AND status = :status";
        let (transformed, params) = parser.transform_parameters(sql, &HashSet::new()).unwrap();

        assert_eq!(transformed, "SELECT * FROM users WHERE id = $1 AND status = $2");
        assert_eq!(params.len(), 2);

        match &params[0] {
            ParameterSpec::Named { name, position, nullable } => {
                assert_eq!(name, "user_id");
                assert_eq!(*position, 1);
                assert!(!*nullable);
            }
            _ => panic!("Expected named parameter"),
        }

        match &params[1] {
            ParameterSpec::Named { name, position, nullable } => {
                assert_eq!(name, "status");
                assert_eq!(*position, 2);
                assert!(!*nullable);
            }
            _ => panic!("Expected named parameter"),
        }
    }

    #[test]
    fn test_named_parameter_with_array_type_cast() {
        let parser = SqlParser::new();

        // Regression: `:name::type[]` used to lose the `::type` portion because the
        // typecast placeholder was made of word characters and the param-name regex
        // greedily extended across it.
        let cases = [
            (
                "select unnest(:xs::bigint[]);",
                "select unnest($1::bigint[]);",
            ),
            (
                "select (:sales_taxes::currency[])[ord];",
                "select ($1::currency[])[ord];",
            ),
            (
                "select unnest((:xs)::bigint[]);",
                "select unnest(($1)::bigint[]);",
            ),
            (
                "select :a::int + :b::text;",
                "select $1::int + $2::text;",
            ),
        ];

        for (input, expected) in cases {
            let (transformed, _) = parser
                .transform_parameters(input, &HashSet::new())
                .unwrap();
            assert_eq!(transformed, expected, "input: {input}");
        }
    }

    #[test]
    fn test_multiline_query() {
        let parser = SqlParser::new();

        let content = r#"
-- name: GetUserWithPosts :many
SELECT u.id, u.name, p.title
FROM users u
LEFT JOIN posts p ON u.id = p.user_id
WHERE u.id = :user_id;

-- name: CreateUser :one
INSERT INTO users (name, email)
VALUES (:name, :email)
RETURNING *;
        "#;

        let queries = parser.parse_content(content, PathBuf::from("test.sql")).unwrap();
        assert_eq!(queries.len(), 2);

        assert!(queries[0].original_sql.contains("LEFT JOIN"));
        assert!(queries[1].original_sql.contains("RETURNING"));
    }

    #[test]
    fn test_positional_parameters() {
        let parser = SqlParser::new();

        let sql = "SELECT * FROM users WHERE id = $1 AND status = $2";
        let (transformed, params) = parser.transform_parameters(sql, &HashSet::new()).unwrap();

        // Should remain unchanged
        assert_eq!(transformed, sql);
        // Parameters detected
        assert_eq!(params.len(), 2);

        for (i, param) in params.iter().enumerate() {
            match param {
                ParameterSpec::Positional { position, nullable } => {
                    assert_eq!(*position, i + 1);
                    assert!(!*nullable);
                }
                _ => panic!("Expected positional parameter"),
            }
        }
    }

    #[test]
    fn test_mixed_parameters() {
        let parser = SqlParser::new();

        // Named parameter gets transformed first
        let sql = "SELECT * FROM users WHERE id = :user_id AND created > $2";
        let (transformed, params) = parser.transform_parameters(sql, &HashSet::new()).unwrap();

        assert_eq!(transformed, "SELECT * FROM users WHERE id = $1 AND created > $2");
        assert_eq!(params.len(), 2);

        match &params[0] {
            ParameterSpec::Named { name, position, nullable } => {
                assert_eq!(name, "user_id");
                assert_eq!(*position, 1);
                assert!(!*nullable);
            }
            _ => panic!("Expected named parameter"),
        }
    }

    #[test]
    fn test_nullable_named_parameters_via_annotation() {
        let parser = SqlParser::new();

        // Test that @pgrpc_nullable annotation marks parameters as nullable
        let sql = "SELECT * FROM users WHERE id = :user_id AND email = :email";
        let mut nullable_annotations = HashSet::new();
        nullable_annotations.insert("email".to_string());
        let (transformed, params) = parser.transform_parameters(sql, &nullable_annotations).unwrap();

        assert_eq!(transformed, "SELECT * FROM users WHERE id = $1 AND email = $2");
        assert_eq!(params.len(), 2);

        match &params[0] {
            ParameterSpec::Named { name, position, nullable } => {
                assert_eq!(name, "user_id");
                assert_eq!(*position, 1);
                assert!(!*nullable);
            }
            _ => panic!("Expected named parameter"),
        }

        match &params[1] {
            ParameterSpec::Named { name, position, nullable } => {
                assert_eq!(name, "email");
                assert_eq!(*position, 2);
                assert!(*nullable);
            }
            _ => panic!("Expected named parameter"),
        }
    }

    #[test]
    fn test_nullable_positional_parameters() {
        let parser = SqlParser::new();

        let sql = "SELECT * FROM users WHERE id = $1? AND status = $2";
        let (transformed, params) = parser.transform_parameters(sql, &HashSet::new()).unwrap();

        assert_eq!(transformed, "SELECT * FROM users WHERE id = $1 AND status = $2");
        assert_eq!(params.len(), 2);

        match &params[0] {
            ParameterSpec::Positional { position, nullable } => {
                assert_eq!(*position, 1);
                assert!(*nullable);
            }
            _ => panic!("Expected positional parameter"),
        }

        match &params[1] {
            ParameterSpec::Positional { position, nullable } => {
                assert_eq!(*position, 2);
                assert!(!*nullable);
            }
            _ => panic!("Expected positional parameter"),
        }
    }

    #[test]
    fn test_mixed_nullable_parameters_via_annotation() {
        let parser = SqlParser::new();

        // Test mixing nullable named parameter (via annotation) with positional parameter
        let sql = "SELECT * FROM users WHERE id = :user_id AND created > $2";
        let mut nullable_annotations = HashSet::new();
        nullable_annotations.insert("user_id".to_string());
        let (transformed, params) = parser.transform_parameters(sql, &nullable_annotations).unwrap();

        assert_eq!(transformed, "SELECT * FROM users WHERE id = $1 AND created > $2");
        assert_eq!(params.len(), 2);

        match &params[0] {
            ParameterSpec::Named { name, position, nullable } => {
                assert_eq!(name, "user_id");
                assert_eq!(*position, 1);
                assert!(*nullable);
            }
            _ => panic!("Expected named parameter"),
        }

        match &params[1] {
            ParameterSpec::Positional { position, nullable } => {
                assert_eq!(*position, 2);
                assert!(!*nullable);
            }
            _ => panic!("Expected positional parameter"),
        }
    }

    #[test]
    fn test_opt_query_type() {
        let parser = SqlParser::new();

        let content = r#"
-- name: FindUserByEmail :opt
SELECT * FROM users WHERE email = :email;

-- name: GetUserRequired :one
SELECT * FROM users WHERE id = :id;
        "#;

        let queries = parser.parse_content(content, PathBuf::from("test.sql")).unwrap();
        assert_eq!(queries.len(), 2);

        assert_eq!(queries[0].name, "FindUserByEmail");
        assert_eq!(queries[0].explicit_query_type, Some(QueryType::Opt));
        assert_eq!(queries[1].name, "GetUserRequired");
        assert_eq!(queries[1].explicit_query_type, Some(QueryType::One));
    }

    #[test]
    fn test_pgrpc_annotations() {
        let parser = SqlParser::new();

        let content = r#"
-- name: CreateUser :one
-- @pgrpc_not_null(id)
-- @pgrpc_not_null(created_at)
INSERT INTO users (email, name) VALUES (:email, :name) RETURNING *;

-- name: UpdateUser :exec
UPDATE users SET name = :name WHERE id = :id;
        "#;

        let queries = parser.parse_content(content, PathBuf::from("test.sql")).unwrap();
        assert_eq!(queries.len(), 2);

        // Check first query annotations
        assert_eq!(queries[0].name, "CreateUser");
        assert!(queries[0].not_null_annotations.contains("id"));
        assert!(queries[0].not_null_annotations.contains("created_at"));
        assert_eq!(queries[0].not_null_annotations.len(), 2);

        // Check second query annotations
        assert_eq!(queries[1].name, "UpdateUser");
        assert!(queries[1].not_null_annotations.is_empty());
    }

    #[test]
    fn bad_query_annotations_are_errors() {
        let parser = SqlParser::new();
        for (annotation, expected) in [
            ("@pgrpc_throws 23505", "has been removed"),
            ("@pgrpc_not_nul(id)", "unknown annotation"),
            ("@pgrpc_not_null", "does not apply to a query"),
            ("@pgrpc_nullable(nope)", "not a parameter"),
        ] {
            let content = format!("-- name: GetUser :one\n-- {annotation}\nSELECT * FROM users WHERE id = :id;");
            let err = parser
                .parse_content(&content, PathBuf::from("test.sql"))
                .unwrap_err()
                .to_string();
            assert!(err.contains(expected), "{annotation}: {err}");
            assert!(err.contains("GetUser"), "{err}");
        }
    }

    #[test]
    fn test_pgrpc_nullable_annotation() {
        let parser = SqlParser::new();

        let content = r#"
-- name: FindUsers :many
-- @pgrpc_nullable(name, email)
SELECT * FROM users WHERE name = :name OR email = :email;

-- name: FindUsersByStatus :many
-- @pgrpc_nullable(status)
SELECT * FROM users WHERE status = :status AND active = :active;
        "#;

        let queries = parser.parse_content(content, PathBuf::from("test.sql")).unwrap();
        assert_eq!(queries.len(), 2);

        // Check first query - both name and email should be nullable
        assert_eq!(queries[0].name, "FindUsers");
        assert!(queries[0].nullable_param_annotations.contains("name"));
        assert!(queries[0].nullable_param_annotations.contains("email"));
        assert_eq!(queries[0].nullable_param_annotations.len(), 2);
        assert_eq!(queries[0].parameters.len(), 2);

        // Verify parameters are marked as nullable
        for param in &queries[0].parameters {
            match param {
                ParameterSpec::Named { name, nullable, .. } => {
                    assert!(
                        *nullable,
                        "Parameter '{}' should be nullable",
                        name
                    );
                }
                _ => panic!("Expected named parameter"),
            }
        }

        // Check second query - status is nullable, active is not
        assert_eq!(queries[1].name, "FindUsersByStatus");
        assert!(queries[1].nullable_param_annotations.contains("status"));
        assert!(!queries[1].nullable_param_annotations.contains("active"));
        assert_eq!(queries[1].nullable_param_annotations.len(), 1);
        assert_eq!(queries[1].parameters.len(), 2);

        // Verify status is nullable, active is not
        for param in &queries[1].parameters {
            match param {
                ParameterSpec::Named { name, nullable, .. } => {
                    if name == "status" {
                        assert!(*nullable, "status should be nullable");
                    } else if name == "active" {
                        assert!(!*nullable, "active should not be nullable");
                    }
                }
                _ => panic!("Expected named parameter"),
            }
        }
    }

    #[test]
    fn test_optional_query_type() {
        let parser = SqlParser::new();

        let content = r#"
-- name: InferredQuery
SELECT * FROM users WHERE id = :id;

-- name: ExplicitOneQuery :one
SELECT * FROM users WHERE id = :id;

-- name: InferredInsert
INSERT INTO users (name) VALUES (:name) RETURNING *;
        "#;

        let queries = parser.parse_content(content, PathBuf::from("test.sql")).unwrap();
        assert_eq!(queries.len(), 3);

        // First query - no explicit type, should have None for explicit_query_type
        assert_eq!(queries[0].name, "InferredQuery");
        assert!(queries[0].explicit_query_type.is_none());

        // Second query - explicit :one type
        assert_eq!(queries[1].name, "ExplicitOneQuery");
        assert_eq!(queries[1].explicit_query_type, Some(QueryType::One));

        // Third query - no explicit type
        assert_eq!(queries[2].name, "InferredInsert");
        assert!(queries[2].explicit_query_type.is_none());
    }

    fn transform(sql: &str) -> (String, Vec<String>) {
        let (transformed, params) = SqlParser::new()
            .transform_parameters(sql, &HashSet::new())
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        let names = params
            .iter()
            .map(|p| match p {
                ParameterSpec::Named { name, .. } => name.clone(),
                ParameterSpec::Positional { position, .. } => format!("${position}"),
            })
            .collect();
        (transformed, names)
    }

    #[test]
    fn test_colon_inside_string_literal_is_not_a_parameter() {
        let sql = "SELECT to_char(ts, 'HH24:MI:SS'), E'a:b', $$ :c $1 $$, $tag$ :d $tag$ FROM t WHERE id = :id";
        let (transformed, names) = transform(sql);
        assert_eq!(
            transformed,
            "SELECT to_char(ts, 'HH24:MI:SS'), E'a:b', $$ :c $1 $$, $tag$ :d $tag$ FROM t WHERE id = $1"
        );
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_positional_inside_string_literal_is_not_a_parameter() {
        let (transformed, names) = transform("SELECT 'costs $5', '$2?' WHERE a = $1");
        assert_eq!(transformed, "SELECT 'costs $5', '$2?' WHERE a = $1");
        assert_eq!(names, vec!["$1"]);
    }

    #[test]
    fn test_colon_inside_quoted_identifier_is_not_a_parameter() {
        let (transformed, names) = transform(r#"SELECT "a:b" FROM t WHERE "x:y" = :val"#);
        assert_eq!(transformed, r#"SELECT "a:b" FROM t WHERE "x:y" = $1"#);
        assert_eq!(names, vec!["val"]);
    }

    #[test]
    fn test_colon_inside_comment_is_not_a_parameter() {
        let sql = "SELECT a -- pick :one of $3\n, b /* or :two */ FROM t WHERE id = :id";
        let (transformed, names) = transform(sql);
        assert_eq!(
            transformed,
            "SELECT a -- pick :one of $3\n, b /* or :two */ FROM t WHERE id = $1"
        );
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_array_slice_is_not_a_parameter() {
        // A colon directly after an expression is a slice separator.
        for sql in [
            "SELECT arr[1:n] FROM t",
            "SELECT arr[lo:hi] FROM t",
            "SELECT arr[1 :n] FROM t",
            "SELECT (arr)[f(x):n] FROM t",
            "SELECT arr[a[1]:n] FROM t",
            "SELECT arr[$1:n] FROM t",
            // `start` is a keyword, but in a subscript it is a column.
            "SELECT arr[start:n] FROM t",
        ] {
            let (transformed, names) = transform(sql);
            assert_eq!(transformed, sql);
            let expected: Vec<String> = if sql.contains("$1") { vec!["$1".into()] } else { vec![] };
            assert_eq!(names, expected, "{sql}");
        }

        // Parameters as slice bounds: after `[`, after the separator, or parenthesized.
        let (transformed, names) = transform("SELECT arr[:lo::int : :hi], arr[1:(:n)], arr[1: :m] FROM t");
        assert_eq!(transformed, "SELECT arr[$1::int : $2], arr[1:($3)], arr[1: $4] FROM t");
        assert_eq!(names, vec!["lo", "hi", "n", "m"]);
    }

    #[test]
    fn test_named_parameter_contexts() {
        let sql = "SELECT :a, f(:b), -:c, x = :d, y IN (:e, :f) FROM t \
                   WHERE z BETWEEN :g AND :h AND w LIKE :i ORDER BY :j LIMIT :k OFFSET :a";
        let (transformed, names) = transform(sql);
        assert_eq!(
            transformed,
            "SELECT $1, f($2), -$3, x = $4, y IN ($5, $6) FROM t \
             WHERE z BETWEEN $7 AND $8 AND w LIKE $9 ORDER BY $10 LIMIT $11 OFFSET $1"
        );
        assert_eq!(names.len(), 11);
    }

    #[test]
    fn test_keyword_named_parameters() {
        // Parameter names that happen to be SQL keywords.
        let (transformed, names) =
            transform("SELECT * FROM t WHERE name = :name AND type = :type LIMIT :limit");
        assert_eq!(transformed, "SELECT * FROM t WHERE name = $1 AND type = $2 LIMIT $3");
        assert_eq!(names, vec!["name", "type", "limit"]);
    }

    #[test]
    fn test_colon_equals_and_json_key_separator() {
        let (transformed, names) = transform("SELECT f(a := :x), json_object('k' : :v, k2 : v2)");
        assert_eq!(transformed, "SELECT f(a := $1), json_object('k' : $2, k2 : v2)");
        assert_eq!(names, vec!["x", "v"]);
    }

    #[test]
    fn test_oversized_positional_parameter_is_an_error() {
        let parser = SqlParser::new();
        for sql in [
            "SELECT $99999999999999999999999",
            "SELECT $65536",
            "SELECT $0",
        ] {
            let err = parser
                .transform_parameters(sql, &HashSet::new())
                .expect_err(sql);
            assert!(err.to_string().contains("invalid parameter"), "{err}");
        }
    }

    #[test]
    fn test_named_and_positional_collision_is_an_error() {
        let parser = SqlParser::new();
        // `:id` is numbered $1, the same as the explicit $1.
        let err = parser
            .transform_parameters("SELECT * FROM t WHERE a = $1 AND b = :id", &HashSet::new())
            .unwrap_err();
        assert!(err.to_string().contains("collides"), "{err}");
        assert!(err.to_string().contains(":id"), "{err}");

        // Reported with the query's name and location.
        let content = "-- name: Clash :many\nSELECT * FROM t WHERE a = :x AND b = :y AND c = $2;\n";
        let err = parser
            .parse_content(content, PathBuf::from("clash.sql"))
            .unwrap_err();
        assert!(err.to_string().contains("Clash"), "{err}");
        assert!(err.to_string().contains("clash.sql:1"), "{err}");
    }

    #[test]
    fn test_positional_gap_keeps_positions() {
        let (_, names) = transform("SELECT $3::int, $1?::text");
        assert_eq!(names, vec!["$1", "$2", "$3"]);

        let (_, params) = SqlParser::new()
            .transform_parameters("SELECT $3::int, $1?::text, $1", &HashSet::new())
            .unwrap();
        let nullable: Vec<bool> = params
            .iter()
            .map(|p| match p {
                ParameterSpec::Positional { nullable, .. } => *nullable,
                ParameterSpec::Named { nullable, .. } => *nullable,
            })
            .collect();
        assert_eq!(nullable, vec![true, false, false]);
    }

    #[test]
    fn test_query_type_from_str() {
        assert_eq!("one".parse::<QueryType>().unwrap(), QueryType::One);
        assert_eq!("execrows".parse::<QueryType>().unwrap(), QueryType::ExecRows);
        assert!("several".parse::<QueryType>().is_err());
    }
}
