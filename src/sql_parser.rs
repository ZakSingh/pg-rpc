use anyhow::{anyhow, Result};
use regex::Regex;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

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

impl QueryType {
    pub fn from_str(s: &str) -> Result<Self> {
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
    pub query_type: QueryType,
    /// Explicitly specified query type (None if inferred)
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
    /// SQL state codes that this query can throw via @pgrpc_throws XXXXX
    pub throws_annotations: Vec<String>,
    /// Parameter names annotated as nullable with @pgrpc_nullable(param1, param2)
    pub nullable_param_annotations: HashSet<String>,
}

pub struct SqlParser {
    /// Regex for matching -- name: FunctionName :type
    annotation_regex: Regex,
    /// Regex for matching named parameters :param_name
    named_param_regex: Regex,
    /// Regex for matching @pgrpc_not_null(col1, col2, ...)
    not_null_annotation_regex: Regex,
    /// Regex for matching @pgrpc_throws XXXXX
    throws_annotation_regex: Regex,
    /// Regex for matching @pgrpc_nullable(param1, param2, ...)
    nullable_param_annotation_regex: Regex,
}

impl SqlParser {
    pub fn new() -> Self {
        Self {
            // Query type is now optional - (?::([a-z]+))? makes the :type part optional
            annotation_regex: Regex::new(r"^--\s*name:\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(?::([a-z]+))?\s*$")
                .unwrap(),
            named_param_regex: Regex::new(r":([a-zA-Z_][a-zA-Z0-9_]*)").unwrap(),
            not_null_annotation_regex: Regex::new(r"@pgrpc_not_null\(([^)]+)\)").unwrap(),
            throws_annotation_regex: Regex::new(r"@pgrpc_throws\s+([A-Za-z0-9]{5})").unwrap(),
            nullable_param_annotation_regex: Regex::new(r"@pgrpc_nullable\(([^)]+)\)").unwrap(),
        }
    }

    /// Parse all SQL files matching the given glob patterns
    pub fn parse_files(&self, patterns: &[String]) -> Result<Vec<ParsedQuery>> {
        let mut all_queries = Vec::new();

        for pattern in patterns {
            let paths = glob::glob(pattern)
                .map_err(|e| anyhow!("Invalid glob pattern '{}': {}", pattern, e))?;

            for path_result in paths {
                let path = path_result
                    .map_err(|e| anyhow!("Error reading path from glob: {}", e))?;

                if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                    let queries = self.parse_file(&path)?;
                    all_queries.extend(queries);
                }
            }
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

                // Use explicit type or placeholder (Many) - will be replaced during introspection
                let query_type = explicit_query_type.clone().unwrap_or(QueryType::Many);

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

                // Parse @pgrpc_not_null, @pgrpc_throws, and @pgrpc_nullable annotations from comments
                let (not_null_annotations, throws_annotations, nullable_param_annotations) =
                    self.parse_annotations(&comment_lines);

                // Transform parameters and extract specifications
                let (postgres_sql, parameters) =
                    self.transform_parameters(&original_sql, &nullable_param_annotations)?;

                queries.push(ParsedQuery {
                    name,
                    query_type,
                    explicit_query_type,
                    original_sql,
                    postgres_sql,
                    parameters,
                    file_path: file_path.clone(),
                    line_number,
                    not_null_annotations,
                    throws_annotations,
                    nullable_param_annotations,
                });
            } else {
                i += 1;
            }
        }

        Ok(queries)
    }

    /// Parse @pgrpc_not_null, @pgrpc_throws, and @pgrpc_nullable annotations from comment lines
    fn parse_annotations(
        &self,
        comment_lines: &[&str],
    ) -> (HashSet<String>, Vec<String>, HashSet<String>) {
        let mut not_null_annotations = HashSet::new();
        let mut throws_annotations = Vec::new();
        let mut nullable_param_annotations = HashSet::new();

        for line in comment_lines {
            // Parse @pgrpc_not_null(col1, col2, ...)
            for cap in self.not_null_annotation_regex.captures_iter(line) {
                if let Some(cols) = cap.get(1) {
                    for col in cols.as_str().split(',') {
                        let col = col.trim();
                        if !col.is_empty() {
                            not_null_annotations.insert(col.to_string());
                        }
                    }
                }
            }

            // Parse @pgrpc_throws XXXXX
            for cap in self.throws_annotation_regex.captures_iter(line) {
                if let Some(code) = cap.get(1) {
                    throws_annotations.push(code.as_str().to_string());
                }
            }

            // Parse @pgrpc_nullable(param1, param2, ...)
            for cap in self.nullable_param_annotation_regex.captures_iter(line) {
                if let Some(params) = cap.get(1) {
                    for param in params.as_str().split(',') {
                        let param = param.trim();
                        if !param.is_empty() {
                            nullable_param_annotations.insert(param.to_string());
                        }
                    }
                }
            }
        }

        (not_null_annotations, throws_annotations, nullable_param_annotations)
    }

    /// Transform named parameters (:param) to positional ($N) and extract parameter specs.
    /// Parameters in `nullable_annotations` will be marked as nullable.
    fn transform_parameters(
        &self,
        sql: &str,
        nullable_annotations: &HashSet<String>,
    ) -> Result<(String, Vec<ParameterSpec>)> {
        let mut position = 1;
        let mut parameters = Vec::new();
        let mut seen_params: std::collections::HashMap<String, (usize, bool)> =
            std::collections::HashMap::new();

        // Temporarily replace :: (type cast operator) with a placeholder to avoid matching it
        const TYPECAST_PLACEHOLDER: &str = "___PGRPC_TYPECAST___";
        let sql_with_placeholder = sql.replace("::", TYPECAST_PLACEHOLDER);

        // Process named parameters (:param_name)
        let sql_after_named = self.named_param_regex.replace_all(&sql_with_placeholder, |caps: &regex::Captures| {
            let param_name = caps[1].to_string();

            // Check if we've seen this parameter name before
            if let Some(&(existing_pos, _)) = seen_params.get(&param_name) {
                // Reuse the same position for duplicate parameter names
                format!("${}", existing_pos)
            } else {
                // Check if this parameter is marked nullable via @pgrpc_nullable annotation
                let is_nullable = nullable_annotations.contains(&param_name);

                // New parameter, assign a position
                seen_params.insert(param_name.clone(), (position, is_nullable));
                parameters.push(ParameterSpec::Named {
                    name: param_name,
                    position,
                    nullable: is_nullable,
                });

                let replacement = format!("${}", position);
                position += 1;
                replacement
            }
        }).to_string();

        // Check for any existing $N or $N? parameters in the ORIGINAL SQL
        let positional_regex = Regex::new(r"\$(\d+)(\?)?").unwrap();
        let mut positional_params: std::collections::HashMap<usize, bool> = std::collections::HashMap::new();

        for cap in positional_regex.captures_iter(sql) {
            let num: usize = cap[1].parse().unwrap();
            let nullable = cap.get(2).is_some();
            positional_params.insert(num, nullable);
        }

        // Transform $N? to $N in the SQL
        let sql_after_positional = Regex::new(r"\$(\d+)\?").unwrap().replace_all(&sql_after_named, "$$1").to_string();

        // Restore :: type cast operator
        let final_sql = sql_after_positional.replace(TYPECAST_PLACEHOLDER, "::");

        // Add positional parameters that weren't already handled as named parameters
        for (&num, &nullable) in &positional_params {
            // Check if this position is already covered by a named parameter
            if !parameters.iter().any(|p| matches!(p, ParameterSpec::Named { position: pos, .. } if *pos == num)) {
                // This is a bare positional parameter, add it
                while parameters.len() < num {
                    let pos = parameters.len() + 1;
                    if !parameters.iter().any(|p| match p {
                        ParameterSpec::Named { position, .. } => *position == pos,
                        ParameterSpec::Positional { position, .. } => *position == pos,
                    }) {
                        // Use the nullable flag if we have it, otherwise default to false
                        let is_nullable = positional_params.get(&pos).copied().unwrap_or(false);
                        parameters.push(ParameterSpec::Positional {
                            position: pos,
                            nullable: is_nullable,
                        });
                    }
                }
            }
        }

        Ok((final_sql, parameters))
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
        assert_eq!(queries[0].query_type, QueryType::One);
        assert_eq!(queries[1].name, "ListAuthors");
        assert_eq!(queries[1].query_type, QueryType::Many);
        assert_eq!(queries[2].name, "DeleteAuthor");
        assert_eq!(queries[2].query_type, QueryType::Exec);
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
                assert_eq!(*nullable, false);
            }
            _ => panic!("Expected named parameter"),
        }

        match &params[1] {
            ParameterSpec::Named { name, position, nullable } => {
                assert_eq!(name, "status");
                assert_eq!(*position, 2);
                assert_eq!(*nullable, false);
            }
            _ => panic!("Expected named parameter"),
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
                    assert_eq!(*nullable, false);
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
                assert_eq!(*nullable, false);
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
                assert_eq!(*nullable, false);
            }
            _ => panic!("Expected named parameter"),
        }

        match &params[1] {
            ParameterSpec::Named { name, position, nullable } => {
                assert_eq!(name, "email");
                assert_eq!(*position, 2);
                assert_eq!(*nullable, true);
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
                assert_eq!(*nullable, true);
            }
            _ => panic!("Expected positional parameter"),
        }

        match &params[1] {
            ParameterSpec::Positional { position, nullable } => {
                assert_eq!(*position, 2);
                assert_eq!(*nullable, false);
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
                assert_eq!(*nullable, true);
            }
            _ => panic!("Expected named parameter"),
        }

        match &params[1] {
            ParameterSpec::Positional { position, nullable } => {
                assert_eq!(*position, 2);
                assert_eq!(*nullable, false);
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
        assert_eq!(queries[0].query_type, QueryType::Opt);
        assert_eq!(queries[1].name, "GetUserRequired");
        assert_eq!(queries[1].query_type, QueryType::One);
    }

    #[test]
    fn test_pgrpc_annotations() {
        let parser = SqlParser::new();

        let content = r#"
-- name: CreateUser :one
-- @pgrpc_not_null(id, created_at)
-- @pgrpc_throws 23505
INSERT INTO users (email, name) VALUES (:email, :name) RETURNING *;

-- name: UpdateUser :exec
-- @pgrpc_throws P0001
-- @pgrpc_throws 23503
UPDATE users SET name = :name WHERE id = :id;
        "#;

        let queries = parser.parse_content(content, PathBuf::from("test.sql")).unwrap();
        assert_eq!(queries.len(), 2);

        // Check first query annotations
        assert_eq!(queries[0].name, "CreateUser");
        assert!(queries[0].not_null_annotations.contains("id"));
        assert!(queries[0].not_null_annotations.contains("created_at"));
        assert_eq!(queries[0].not_null_annotations.len(), 2);
        assert_eq!(queries[0].throws_annotations, vec!["23505"]);

        // Check second query annotations
        assert_eq!(queries[1].name, "UpdateUser");
        assert!(queries[1].not_null_annotations.is_empty());
        assert_eq!(queries[1].throws_annotations, vec!["P0001", "23503"]);
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
        // Default placeholder is Many when not specified
        assert_eq!(queries[0].query_type, QueryType::Many);

        // Second query - explicit :one type
        assert_eq!(queries[1].name, "ExplicitOneQuery");
        assert_eq!(queries[1].explicit_query_type, Some(QueryType::One));
        assert_eq!(queries[1].query_type, QueryType::One);

        // Third query - no explicit type
        assert_eq!(queries[2].name, "InferredInsert");
        assert!(queries[2].explicit_query_type.is_none());
    }
}
