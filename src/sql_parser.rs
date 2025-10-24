use anyhow::{anyhow, Result};
use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QueryType {
    /// Returns Option<T> - query_opt, single row or None
    One,
    /// Returns Vec<T> - query, multiple rows
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
            "many" => Ok(QueryType::Many),
            "exec" => Ok(QueryType::Exec),
            "execrows" => Ok(QueryType::ExecRows),
            _ => Err(anyhow!("Unknown query type: {}", s)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ParameterSpec {
    /// Named parameter: :param_name or pgrpc.narg('param_name') (nullable)
    Named { name: String, position: usize, nullable: bool },
    /// Positional parameter: $N or $N? (nullable)
    Positional { position: usize, nullable: bool },
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub name: String,
    pub query_type: QueryType,
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
}

pub struct SqlParser {
    /// Regex for matching -- name: FunctionName :type
    annotation_regex: Regex,
    /// Regex for matching named parameters :param_name
    named_param_regex: Regex,
    /// Regex for matching nullable parameters pgrpc.narg('param_name')
    nullable_param_regex: Regex,
}

impl SqlParser {
    pub fn new() -> Self {
        Self {
            annotation_regex: Regex::new(r"^--\s*name:\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:([a-z]+)\s*$")
                .unwrap(),
            named_param_regex: Regex::new(r":([a-zA-Z_][a-zA-Z0-9_]*)").unwrap(),
            nullable_param_regex: Regex::new(r"pgrpc\.narg\('([a-zA-Z_][a-zA-Z0-9_]*)'\)").unwrap(),
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
                let type_str = captures.get(2).unwrap().as_str();
                let query_type = QueryType::from_str(type_str)
                    .map_err(|e| anyhow!("Error at {}:{}: {}", file_path.display(), i + 1, e))?;

                let line_number = i + 1;

                // Collect the SQL query (all lines until next annotation or EOF)
                i += 1;
                let mut sql_lines = Vec::new();

                while i < lines.len() {
                    let sql_line = lines[i];

                    // Stop if we hit another annotation
                    if self.annotation_regex.is_match(sql_line.trim()) {
                        break;
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

                // Transform parameters and extract specifications
                let (postgres_sql, parameters) = self.transform_parameters(&original_sql)?;

                queries.push(ParsedQuery {
                    name,
                    query_type,
                    original_sql,
                    postgres_sql,
                    parameters,
                    file_path: file_path.clone(),
                    line_number,
                });
            } else {
                i += 1;
            }
        }

        Ok(queries)
    }

    /// Transform named parameters (:param) and nullable parameters (pgrpc.narg('param'))
    /// to positional ($N) and extract parameter specs
    fn transform_parameters(&self, sql: &str) -> Result<(String, Vec<ParameterSpec>)> {
        let mut position = 1;
        let mut parameters = Vec::new();
        let mut seen_params: std::collections::HashMap<String, (usize, bool)> = std::collections::HashMap::new();

        // Temporarily replace :: (type cast operator) with a placeholder to avoid matching it
        const TYPECAST_PLACEHOLDER: &str = "___PGRPC_TYPECAST___";
        let sql_with_placeholder = sql.replace("::", TYPECAST_PLACEHOLDER);

        // Step 1: Process nullable parameters first (pgrpc.narg('param_name'))
        let sql_after_nullable = self.nullable_param_regex.replace_all(&sql_with_placeholder, |caps: &regex::Captures| {
            let param_name = caps[1].to_string();

            // Check if we've seen this parameter name before
            if let Some(&(existing_pos, existing_nullable)) = seen_params.get(&param_name) {
                // Reuse the same position for duplicate parameter names
                if !existing_nullable {
                    log::warn!(
                        "Parameter '{}' has inconsistent nullable annotations (both pgrpc.narg and non-nullable)",
                        param_name
                    );
                }
                format!("${}", existing_pos)
            } else {
                // New parameter, assign a position
                seen_params.insert(param_name.clone(), (position, true));
                parameters.push(ParameterSpec::Named {
                    name: param_name,
                    position,
                    nullable: true,
                });

                let replacement = format!("${}", position);
                position += 1;
                replacement
            }
        }).to_string();

        // Step 2: Process regular named parameters (:param_name)
        let sql_after_named = self.named_param_regex.replace_all(&sql_after_nullable, |caps: &regex::Captures| {
            let param_name = caps[1].to_string();

            // Check if we've seen this parameter name before
            if let Some(&(existing_pos, existing_nullable)) = seen_params.get(&param_name) {
                // Reuse the same position for duplicate parameter names
                if existing_nullable {
                    log::warn!(
                        "Parameter '{}' has inconsistent nullable annotations (both pgrpc.narg and non-nullable)",
                        param_name
                    );
                }
                format!("${}", existing_pos)
            } else {
                // New parameter, assign a position
                seen_params.insert(param_name.clone(), (position, false));
                parameters.push(ParameterSpec::Named {
                    name: param_name,
                    position,
                    nullable: false,
                });

                let replacement = format!("${}", position);
                position += 1;
                replacement
            }
        }).to_string();

        // Step 3: Check for any existing $N or $N? parameters in the ORIGINAL SQL
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
        let (transformed, params) = parser.transform_parameters(sql).unwrap();

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
        let (transformed, params) = parser.transform_parameters(sql).unwrap();

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
        let (transformed, params) = parser.transform_parameters(sql).unwrap();

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
    fn test_nullable_named_parameters() {
        let parser = SqlParser::new();

        let sql = "SELECT * FROM users WHERE id = :user_id AND email = pgrpc.narg('email')";
        let (transformed, params) = parser.transform_parameters(sql).unwrap();

        assert_eq!(transformed, "SELECT * FROM users WHERE id = $2 AND email = $1");
        assert_eq!(params.len(), 2);

        match &params[0] {
            ParameterSpec::Named { name, position, nullable } => {
                assert_eq!(name, "email");
                assert_eq!(*position, 1);
                assert_eq!(*nullable, true);
            }
            _ => panic!("Expected named parameter"),
        }

        match &params[1] {
            ParameterSpec::Named { name, position, nullable } => {
                assert_eq!(name, "user_id");
                assert_eq!(*position, 2);
                assert_eq!(*nullable, false);
            }
            _ => panic!("Expected named parameter"),
        }
    }

    #[test]
    fn test_nullable_positional_parameters() {
        let parser = SqlParser::new();

        let sql = "SELECT * FROM users WHERE id = $1? AND status = $2";
        let (transformed, params) = parser.transform_parameters(sql).unwrap();

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
    fn test_mixed_nullable_parameters() {
        let parser = SqlParser::new();

        let sql = "SELECT * FROM users WHERE id = pgrpc.narg('user_id') AND created > $2";
        let (transformed, params) = parser.transform_parameters(sql).unwrap();

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
}
