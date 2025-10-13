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
    /// Named parameter: @param_name
    Named { name: String, position: usize },
    /// Positional parameter: $N (will need name inference)
    Positional { position: usize },
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
    /// Regex for matching named parameters @param_name
    named_param_regex: Regex,
}

impl SqlParser {
    pub fn new() -> Self {
        Self {
            annotation_regex: Regex::new(r"^--\s*name:\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:([a-z]+)\s*$")
                .unwrap(),
            named_param_regex: Regex::new(r"@([a-zA-Z_][a-zA-Z0-9_]*)").unwrap(),
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

    /// Transform named parameters (@param) to positional ($N) and extract parameter specs
    fn transform_parameters(&self, sql: &str) -> Result<(String, Vec<ParameterSpec>)> {
        let mut position = 1;
        let mut parameters = Vec::new();
        let mut seen_names = std::collections::HashSet::new();

        // Find all named parameters and their positions
        let transformed_sql = self.named_param_regex.replace_all(sql, |caps: &regex::Captures| {
            let param_name = caps[1].to_string();

            // Check for duplicate parameter names
            if seen_names.contains(&param_name) {
                // We can't return an error here due to closure constraints
                // We'll handle this by allowing it but it will be caught during introspection
                log::warn!("Duplicate parameter name: @{}", param_name);
            }
            seen_names.insert(param_name.clone());

            parameters.push(ParameterSpec::Named {
                name: param_name,
                position,
            });

            let replacement = format!("${}", position);
            position += 1;
            replacement
        }).to_string();

        // Now check for any existing $N parameters in the transformed SQL
        let positional_regex = Regex::new(r"\$(\d+)").unwrap();
        for cap in positional_regex.captures_iter(&transformed_sql) {
            let num: usize = cap[1].parse().unwrap();

            // Check if this position is already covered by a named parameter
            if !parameters.iter().any(|p| matches!(p, ParameterSpec::Named { position: pos, .. } if *pos == num)) {
                // This is a bare positional parameter, add it
                while parameters.len() < num {
                    let pos = parameters.len() + 1;
                    if !parameters.iter().any(|p| match p {
                        ParameterSpec::Named { position, .. } => *position == pos,
                        ParameterSpec::Positional { position } => *position == pos,
                    }) {
                        parameters.push(ParameterSpec::Positional { position: pos });
                    }
                }
            }
        }

        Ok((transformed_sql, parameters))
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
SELECT * FROM authors WHERE id = @author_id;

-- name: ListAuthors :many
SELECT * FROM authors ORDER BY name;

-- name: DeleteAuthor :exec
DELETE FROM authors WHERE id = @author_id;
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

        let sql = "SELECT * FROM users WHERE id = @user_id AND status = @status";
        let (transformed, params) = parser.transform_parameters(sql).unwrap();

        assert_eq!(transformed, "SELECT * FROM users WHERE id = $1 AND status = $2");
        assert_eq!(params.len(), 2);

        match &params[0] {
            ParameterSpec::Named { name, position } => {
                assert_eq!(name, "user_id");
                assert_eq!(*position, 1);
            }
            _ => panic!("Expected named parameter"),
        }

        match &params[1] {
            ParameterSpec::Named { name, position } => {
                assert_eq!(name, "status");
                assert_eq!(*position, 2);
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
WHERE u.id = @user_id;

-- name: CreateUser :one
INSERT INTO users (name, email)
VALUES (@name, @email)
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
                ParameterSpec::Positional { position } => {
                    assert_eq!(*position, i + 1);
                }
                _ => panic!("Expected positional parameter"),
            }
        }
    }

    #[test]
    fn test_mixed_parameters() {
        let parser = SqlParser::new();

        // Named parameter gets transformed first
        let sql = "SELECT * FROM users WHERE id = @user_id AND created > $2";
        let (transformed, params) = parser.transform_parameters(sql).unwrap();

        assert_eq!(transformed, "SELECT * FROM users WHERE id = $1 AND created > $2");
        assert_eq!(params.len(), 2);

        match &params[0] {
            ParameterSpec::Named { name, position } => {
                assert_eq!(name, "user_id");
                assert_eq!(*position, 1);
            }
            _ => panic!("Expected named parameter"),
        }
    }
}
