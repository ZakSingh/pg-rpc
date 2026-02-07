use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashSet;

// Compiled regex patterns (compiled once, reused everywhere)
static NOT_NULL_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"@pgrpc_not_null\(([^)]+)\)").unwrap()
});

static THROWS_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"@pgrpc_throws\s+([A-Za-z0-9]{5})").unwrap()
});

static NULLABLE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"@pgrpc_nullable\(([^)]+)\)").unwrap()
});

/// Parse @pgrpc_not_null(col1, col2, ...) from a comment string
pub fn parse_not_null(comment: &str) -> HashSet<String> {
    let mut result = HashSet::new();
    for cap in NOT_NULL_REGEX.captures_iter(comment) {
        if let Some(cols) = cap.get(1) {
            for col in cols.as_str().split(',') {
                let col = col.trim();
                if !col.is_empty() {
                    result.insert(col.to_string());
                }
            }
        }
    }
    result
}

/// Parse @pgrpc_throws XXXXX from a comment string
pub fn parse_throws(comment: &str) -> Vec<String> {
    THROWS_REGEX
        .captures_iter(comment)
        .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .collect()
}

/// Parse @pgrpc_nullable(param1, param2, ...) from a comment string
pub fn parse_nullable(comment: &str) -> HashSet<String> {
    let mut result = HashSet::new();
    for cap in NULLABLE_REGEX.captures_iter(comment) {
        if let Some(params) = cap.get(1) {
            for param in params.as_str().split(',') {
                let param = param.trim();
                if !param.is_empty() {
                    result.insert(param.to_string());
                }
            }
        }
    }
    result
}

/// Check if comment contains @pgrpc_not_null (for simple boolean checks)
pub fn has_not_null(comment: &str) -> bool {
    comment.contains("@pgrpc_not_null")
}

/// Check if comment contains @pgrpc_flatten
pub fn has_flatten(comment: &str) -> bool {
    comment.contains("@pgrpc_flatten")
}

/// Check if comment contains @pgrpc_return_opt
pub fn has_return_opt(comment: &str) -> bool {
    comment.contains("@pgrpc_return_opt")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_not_null_single() {
        let comment = "@pgrpc_not_null(id)";
        let result = parse_not_null(comment);
        assert_eq!(result.len(), 1);
        assert!(result.contains("id"));
    }

    #[test]
    fn test_parse_not_null_multiple() {
        let comment = "@pgrpc_not_null(id, name, created_at)";
        let result = parse_not_null(comment);
        assert_eq!(result.len(), 3);
        assert!(result.contains("id"));
        assert!(result.contains("name"));
        assert!(result.contains("created_at"));
    }

    #[test]
    fn test_parse_not_null_with_surrounding_text() {
        let comment = "This is a comment with @pgrpc_not_null(field1, field2) annotation";
        let result = parse_not_null(comment);
        assert_eq!(result.len(), 2);
        assert!(result.contains("field1"));
        assert!(result.contains("field2"));
    }

    #[test]
    fn test_parse_not_null_multiple_annotations() {
        let comment = "@pgrpc_not_null(a) some text @pgrpc_not_null(b, c)";
        let result = parse_not_null(comment);
        assert_eq!(result.len(), 3);
        assert!(result.contains("a"));
        assert!(result.contains("b"));
        assert!(result.contains("c"));
    }

    #[test]
    fn test_parse_throws() {
        let comment = "@pgrpc_throws 23505 @pgrpc_throws P0001";
        let result = parse_throws(comment);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"23505".to_string()));
        assert!(result.contains(&"P0001".to_string()));
    }

    #[test]
    fn test_parse_nullable() {
        let comment = "@pgrpc_nullable(email, name)";
        let result = parse_nullable(comment);
        assert_eq!(result.len(), 2);
        assert!(result.contains("email"));
        assert!(result.contains("name"));
    }

    #[test]
    fn test_has_not_null() {
        assert!(has_not_null("@pgrpc_not_null"));
        assert!(has_not_null("some text @pgrpc_not_null(id)"));
        assert!(!has_not_null("no annotation here"));
    }

    #[test]
    fn test_has_flatten() {
        assert!(has_flatten("@pgrpc_flatten"));
        assert!(has_flatten("some text @pgrpc_flatten more"));
        assert!(!has_flatten("no annotation here"));
    }

    #[test]
    fn test_has_return_opt() {
        assert!(has_return_opt("@pgrpc_return_opt"));
        assert!(has_return_opt("some text @pgrpc_return_opt more"));
        assert!(!has_return_opt("no annotation here"));
    }
}
