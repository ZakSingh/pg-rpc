use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::BTreeSet;

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

static MESSAGE_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"@pgrpc_message[ \t]+([^\r\n]*)").unwrap());

static FIELD_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"@pgrpc_field\(([^)]*)\)").unwrap());

/// Parse `@pgrpc_message <text to end of line>` from a constraint comment.
pub fn parse_message(comment: &str) -> Option<String> {
    MESSAGE_REGEX
        .captures(comment)
        .and_then(|cap| cap.get(1))
        .map(|m| m.as_str().trim().to_string())
        .filter(|m| !m.is_empty())
}

/// Parse `@pgrpc_field(name)` / `@pgrpc_field()` from a constraint comment.
///
/// `None`: no tag. `Some(None)`: `@pgrpc_field()`, i.e. explicitly no field
/// (form-level). `Some(Some(name))`: the field to attribute the message to.
pub fn parse_field(comment: &str) -> Option<Option<String>> {
    FIELD_REGEX.captures(comment).map(|cap| {
        let name = cap.get(1).map(|m| m.as_str().trim()).unwrap_or("");
        if name.is_empty() {
            None
        } else {
            Some(name.to_string())
        }
    })
}

/// Parse @pgrpc_not_null(col1, col2, ...) from a comment string
pub fn parse_not_null(comment: &str) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
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
pub fn parse_nullable(comment: &str) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
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

/// Check if comment contains @pgrpc_return_opt
pub fn has_return_opt(comment: &str) -> bool {
    comment.contains("@pgrpc_return_opt")
}

/// Check if comment contains @pgrpc_builder
pub fn has_builder(comment: &str) -> bool {
    comment.contains("@pgrpc_builder")
}

/// Check if comment contains @pgrpc_strict
pub fn has_strict(comment: &str) -> bool {
    comment.contains("@pgrpc_strict")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_runs_to_end_of_line_and_is_optional() {
        let c = "Printed on the certificate, where there is room.\n\n  @pgrpc_message Title must be 60 characters or fewer  \n@pgrpc_field(title)";
        assert_eq!(parse_message(c).as_deref(), Some("Title must be 60 characters or fewer"));
        assert_eq!(parse_message("just prose, no tag"), None);
        assert_eq!(parse_message("@pgrpc_message   "), None);
    }

    #[test]
    fn field_tag_three_states() {
        assert_eq!(parse_field("no tag here"), None);
        assert_eq!(parse_field("@pgrpc_field()"), Some(None));
        assert_eq!(parse_field("@pgrpc_field( promo_code )"), Some(Some("promo_code".to_string())));
    }

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
    fn test_has_return_opt() {
        assert!(has_return_opt("@pgrpc_return_opt"));
        assert!(has_return_opt("some text @pgrpc_return_opt more"));
        assert!(!has_return_opt("no annotation here"));
    }

    #[test]
    fn test_has_builder() {
        assert!(has_builder("@pgrpc_builder"));
        assert!(has_builder("@pgrpc_builder Filter for listing items"));
        assert!(has_builder("some text @pgrpc_builder more"));
        assert!(!has_builder("no annotation here"));
    }

    #[test]
    fn test_has_strict() {
        assert!(has_strict("@pgrpc_strict"));
        assert!(has_strict("US zip code. @pgrpc_strict"));
        assert!(!has_strict("no annotation here"));
    }
}
