use crate::fn_index::FunctionId;
use regex::Regex;
use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;

pub type Span = Range<usize>;

pub type SrcLoc = (PathBuf, Span);
fn find_closing_dollar_tag(content: &str) -> Option<(usize, &str)> {
    let mut pos = 0;
    let mut nested = Vec::new();

    while pos < content.len() {
        if let Some(dollar) = content[pos..].find('$') {
            pos += dollar;

            // Find the next dollar sign to complete the tag
            if let Some(next_dollar) = content[pos + 1..].find('$') {
                let tag = &content[pos..=pos + next_dollar + 1];

                if let Some(last_tag) = nested.last() {
                    if tag == *last_tag {
                        nested.pop();
                        if nested.is_empty() {
                            return Some((pos + tag.len(), tag));
                        }
                    } else {
                        nested.push(tag);
                    }
                } else {
                    nested.push(tag);
                }

                pos += tag.len();
            } else {
                break;
            }
        } else {
            break;
        }
    }

    None
}

pub fn get_function_spans(content: &str) -> std::io::Result<HashMap<FunctionId, Span>> {
    let mut functions: HashMap<FunctionId, Span> = HashMap::default();

    // Removed ^ anchor and added more flexible whitespace handling
    let fn_regex = Regex::new(r"(?i)CREATE\s+(OR\s+REPLACE\s+)?FUNCTION\s+([^(\s]+)").unwrap();

    let mut search_start = 0;
    while let Some(fn_match) = fn_regex.find_at(&content, search_start) {
        if let Some(caps) = fn_regex.captures_at(&content, search_start) {
            let fn_name = caps.get(2).unwrap().as_str();
            // Remove any inline comments from function name
            let fn_id = FunctionId::new(fn_name.split("/*").next().unwrap_or(fn_name).trim());
            let fn_start = fn_match.start();

            // Find first opening dollar tag
            if let Some(body_start) = content[fn_match.end()..].find('$') {
                let body_start = fn_match.end() + body_start;

                // Find matching closing tag
                if let Some((body_end, _tag)) = find_closing_dollar_tag(&content[body_start..]) {
                    let absolute_body_end = body_start + body_end;

                    // Find terminating semicolon
                    if let Some(semicolon_pos) = content[absolute_body_end..].find(';') {
                        let fn_end = absolute_body_end + semicolon_pos + 1;

                        functions.insert(
                            fn_id,
                            Span {
                                start: fn_start,
                                end: fn_end,
                            },
                        );

                        search_start = fn_end;
                        continue;
                    }
                }
            }
        }
        search_start = fn_match.end();
    }

    Ok(functions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_closing_dollar_tag() {
        let content = "$body$\nBEGIN\n    RETURN 1;\nEND;\n$body$";
        let result = find_closing_dollar_tag(content);
        assert!(result.is_some());
        let (pos, tag) = result.unwrap();
        assert_eq!(tag, "$body$");
        assert_eq!(pos, content.len());
    }

    #[test]
    fn test_multiple_functions() {
        let sql = r#"
CREATE FUNCTION func1() RETURNS void
$func$
BEGIN
    NULL;
END;
$func$;

CREATE FUNCTION func2() RETURNS void
$func$
BEGIN
    NULL;
END;
$func$;
"#;
        let spans = get_function_spans(sql).unwrap();
        assert_eq!(spans.len(), 2, "Should find exactly two functions");
        assert!(
            spans.contains_key(&FunctionId::new("func1")),
            "Function 'func1' not found in spans"
        );
        assert!(
            spans.contains_key(&FunctionId::new("func2")),
            "Function 'func2' not found in spans"
        );
    }

    #[test]
    fn test_function_with_comments() {
        let sql = r#"
-- This is a comment
CREATE FUNCTION /* inline comment */ commented_func() RETURNS void
$body$
BEGIN
    -- Inside function comment
    NULL;
END;
$body$;
"#;
        let spans = get_function_spans(sql).unwrap();
        assert_eq!(spans.len(), 1, "Should find exactly one function");
        assert!(
            spans.contains_key(&FunctionId::new("commented_func")),
            "Function 'commented_func' not found in spans"
        );
    }

    #[test]
    fn test_function_not_at_start() {
        let sql = r#"
-- Some initial comments
-- and more comments

SELECT 1;

CREATE FUNCTION not_first() RETURNS void
$body$
BEGIN
    NULL;
END;
$body$;
"#;
        let spans = get_function_spans(sql).unwrap();
        assert_eq!(spans.len(), 1, "Should find exactly one function");
        assert!(
            spans.contains_key(&FunctionId::new("not_first")),
            "Function 'not_first' not found in spans"
        );
    }
}
