use regex::Regex;
use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;

pub type Span = Range<usize>;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct FunctionId {
    pub schema: String,
    pub name: String,
}

impl FunctionId {
    fn new(name: &str) -> Self {
        if let Some((schema, name)) = name.split_once('.') {
            Self {
                schema: schema.to_string(),
                name: name.to_string(),
            }
        } else {
            Self {
                schema: "public".to_string(),
                name: name.to_string(),
            }
        }
    }
}

fn find_closing_dollar_tag(content: &str) -> Option<(usize, &str)> {
    let mut pos = 0;
    let mut nested = Vec::new();

    while pos < content.len() {
        if let Some(dollar) = content[pos..].find('$') {
            pos += dollar;

            // Find the end of this tag
            let tag_end = content[pos..].find('$').map(|i| i + 1).unwrap_or(1);
            let tag = &content[pos..pos + tag_end];

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
    }

    None
}

pub fn get_function_spans(content: &str) -> std::io::Result<HashMap<FunctionId, Span>> {
    let mut functions: HashMap<FunctionId, Span> = HashMap::default();

    let fn_regex = Regex::new(r"(?i)CREATE\s+(OR\s+REPLACE\s+)?FUNCTION\s+([^\s(]+)").unwrap();

    let mut search_start = 0;
    while let Some(fn_match) = fn_regex.find_at(&content, search_start) {
        if let Some(caps) = fn_regex.captures_at(&content, search_start) {
            let fn_id = FunctionId::new(caps.get(2).unwrap().as_str());
            let fn_start = fn_match.start();

            // Find first dollar tag after function declaration
            if let Some(body_start) = content[fn_match.end()..].find('$') {
                let body_start = fn_match.end() + body_start;

                // Find matching closing tag, handling nested tags
                if let Some((body_end, _)) = find_closing_dollar_tag(&content[body_start..]) {
                    let body_end = body_start + body_end;

                    // Find terminating semicolon
                    if let Some(semicolon) = content[body_end..].find(';') {
                        let fn_end = body_end + semicolon + 1;

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
