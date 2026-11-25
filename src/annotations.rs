//! The `@pgrpc_*` annotation language.
//!
//! Annotations live in `COMMENT ON ...` text and in the `--` comment lines of
//! a query. They state facts about the database that the analysis cannot see;
//! Rust-side codegen preferences belong in the config instead.
//!
//! | Tag | Meaning |
//! |---|---|
//! | `@pgrpc_not_null` | this object (a column, a function's return value) is never NULL |
//! | `@pgrpc_not_null(a, b)` | the named children (attributes, OUT params, result columns) are never NULL |
//! | `@pgrpc_not_null(*)` | every attribute of this composite type is never NULL |
//! | `@pgrpc_nullable` | this column may be NULL (an exception to the type's `(*)`) |
//! | `@pgrpc_nullable(a, b)` | the named children may be NULL (query parameters; exceptions to `(*)`) |
//! | `@pgrpc_message text` | user-facing message, to the end of the line |
//! | `@pgrpc_field(name)` / `@pgrpc_field()` | the form field a message belongs to, or none |
//!
//! Every comment is parsed once by [`parse`]. A tag that is unknown, malformed
//! or on an object where it means nothing is an error ([`Annotations::misplaced`]),
//! never silently ignored.

use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};

static TAG: Lazy<Regex> = Lazy::new(|| Regex::new(r"@pgrpc_([A-Za-z0-9_]*)").unwrap());
static NAME: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_$]*$").unwrap());

/// The annotations found in one comment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Annotations {
    /// Bare `@pgrpc_not_null`.
    pub not_null_this: bool,
    /// `@pgrpc_not_null(*)`.
    pub not_null_all: bool,
    /// Names listed in `@pgrpc_not_null(a, b)`.
    pub not_null: BTreeSet<String>,
    /// Bare `@pgrpc_nullable`.
    pub nullable_this: bool,
    /// Names listed in `@pgrpc_nullable(a, b)`.
    pub nullable: BTreeSet<String>,
    /// `@pgrpc_message <text to end of line>`.
    pub message: Option<String>,
    /// `@pgrpc_field(name)` is `Some(Some(name))`; `@pgrpc_field()`, i.e.
    /// explicitly no field, is `Some(None)`.
    pub field: Option<Option<String>>,
}

/// What a comment is attached to, which decides the annotations it may carry.
#[derive(Debug, Clone, Copy)]
pub enum Site<'a> {
    /// A column of a table, view or composite type, or a payload field.
    Column,
    /// A table, view or composite type with the given columns. `all` allows
    /// `@pgrpc_not_null(*)` (standalone composite types, where nothing can be
    /// inferred); `message` allows `@pgrpc_message` / `@pgrpc_field` (error
    /// payload types).
    Type { children: &'a [&'a str], all: bool, message: bool },
    /// A function. `returns_value`: it returns a single value (not a set, not
    /// void, no OUT params), so bare `@pgrpc_not_null` applies. `outs`: its
    /// OUT / TABLE parameters.
    Function { returns_value: bool, outs: &'a [&'a str] },
    /// The comment lines of a query in a query file. Result columns and
    /// parameters are checked by the caller once they are known.
    Query,
    /// A table or domain constraint.
    Constraint,
    /// Anything else (a domain, an enum, ...): no annotation applies.
    Other(&'a str),
}

/// Tags that used to exist, with what replaces them.
fn removed(tag: &str) -> Option<&'static str> {
    Some(match tag {
        "strict" => "list the domain under `strict_domains` in the config instead",
        "builder" => "list the type under `builders` in the config instead",
        "throws" => "errors are inferred from the function or query itself",
        "return_opt" => "return the row type (not SETOF) to get an `Option`",
        _ => return None,
    })
}

/// `(a, b)` directly after a tag: the items and the text after the `)`.
/// `Ok(None)` when the tag is not followed by `(`.
fn list(rest: &str) -> Result<Option<Vec<&str>>, &'static str> {
    let Some(inner) = rest.strip_prefix('(') else {
        return Ok(None);
    };
    let Some(end) = inner.find(')') else {
        return Err("is missing its closing `)`");
    };
    Ok(Some(
        inner[..end].split(',').map(str::trim).filter(|s| !s.is_empty()).collect(),
    ))
}

/// Parse every annotation in a comment. The second value lists what is wrong
/// with it (unknown tags, bad syntax, contradictions); the annotations are
/// whatever could be made of the rest.
pub fn parse(comment: &str) -> (Annotations, Vec<String>) {
    let mut a = Annotations::default();
    let mut errors = Vec::new();

    for cap in TAG.captures_iter(comment) {
        let tag = &cap[1];
        let rest = &comment[cap.get(0).unwrap().end()..];
        match tag {
            "not_null" | "nullable" => {
                let (this, names) = if tag == "not_null" {
                    (&mut a.not_null_this, &mut a.not_null)
                } else {
                    (&mut a.nullable_this, &mut a.nullable)
                };
                match list(rest) {
                    Err(e) => errors.push(format!("`@pgrpc_{tag}(` {e}")),
                    Ok(None) => *this = true,
                    Ok(Some(items)) if items.is_empty() => {
                        errors.push(format!("`@pgrpc_{tag}()` names nothing"))
                    }
                    Ok(Some(items)) if items == ["*"] => {
                        if tag == "not_null" {
                            a.not_null_all = true;
                        } else {
                            errors.push("`@pgrpc_nullable(*)` is not supported".to_string());
                        }
                    }
                    Ok(Some(items)) => {
                        for item in items {
                            if NAME.is_match(item) {
                                names.insert(item.to_string());
                            } else {
                                errors.push(format!("`@pgrpc_{tag}(...)`: `{item}` is not a name"));
                            }
                        }
                    }
                }
            }
            "message" => {
                let text = rest.lines().next().unwrap_or("").trim();
                if text.is_empty() {
                    errors.push("`@pgrpc_message` has no text".to_string());
                } else if a.message.replace(text.to_string()).is_some() {
                    errors.push("more than one `@pgrpc_message`".to_string());
                }
            }
            "field" => match list(rest) {
                Err(e) => errors.push(format!("`@pgrpc_field(` {e}")),
                Ok(None) => errors.push(
                    "`@pgrpc_field` needs `(name)`, or `()` for no field".to_string(),
                ),
                Ok(Some(items)) if items.len() > 1 => {
                    errors.push("`@pgrpc_field(...)` takes one name".to_string())
                }
                Ok(Some(items)) => {
                    let name = items.first().map(|s| s.to_string());
                    if a.field.replace(name).is_some() {
                        errors.push("more than one `@pgrpc_field`".to_string());
                    }
                }
            },
            _ => errors.push(match removed(tag) {
                Some(instead) => format!("`@pgrpc_{tag}` has been removed: {instead}"),
                None => format!(
                    "unknown annotation `@pgrpc_{tag}` (known: @pgrpc_not_null, \
                     @pgrpc_nullable, @pgrpc_message, @pgrpc_field)"
                ),
            }),
        }
    }

    if a.not_null_this && (a.not_null_all || !a.not_null.is_empty()) {
        errors.push("`@pgrpc_not_null` is used both bare and with a list".to_string());
    }
    if a.nullable_this && !a.nullable.is_empty() {
        errors.push("`@pgrpc_nullable` is used both bare and with a list".to_string());
    }
    if a.not_null_this && a.nullable_this {
        errors.push("both `@pgrpc_not_null` and `@pgrpc_nullable`".to_string());
    }

    (a, errors)
}

impl Annotations {
    /// The annotations of an optional comment, ignoring anything wrong with
    /// it: problems are reported once, by the validation pass, not at every
    /// place the comment is read.
    pub fn of(comment: Option<&str>) -> Self {
        comment.map(|c| parse(c).0).unwrap_or_default()
    }

    /// Whether these annotations (those of a type, relation or function) and
    /// `child`'s own (those of the column) declare the child `name` NOT NULL.
    /// The column's own annotation wins over the parent's lists, and a
    /// `nullable` exception wins over `(*)`.
    pub fn child_not_null(&self, name: &str, child: &Annotations) -> bool {
        if child.nullable_this {
            false
        } else if child.not_null_this {
            true
        } else if self.nullable.contains(name) {
            false
        } else {
            self.not_null_all || self.not_null.contains(name)
        }
    }

    /// [`Self::child_not_null`] for a child that has no comment of its own.
    pub fn names_not_null(&self, name: &str) -> bool {
        self.child_not_null(name, &Annotations::default())
    }

    /// The annotations that mean nothing at `site`, one message each.
    pub fn misplaced(&self, site: Site) -> Vec<String> {
        let mut errors = Vec::new();
        let mut no = |used: bool, what: &str, why: &str| {
            if used {
                errors.push(format!("{what} {why}"));
            }
        };
        let has_lists = self.not_null_all || !self.not_null.is_empty() || !self.nullable.is_empty();
        let has_message = self.message.is_some() || self.field.is_some();

        match site {
            Site::Column => {
                no(has_lists, "a list of names", "does not apply to a column; use the bare form");
                no(has_message, "`@pgrpc_message` / `@pgrpc_field`", "does not apply to a column");
            }
            Site::Type { children, all, message } => {
                no(
                    self.not_null_this || self.nullable_this,
                    "bare `@pgrpc_not_null` / `@pgrpc_nullable`",
                    "does not apply to a type; name its columns: `@pgrpc_not_null(a, b)`",
                );
                no(
                    self.not_null_all && !all,
                    "`@pgrpc_not_null(*)`",
                    "is only for standalone composite types; nullability here is inferred",
                );
                no(
                    has_message && !message,
                    "`@pgrpc_message` / `@pgrpc_field`",
                    "applies to constraints and error payload types only",
                );
                // (On a query the two lists name different things: result
                // columns and parameters.)
                for name in self.not_null.intersection(&self.nullable) {
                    errors.push(format!("`{name}` is listed as both not null and nullable"));
                }
                for name in self.not_null.iter().chain(&self.nullable) {
                    if !children.contains(&name.as_str()) {
                        errors.push(format!("`{name}` is not one of its columns"));
                    }
                }
                if !self.nullable.is_empty() && !self.not_null_all {
                    errors.push(
                        "`@pgrpc_nullable(...)` only makes sense next to `@pgrpc_not_null(*)`"
                            .to_string(),
                    );
                }
            }
            Site::Function { returns_value, outs } => {
                no(
                    self.not_null_this && !returns_value,
                    "bare `@pgrpc_not_null`",
                    if outs.is_empty() {
                        "applies to a function returning a single value; this one returns a set or nothing"
                    } else {
                        "does not apply to a function with OUT parameters; name them: `@pgrpc_not_null(a, b)`"
                    },
                );
                no(self.nullable_this, "bare `@pgrpc_nullable`", "does not apply to a function");
                no(self.not_null_all, "`@pgrpc_not_null(*)`", "does not apply to a function");
                no(
                    !self.nullable.is_empty(),
                    "`@pgrpc_nullable(...)`",
                    "does not apply to a function (arguments are always passed as given)",
                );
                no(has_message, "`@pgrpc_message` / `@pgrpc_field`", "does not apply to a function");
                for name in &self.not_null {
                    if !outs.contains(&name.as_str()) {
                        errors.push(format!(
                            "`@pgrpc_not_null({name})`: `{name}` is not an OUT parameter"
                        ));
                    }
                }
            }
            Site::Query => {
                no(
                    self.not_null_this || self.nullable_this,
                    "bare `@pgrpc_not_null` / `@pgrpc_nullable`",
                    "does not apply to a query; name the columns or parameters",
                );
                no(self.not_null_all, "`@pgrpc_not_null(*)`", "does not apply to a query");
                no(has_message, "`@pgrpc_message` / `@pgrpc_field`", "does not apply to a query");
            }
            Site::Constraint => {
                no(
                    self.not_null_this || self.nullable_this || has_lists,
                    "`@pgrpc_not_null` / `@pgrpc_nullable`",
                    "does not apply to a constraint",
                );
                no(
                    self.field.is_some() && self.message.is_none(),
                    "`@pgrpc_field`",
                    "has no `@pgrpc_message` to attach to",
                );
            }
            Site::Other(kind) => {
                if *self != Annotations::default() {
                    errors.push(format!("no annotation applies to {kind}"));
                }
            }
        }
        errors
    }
}

/// Everything wrong with the annotations in `comment`, given what it is
/// attached to: syntax, unknown tags, and tags that mean nothing there.
pub fn check(comment: &str, site: Site) -> Vec<String> {
    let (annotations, mut errors) = parse(comment);
    errors.extend(annotations.misplaced(site));
    errors
}

/// `Err` listing every problem, if there are any.
pub fn fail_on(problems: Vec<String>) -> anyhow::Result<()> {
    if problems.is_empty() {
        return Ok(());
    }
    anyhow::bail!(
        "invalid @pgrpc annotations:\n  {}",
        problems.into_iter().collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>().join("\n  ")
    )
}

static REDUNDANT: AtomicUsize = AtomicUsize::new(0);

/// Report a `@pgrpc_not_null` that asserts what the analysis already proves.
/// Such an annotation changes nothing today and would hide the day a schema
/// change makes it wrong, so it is better deleted.
pub fn warn_redundant(owner: &str, name: &str) {
    REDUNDANT.fetch_add(1, Ordering::Relaxed);
    log::warn!("{owner}: @pgrpc_not_null on `{name}` is redundant, it is already inferred NOT NULL");
}

/// How many redundant annotations [`warn_redundant`] has reported.
pub fn redundant_count() -> usize {
    REDUNDANT.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(comment: &str) -> Annotations {
        let (a, errors) = parse(comment);
        assert!(errors.is_empty(), "{errors:?}");
        a
    }

    fn names(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn message_runs_to_end_of_line_and_is_optional() {
        let c = "Printed on the certificate, where there is room.\n\n  @pgrpc_message Title must be 60 characters or fewer  \n@pgrpc_field(title)";
        let a = ok(c);
        assert_eq!(a.message.as_deref(), Some("Title must be 60 characters or fewer"));
        assert_eq!(a.field, Some(Some("title".to_string())));
        assert_eq!(ok("just prose, no tag").message, None);
        assert!(!parse("@pgrpc_message   ").1.is_empty());
    }

    #[test]
    fn message_may_contain_parentheses() {
        let a = ok("@pgrpc_message Weight exceeds the carrier maximum (66 lb / 30 kg).");
        assert_eq!(a.message.as_deref(), Some("Weight exceeds the carrier maximum (66 lb / 30 kg)."));
    }

    #[test]
    fn field_tag_three_states() {
        assert_eq!(ok("no tag here").field, None);
        assert_eq!(ok("@pgrpc_field()").field, Some(None));
        assert_eq!(ok("@pgrpc_field( promo_code )").field, Some(Some("promo_code".to_string())));
        assert!(!parse("@pgrpc_field").1.is_empty());
    }

    #[test]
    fn not_null_bare_list_and_all() {
        assert!(ok("Magnitude. @pgrpc_not_null").not_null_this);
        let a = ok("@pgrpc_not_null(a) some text @pgrpc_not_null(b, c)");
        assert!(!a.not_null_this);
        assert_eq!(a.not_null, names(&["a", "b", "c"]));
        let a = ok("@pgrpc_not_null(*) @pgrpc_nullable(note)");
        assert!(a.not_null_all);
        assert_eq!(a.nullable, names(&["note"]));
    }

    #[test]
    fn a_list_is_not_also_the_bare_form() {
        // `@pgrpc_not_null(p_id)` used to count as bare too (substring match).
        assert!(!ok("@pgrpc_not_null(p_id)").not_null_this);
    }

    #[test]
    fn unknown_and_removed_tags_are_errors() {
        let (_, errors) = parse("@pgrpc_omit");
        assert!(errors[0].contains("unknown annotation `@pgrpc_omit`"), "{errors:?}");
        let (_, errors) = parse("@pgrpc_not_nul(a)");
        assert!(errors[0].contains("unknown annotation"), "{errors:?}");
        for tag in ["strict", "builder", "throws 23505", "return_opt"] {
            let (_, errors) = parse(&format!("@pgrpc_{tag}"));
            assert!(errors[0].contains("has been removed"), "{errors:?}");
        }
    }

    #[test]
    fn malformed_lists_are_errors() {
        assert!(!parse("@pgrpc_not_null(a, b").1.is_empty());
        assert!(!parse("@pgrpc_not_null()").1.is_empty());
        assert!(!parse("@pgrpc_not_null(a b)").1.is_empty());
        assert!(!parse("@pgrpc_nullable(*)").1.is_empty());
        assert!(!parse("@pgrpc_not_null @pgrpc_not_null(a)").1.is_empty());
    }

    #[test]
    fn child_resolution() {
        let none = Annotations::default();
        let ty = ok("@pgrpc_not_null(*) @pgrpc_nullable(note)");
        assert!(ty.names_not_null("id"));
        assert!(!ty.names_not_null("note"));
        assert!(ty.child_not_null("note", &ok("@pgrpc_not_null")));
        assert!(!ty.child_not_null("id", &ok("@pgrpc_nullable")));

        let ty = ok("@pgrpc_not_null(a)");
        assert!(ty.names_not_null("a"));
        assert!(!ty.names_not_null("b"));
        assert!(none.child_not_null("b", &ok("@pgrpc_not_null")));
    }

    #[test]
    fn placement() {
        let cols = ["a", "b"];
        let ty = Site::Type { children: &cols, all: true, message: false };
        assert!(check("@pgrpc_not_null(a, b)", ty).is_empty());
        assert!(check("@pgrpc_not_null(*) @pgrpc_nullable(b)", ty).is_empty());
        assert!(!check("@pgrpc_not_null", ty).is_empty());
        assert!(!check("@pgrpc_not_null(c)", ty).is_empty());
        assert!(!check("@pgrpc_nullable(b)", ty).is_empty());
        assert!(!check("@pgrpc_not_null(*) @pgrpc_not_null(a) @pgrpc_nullable(a)", ty).is_empty());
        assert!(!check("@pgrpc_message Nope", ty).is_empty());
        let view = Site::Type { children: &cols, all: false, message: false };
        assert!(!check("@pgrpc_not_null(*)", view).is_empty());

        assert!(check("@pgrpc_not_null", Site::Column).is_empty());
        assert!(check("@pgrpc_nullable", Site::Column).is_empty());
        assert!(!check("@pgrpc_not_null(a)", Site::Column).is_empty());

        let scalar = Site::Function { returns_value: true, outs: &[] };
        assert!(check("@pgrpc_not_null", scalar).is_empty());
        assert!(!check("@pgrpc_not_null(p_id)", scalar).is_empty());
        let outs = ["o_a"];
        let with_outs = Site::Function { returns_value: false, outs: &outs };
        assert!(check("@pgrpc_not_null(o_a)", with_outs).is_empty());
        assert!(!check("@pgrpc_not_null", with_outs).is_empty());
        assert!(!check("@pgrpc_not_null(p_in)", with_outs).is_empty());
        let set = Site::Function { returns_value: false, outs: &[] };
        assert!(!check("@pgrpc_not_null", set).is_empty());

        assert!(check("@pgrpc_message No\n@pgrpc_field()", Site::Constraint).is_empty());
        assert!(!check("@pgrpc_field(a)", Site::Constraint).is_empty());
        assert!(!check("@pgrpc_not_null", Site::Constraint).is_empty());

        assert!(check("@pgrpc_not_null(a) @pgrpc_nullable(p)", Site::Query).is_empty());
        // A result column and a parameter may share a name.
        assert!(check("@pgrpc_not_null(grade) @pgrpc_nullable(grade)", Site::Query).is_empty());
        assert!(!check("@pgrpc_not_null", Site::Query).is_empty());

        assert!(check("Just prose.", Site::Other("a domain")).is_empty());
        assert!(!check("@pgrpc_not_null", Site::Other("a domain")).is_empty());
    }
}
