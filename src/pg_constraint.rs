use postgres::types::{FromSql, Type};
use serde::Deserialize;
use smallvec::SmallVec;
use std::error::Error;
use ustr::Ustr;

/// What a foreign key does to its rows when the referenced row is deleted
/// (`confdeltype`) or its key is updated (`confupdtype`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
pub enum FkAction {
    #[serde(rename = "r")]
    Restrict,
    #[serde(rename = "c")]
    Cascade,
    #[serde(rename = "a")]
    NoAction,
    #[serde(rename = "n")]
    SetNull,
    #[serde(rename = "d")]
    SetDefault,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
pub struct CheckConstraint {
    pub(crate) name: Ustr,
    /// `comment on constraint ...`, carrying `@pgrpc_message` / `@pgrpc_field`.
    #[serde(default)]
    pub(crate) comment: Option<String>,
    pub(crate) columns: SmallVec<Ustr, 2>,
    #[serde(default)]
    pub(crate) check_expression: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
pub struct ForeignKeyConstraint {
    pub(crate) name: Ustr,
    /// `comment on constraint ...`, carrying `@pgrpc_message` / `@pgrpc_field`.
    #[serde(default)]
    pub(crate) comment: Option<String>,
    pub(crate) columns: SmallVec<Ustr, 2>,
    pub(crate) on_delete: FkAction,
    pub(crate) on_update: FkAction,
    /// The referenced table name (schema-qualified if not in search_path)
    #[serde(default)]
    pub(crate) ref_table: Option<String>,
    /// The referenced columns in the foreign table
    #[serde(default)]
    pub(crate) ref_columns: Option<SmallVec<Ustr, 2>>,
    /// OID of the referenced table, for finding the foreign keys that point
    /// *at* a table (a DELETE or key UPDATE there fails on RESTRICT /
    /// NO ACTION ones).
    #[serde(default)]
    pub(crate) ref_table_oid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
pub struct PrimaryKeyConstraint {
    pub(crate) name: Ustr,
    /// `comment on constraint ...`, carrying `@pgrpc_message` / `@pgrpc_field`.
    #[serde(default)]
    pub(crate) comment: Option<String>,
    pub(crate) columns: SmallVec<Ustr, 2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
pub struct UniqueConstraint {
    pub(crate) name: Ustr,
    /// `comment on constraint ...`, carrying `@pgrpc_message` / `@pgrpc_field`.
    #[serde(default)]
    pub(crate) comment: Option<String>,
    /// The plain key columns. Empty for an index over expressions only.
    pub columns: SmallVec<Ustr, 2>,
    /// A partial (`WHERE ...`) or expression unique index. Its uniqueness
    /// depends on more than `columns`: a write to *any* column of the table
    /// (one the predicate or an expression reads) can violate it, and
    /// equality on `columns` does not identify a single row.
    #[serde(default)]
    pub conditional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
pub struct NotNullConstraint {
    pub(crate) name: Ustr,
    pub column: Ustr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
pub struct DefaultConstraint {
    pub(crate) name: Ustr,
    pub column: Ustr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
pub struct DomainConstraint {
    pub(crate) name: Ustr,
    pub column: Ustr,
    /// Name of the domain the CHECK belongs to (what `DbError::datatype()` reports).
    #[serde(default)]
    pub(crate) domain: Option<String>,
    /// Schema of that domain (what `DbError::schema()` reports).
    #[serde(default)]
    pub(crate) domain_schema: Option<String>,
    #[serde(default)]
    pub(crate) comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
pub struct ExclusionConstraint {
    pub(crate) name: Ustr,
    /// `comment on constraint ...`, carrying `@pgrpc_message` / `@pgrpc_field`.
    #[serde(default)]
    pub(crate) comment: Option<String>,
    pub(crate) columns: SmallVec<Ustr, 2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
#[serde(tag = "type")]
pub enum Constraint {
    #[serde(rename = "c")]
    Check(CheckConstraint),
    #[serde(rename = "f")]
    ForeignKey(ForeignKeyConstraint),
    #[serde(rename = "p")]
    PrimaryKey(PrimaryKeyConstraint),
    #[serde(rename = "u")]
    Unique(UniqueConstraint),
    #[serde(rename = "n")]
    NotNull(NotNullConstraint),
    #[serde(rename = "d")]
    Default(DefaultConstraint),
    #[serde(rename = "domain")]
    Domain(DomainConstraint),
    #[serde(rename = "x")]
    Exclusion(ExclusionConstraint),
}

impl FromSql<'_> for Constraint {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if *ty == Type::JSON || *ty == Type::JSONB {
            let json_str = std::str::from_utf8(raw)?;
            let parsed: Constraint = serde_json::from_str(json_str)?;
            Ok(parsed)
        } else {
            Err("unexpected datatype for Constraint".into())
        }
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSON || *ty == Type::JSONB
    }
}

impl Constraint {
    pub fn name(&self) -> Ustr {
        match self {
            Constraint::Check(check) => check.name,
            Constraint::ForeignKey(foreign_key) => foreign_key.name,
            Constraint::PrimaryKey(primary_key) => primary_key.name,
            Constraint::Unique(unique) => unique.name,
            Constraint::NotNull(not_null) => not_null.name,
            Constraint::Default(default) => default.name,
            Constraint::Domain(domain) => domain.name,
            Constraint::Exclusion(exclusion) => exclusion.name,
        }
    }

    /// The columns the constraint spans (one for column-level constraints).
    pub fn columns(&self) -> &[Ustr] {
        match self {
            Constraint::Check(c) => &c.columns,
            Constraint::ForeignKey(f) => &f.columns,
            Constraint::PrimaryKey(p) => &p.columns,
            Constraint::Unique(u) => &u.columns,
            Constraint::Exclusion(x) => &x.columns,
            Constraint::NotNull(n) => std::slice::from_ref(&n.column),
            Constraint::Default(d) => std::slice::from_ref(&d.column),
            Constraint::Domain(d) => std::slice::from_ref(&d.column),
        }
    }

    /// The constraint's Postgres comment, if any.
    pub fn comment(&self) -> Option<&str> {
        match self {
            Constraint::Check(c) => c.comment.as_deref(),
            Constraint::ForeignKey(f) => f.comment.as_deref(),
            Constraint::PrimaryKey(p) => p.comment.as_deref(),
            Constraint::Unique(u) => u.comment.as_deref(),
            Constraint::Exclusion(x) => x.comment.as_deref(),
            Constraint::Domain(d) => d.comment.as_deref(),
            Constraint::NotNull(_) | Constraint::Default(_) => None,
        }
    }

    /// The user-facing `@pgrpc_message` from the constraint's comment.
    pub fn message(&self) -> Option<String> {
        crate::annotations::Annotations::of(self.comment()).message
    }

    /// The form field a violation belongs to: the `@pgrpc_field` override if
    /// present (`@pgrpc_field()` meaning none), otherwise the single
    /// constrained column, otherwise none.
    pub fn field(&self) -> Option<String> {
        match crate::annotations::Annotations::of(self.comment()).field {
            Some(explicit) => explicit,
            None => match self.columns() {
                [single] => Some(single.to_string()),
                _ => None,
            },
        }
    }

    /// Whether a write to `cols` can violate this constraint.
    ///
    /// True when any written column is one the constraint spans. A constraint
    /// whose column list does not tell the whole story is conservatively
    /// affected by every write: a partial or expression unique index (the
    /// predicate and expressions read other columns), and any multi-column
    /// kind that arrived with no columns at all (an exclusion constraint over
    /// expressions, a `CHECK` that names no column).
    pub fn contains_columns(&self, cols: &[Ustr]) -> bool {
        match self {
            Constraint::Unique(u) if u.conditional => true,
            Constraint::Check(_)
            | Constraint::ForeignKey(_)
            | Constraint::PrimaryKey(_)
            | Constraint::Unique(_)
            | Constraint::Exclusion(_) => {
                let spanned = self.columns();
                spanned.is_empty() || cols.iter().any(|c| spanned.contains(c))
            }
            Constraint::NotNull(n) => cols.contains(&n.column),
            Constraint::Default(d) => cols.contains(&d.column),
            Constraint::Domain(d) => cols.contains(&d.column),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ustr::ustr;

    fn parse(json: &str) -> Constraint {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn plain_unique_index_only_matches_its_columns() {
        let c = parse(r#"{"type":"u","name":"t_email_idx","columns":["email"],"conditional":false}"#);
        assert!(c.contains_columns(&[ustr("email")]));
        assert!(!c.contains_columns(&[ustr("name")]));
    }

    #[test]
    fn unique_constraint_json_without_conditional_key_is_unconditional() {
        // pg_constraint-backed UNIQUE rows carry no `conditional` key.
        let c = parse(r#"{"type":"u","name":"t_email_key","columns":["email"]}"#);
        assert!(matches!(&c, Constraint::Unique(u) if !u.conditional));
    }

    #[test]
    fn partial_unique_index_is_affected_by_any_write() {
        // `unique (email) where deleted_at is null`: un-deleting a row writes
        // only `deleted_at` and can still collide.
        let c = parse(r#"{"type":"u","name":"t_live_email","columns":["email"],"conditional":true}"#);
        assert!(c.contains_columns(&[ustr("deleted_at")]));
        assert!(c.contains_columns(&[]));
        assert_eq!(c.field().as_deref(), Some("email"));
    }

    #[test]
    fn expression_only_unique_index_has_no_columns_and_matches_everything() {
        let c = parse(r#"{"type":"u","name":"t_lower_email","columns":[],"conditional":true}"#);
        assert!(c.columns().is_empty());
        assert!(c.contains_columns(&[ustr("anything")]));
    }

    #[test]
    fn column_less_check_and_exclusion_are_conservative() {
        let check = parse(r#"{"type":"c","name":"t_check","columns":[]}"#);
        assert!(check.contains_columns(&[ustr("a")]));
        let excl = parse(r#"{"type":"x","name":"t_excl","columns":[]}"#);
        assert!(excl.contains_columns(&[ustr("a")]));
    }
}
