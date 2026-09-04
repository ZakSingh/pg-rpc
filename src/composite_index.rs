//! In-memory map of composite types (and the domains wrapping them) to their
//! attributes, used to answer "can `(expr).attr` be NULL?".
//!
//! Postgres never marks a view column NOT NULL, and it does not allow NOT NULL
//! on the attributes of a standalone composite type, so when a view projects a
//! single attribute out of a composite-typed column (`(p.length).amount`) the
//! catalog offers no direct evidence about the projection's nullability. The
//! evidence lives on the type instead, in exactly the places pgrpc already
//! consults when it turns the composite into a generated struct:
//!
//!   * `@pgrpc_not_null` in the attribute's comment,
//!   * a bulk `@pgrpc_not_null(a, b)` in the type's comment,
//!   * `attnotnull`, for the row type of a table, and
//!   * a domain over the composite whose CHECK asserts `(VALUE).attr IS NOT NULL`.
//!
//! This index folds all of those into a single `not_null` bit per attribute so
//! [`crate::view_nullability::ViewNullabilityAnalyzer`] can walk an attribute
//! chain without a database round-trip. It is loaded once, for every composite
//! and domain outside the system schemas — a projection may reach a type in
//! any schema regardless of which schemas code is generated for.

use crate::annotations;
use crate::codegen::OID;
use crate::parse_domain::non_null_cols_from_checks;
use anyhow::Context;
use postgres::Client;
use std::collections::{HashMap, HashSet};

/// Upper bound on domain-over-domain-over-... nesting we are willing to follow.
/// Postgres has no cycle here, but a bound keeps the walk total on corrupt input.
const MAX_DOMAIN_DEPTH: usize = 16;

#[derive(Debug, Clone)]
pub struct CompositeAttr {
    pub name: String,
    /// Position within the composite (`pg_attribute.attnum`).
    pub attnum: i16,
    pub type_oid: OID,
    /// True when the type's own metadata proves the attribute NOT NULL
    /// (annotation, bulk annotation, or `attnotnull`). Domain CHECKs are
    /// layered on top at lookup time, since they belong to the wrapping domain
    /// rather than to the composite.
    pub not_null: bool,
}

#[derive(Debug, Clone, Default)]
struct CompositeType {
    attrs: Vec<CompositeAttr>,
}

#[derive(Debug, Clone)]
struct DomainType {
    base_type_oid: OID,
    /// Attributes the domain's CHECK constraints prove NOT NULL, when the
    /// (transitive) base type is a composite. Empty for scalar domains.
    not_null_attrs: HashSet<String>,
}

/// An attribute resolved through a (possibly domain-wrapped) composite type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedAttr {
    pub type_oid: OID,
    /// True when the composite's own metadata or any wrapping domain's CHECK
    /// proves the attribute cannot be NULL.
    pub not_null: bool,
}

#[derive(Debug, Default)]
pub struct CompositeIndex {
    composites: HashMap<OID, CompositeType>,
    domains: HashMap<OID, DomainType>,
}

impl CompositeIndex {
    /// Load every composite type and domain outside the system schemas.
    pub fn new(client: &mut Client) -> anyhow::Result<Self> {
        let mut index = Self::default();

        // Composite types: standalone (`CREATE TYPE ... AS (...)`) and the row
        // types of tables/views/foreign tables. Sequences and indexes have row
        // types too, but nothing projects attributes out of those.
        let composite_rows = client
            .query(
                "SELECT t.oid AS oid, \
                        (SELECT d.description FROM pg_description d \
                          WHERE d.objoid IN (t.oid, t.typrelid) AND d.objsubid = 0 \
                          LIMIT 1) AS comment, \
                        (SELECT array_agg(a.attname::text ORDER BY a.attnum) \
                           FROM pg_attribute a \
                          WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped) AS attr_names, \
                        (SELECT array_agg(a.attnum ORDER BY a.attnum) \
                           FROM pg_attribute a \
                          WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped) AS attr_nums, \
                        (SELECT array_agg(a.atttypid ORDER BY a.attnum) \
                           FROM pg_attribute a \
                          WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped) AS attr_types, \
                        (SELECT array_agg(a.attnotnull ORDER BY a.attnum) \
                           FROM pg_attribute a \
                          WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped) AS attr_notnull, \
                        (SELECT array_agg(d.description ORDER BY a.attnum) \
                           FROM pg_attribute a \
                           LEFT JOIN pg_description d ON d.objoid = t.typrelid AND d.objsubid = a.attnum \
                          WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped) AS attr_comments \
                   FROM pg_type t \
                   JOIN pg_namespace n ON n.oid = t.typnamespace \
                   JOIN pg_class c ON c.oid = t.typrelid \
                  WHERE t.typtype = 'c' \
                    AND c.relkind IN ('c', 'r', 'p', 'v', 'm', 'f') \
                    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
                    AND n.nspname NOT LIKE 'pg_temp_%' \
                    AND n.nspname NOT LIKE 'pg_toast_temp_%'",
                &[],
            )
            .context("Composite type introspection query failed")?;

        for row in &composite_rows {
            let oid: OID = row.get("oid");
            let comment: Option<String> = row.get("comment");
            let names: Option<Vec<String>> = row.get("attr_names");
            let nums: Option<Vec<i16>> = row.get("attr_nums");
            let types: Option<Vec<OID>> = row.get("attr_types");
            let notnull: Option<Vec<bool>> = row.get("attr_notnull");
            let comments: Option<Vec<Option<String>>> = row.get("attr_comments");

            let (Some(names), Some(nums), Some(types), Some(notnull), Some(comments)) =
                (names, nums, types, notnull, comments)
            else {
                // A composite with no attributes; nothing to project.
                index.insert_composite(oid, comment.as_deref(), Vec::new());
                continue;
            };

            let attrs = names
                .into_iter()
                .zip(nums)
                .zip(types)
                .zip(notnull)
                .zip(comments)
                .map(|((((name, attnum), type_oid), attnotnull), comment)| RawAttr {
                    name,
                    attnum,
                    type_oid,
                    attnotnull,
                    comment,
                })
                .collect();

            index.insert_composite(oid, comment.as_deref(), attrs);
        }

        // Domains, with the text of their CHECK constraints. Only domains whose
        // base is (transitively) a composite can prove anything about an
        // attribute, but resolving that needs the whole map, so parse lazily
        // below once every domain is known.
        let domain_rows = client
            .query(
                "SELECT t.oid AS oid, \
                        t.typbasetype AS base_type_oid, \
                        (SELECT array_agg(pg_get_constraintdef(c.oid)) \
                           FROM pg_constraint c \
                          WHERE c.contypid = t.oid AND c.contype = 'c') AS checks \
                   FROM pg_type t \
                   JOIN pg_namespace n ON n.oid = t.typnamespace \
                  WHERE t.typtype = 'd' \
                    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
                    AND n.nspname NOT LIKE 'pg_temp_%' \
                    AND n.nspname NOT LIKE 'pg_toast_temp_%'",
                &[],
            )
            .context("Domain introspection query failed")?;

        let raw_domains: Vec<(OID, OID, Vec<String>)> = domain_rows
            .iter()
            .map(|row| {
                let checks: Option<Vec<String>> = row.get("checks");
                (
                    row.get("oid"),
                    row.get("base_type_oid"),
                    checks.unwrap_or_default(),
                )
            })
            .collect();

        // Register base types first so the CHECK parse can be skipped for
        // domains that don't wrap a composite.
        for (oid, base, _) in &raw_domains {
            index.domains.insert(
                *oid,
                DomainType {
                    base_type_oid: *base,
                    not_null_attrs: HashSet::new(),
                },
            );
        }
        for (oid, base, checks) in &raw_domains {
            if checks.is_empty() || index.composite_root(*base).is_none() {
                continue;
            }
            let check_strs: Vec<&str> = checks.iter().map(String::as_str).collect();
            index.insert_domain(*oid, *base, &check_strs);
        }

        log::info!(
            "Built composite index with {} composite types and {} domains",
            index.composites.len(),
            index.domains.len()
        );

        Ok(index)
    }

    /// An empty index — resolves nothing, so every attribute projection stays
    /// conservatively nullable. Useful for tests.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Register a composite type. `comment` is the type-level comment (bulk
    /// `@pgrpc_not_null(...)` is honored); each attribute's own comment and
    /// `attnotnull` are folded into its `not_null` bit.
    pub fn insert_composite(&mut self, oid: OID, comment: Option<&str>, attrs: Vec<RawAttr>) {
        let bulk_not_null = comment.map(annotations::parse_not_null).unwrap_or_default();

        let attrs = attrs
            .into_iter()
            .map(|a| {
                let annotated = a
                    .comment
                    .as_deref()
                    .is_some_and(annotations::has_not_null);
                CompositeAttr {
                    not_null: a.attnotnull || annotated || bulk_not_null.contains(&a.name),
                    name: a.name,
                    attnum: a.attnum,
                    type_oid: a.type_oid,
                }
            })
            .collect();

        self.composites.insert(oid, CompositeType { attrs });
    }

    /// Register a domain over `base_type_oid`, with the DDL text of its CHECK
    /// constraints (as `pg_get_constraintdef` renders them). Attributes the
    /// CHECKs prove NOT NULL — `(VALUE).attr IS NOT NULL`, possibly guarded by
    /// `VALUE IS NULL OR ...` — become NOT NULL when projected through this
    /// domain. Unparseable CHECKs contribute nothing.
    pub fn insert_domain(&mut self, oid: OID, base_type_oid: OID, checks: &[&str]) {
        let not_null_attrs = if checks.is_empty() {
            HashSet::new()
        } else {
            match non_null_cols_from_checks(checks) {
                Ok(cols) => cols,
                Err(e) => {
                    log::warn!(
                        "Could not parse CHECK constraints of domain {} for attribute nullability: {}",
                        oid,
                        e
                    );
                    HashSet::new()
                }
            }
        };

        self.domains.insert(
            oid,
            DomainType {
                base_type_oid,
                not_null_attrs,
            },
        );
    }

    /// Follow `type_oid` through any wrapping domains to the composite type
    /// underneath, returning its OID. `None` if the chain doesn't end at a
    /// known composite.
    fn composite_root(&self, type_oid: OID) -> Option<OID> {
        let mut oid = type_oid;
        for _ in 0..=MAX_DOMAIN_DEPTH {
            if self.composites.contains_key(&oid) {
                return Some(oid);
            }
            oid = self.domains.get(&oid)?.base_type_oid;
        }
        None
    }

    /// Resolve attribute `attr` of `type_oid`, where `type_oid` is a composite
    /// or a domain (possibly nested) over one.
    ///
    /// Returns `None` when the type is unknown, isn't a composite, or has no
    /// such attribute — callers should treat that as "no evidence" and stay
    /// conservative.
    pub fn attribute(&self, type_oid: OID, attr: &str) -> Option<ResolvedAttr> {
        self.resolve(type_oid, |c| c.attrs.iter().find(|a| a.name == attr))
    }

    /// Like [`Self::attribute`], but addressed by `pg_attribute.attnum` —
    /// the form a post-analysis `FieldSelect` node carries.
    pub fn attribute_by_num(&self, type_oid: OID, attnum: i32) -> Option<ResolvedAttr> {
        self.resolve(type_oid, |c| {
            c.attrs.iter().find(|a| i32::from(a.attnum) == attnum)
        })
    }

    fn resolve(
        &self,
        type_oid: OID,
        pick: impl Fn(&CompositeType) -> Option<&CompositeAttr>,
    ) -> Option<ResolvedAttr> {
        // Walk domain wrappers down to the composite itself, remembering each
        // domain so its CHECKs can be consulted once the attribute is known.
        let mut oid = type_oid;
        let mut picked: Option<&CompositeAttr> = None;
        let mut domain_chain: Vec<&DomainType> = Vec::new();

        for _ in 0..=MAX_DOMAIN_DEPTH {
            if let Some(composite) = self.composites.get(&oid) {
                picked = pick(composite);
                break;
            }
            let domain = self.domains.get(&oid)?;
            domain_chain.push(domain);
            oid = domain.base_type_oid;
        }

        let attr = picked?;
        let proven_by_domain = domain_chain
            .iter()
            .any(|d| d.not_null_attrs.contains(&attr.name));

        Some(ResolvedAttr {
            type_oid: attr.type_oid,
            not_null: attr.not_null || proven_by_domain,
        })
    }
}

/// One attribute as read from `pg_attribute`, before annotations are applied.
#[derive(Debug, Clone)]
pub struct RawAttr {
    pub name: String,
    pub attnum: i16,
    pub type_oid: OID,
    pub attnotnull: bool,
    pub comment: Option<String>,
}

impl RawAttr {
    /// An attribute without `attnotnull` — the shape every attribute of a
    /// standalone composite type has, since Postgres disallows NOT NULL there.
    pub fn new(name: &str, attnum: i16, type_oid: OID, comment: Option<&str>) -> Self {
        Self {
            name: name.to_string(),
            attnum,
            type_oid,
            attnotnull: false,
            comment: comment.map(str::to_string),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NUMERIC: OID = 1700;
    const TEXT: OID = 25;

    const DISTANCE_COMPOSITE: OID = 50_001;
    const DISTANCE_DOMAIN: OID = 50_002;
    const OUTER_DOMAIN: OID = 50_003;

    fn distance_composite(index: &mut CompositeIndex, amount_comment: Option<&str>) {
        index.insert_composite(
            DISTANCE_COMPOSITE,
            None,
            vec![
                RawAttr::new("amount", 1, NUMERIC, amount_comment),
                RawAttr::new("unit", 2, TEXT, None),
            ],
        );
    }

    #[test]
    fn attribute_annotation_marks_not_null() {
        let mut index = CompositeIndex::empty();
        distance_composite(&mut index, Some("Magnitude. @pgrpc_not_null"));

        let amount = index.attribute(DISTANCE_COMPOSITE, "amount").unwrap();
        assert_eq!(amount, ResolvedAttr { type_oid: NUMERIC, not_null: true });

        let unit = index.attribute(DISTANCE_COMPOSITE, "unit").unwrap();
        assert_eq!(unit, ResolvedAttr { type_oid: TEXT, not_null: false });
    }

    #[test]
    fn bulk_type_annotation_marks_listed_attributes() {
        let mut index = CompositeIndex::empty();
        index.insert_composite(
            DISTANCE_COMPOSITE,
            Some("A length. @pgrpc_not_null(amount, unit)"),
            vec![
                RawAttr::new("amount", 1, NUMERIC, None),
                RawAttr::new("unit", 2, TEXT, None),
                RawAttr::new("note", 3, TEXT, None),
            ],
        );

        assert!(index.attribute(DISTANCE_COMPOSITE, "amount").unwrap().not_null);
        assert!(index.attribute(DISTANCE_COMPOSITE, "unit").unwrap().not_null);
        assert!(!index.attribute(DISTANCE_COMPOSITE, "note").unwrap().not_null);
    }

    #[test]
    fn attnotnull_marks_not_null() {
        let mut index = CompositeIndex::empty();
        index.insert_composite(
            DISTANCE_COMPOSITE,
            None,
            vec![RawAttr {
                attnotnull: true,
                ..RawAttr::new("amount", 1, NUMERIC, None)
            }],
        );
        assert!(index.attribute(DISTANCE_COMPOSITE, "amount").unwrap().not_null);
    }

    #[test]
    fn domain_check_proves_attribute_through_the_domain_only() {
        let mut index = CompositeIndex::empty();
        distance_composite(&mut index, None);
        index.insert_domain(
            DISTANCE_DOMAIN,
            DISTANCE_COMPOSITE,
            &["CHECK (((VALUE IS NULL) OR (((VALUE).amount IS NOT NULL) AND ((VALUE).unit IS NOT NULL))))"],
        );

        // Through the domain: both attributes proven by the CHECK.
        assert!(index.attribute(DISTANCE_DOMAIN, "amount").unwrap().not_null);
        assert!(index.attribute(DISTANCE_DOMAIN, "unit").unwrap().not_null);

        // The bare composite carries no such promise.
        assert!(!index.attribute(DISTANCE_COMPOSITE, "amount").unwrap().not_null);
    }

    #[test]
    fn nested_domains_resolve_and_accumulate_checks() {
        let mut index = CompositeIndex::empty();
        distance_composite(&mut index, None);
        index.insert_domain(
            DISTANCE_DOMAIN,
            DISTANCE_COMPOSITE,
            &["CHECK ((VALUE).amount IS NOT NULL)"],
        );
        index.insert_domain(OUTER_DOMAIN, DISTANCE_DOMAIN, &["CHECK ((VALUE).unit IS NOT NULL)"]);

        assert!(index.attribute(OUTER_DOMAIN, "amount").unwrap().not_null);
        assert!(index.attribute(OUTER_DOMAIN, "unit").unwrap().not_null);
        // The inner domain only knows about `amount`.
        assert!(!index.attribute(DISTANCE_DOMAIN, "unit").unwrap().not_null);
    }

    #[test]
    fn unknown_type_or_attribute_is_none() {
        let mut index = CompositeIndex::empty();
        distance_composite(&mut index, None);

        assert_eq!(index.attribute(99_999, "amount"), None);
        assert_eq!(index.attribute(DISTANCE_COMPOSITE, "missing"), None);
        // A scalar domain (base type isn't a composite we know).
        index.insert_domain(OUTER_DOMAIN, TEXT, &["CHECK (VALUE <> '')"]);
        assert_eq!(index.attribute(OUTER_DOMAIN, "amount"), None);
    }

    #[test]
    fn attribute_by_num_uses_attnum_not_position() {
        let mut index = CompositeIndex::empty();
        // Attribute 2 was dropped; the survivors keep their original attnums.
        index.insert_composite(
            DISTANCE_COMPOSITE,
            None,
            vec![
                RawAttr::new("amount", 1, NUMERIC, Some("@pgrpc_not_null")),
                RawAttr::new("unit", 3, TEXT, None),
            ],
        );

        assert_eq!(
            index.attribute_by_num(DISTANCE_COMPOSITE, 3),
            Some(ResolvedAttr { type_oid: TEXT, not_null: false })
        );
        assert_eq!(index.attribute_by_num(DISTANCE_COMPOSITE, 2), None);
    }

    #[test]
    fn unparseable_check_is_ignored() {
        let mut index = CompositeIndex::empty();
        distance_composite(&mut index, None);
        index.insert_domain(DISTANCE_DOMAIN, DISTANCE_COMPOSITE, &["CHECK (this is not sql"]);

        let amount = index.attribute(DISTANCE_DOMAIN, "amount").unwrap();
        assert!(!amount.not_null);
    }
}
