//! In-memory map of domain type names to their OIDs.
//!
//! A cast to a domain (`expr::currency`) is reported by prepared-statement
//! metadata as the domain's *base* type, so query introspection needs to map a
//! cast's target type name back to the domain OID to preserve the domain
//! wrapper (matching `pg_typeof`). Loading every domain once (one cheap catalog
//! query, no correlated subqueries) lets the introspector resolve those casts
//! in-memory, without a per-query round-trip.
//!
//! Scope is *all non-system schemas*, not the codegen schema list: a query can
//! cast to `public.currency` (or an unqualified `currency` resolved through the
//! search path) even when only `api`/`test` code is being generated. Bare
//! (unqualified) names resolve through the connection's effective search path,
//! matching how PostgreSQL itself resolves an unqualified type name.

use crate::codegen::OID;
use anyhow::Context;
use postgres::Client;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct DomainIndex {
    /// Keyed by both the bare name (`currency`) and the schema-qualified name
    /// (`public.currency`) so either form from a cast's `TypeName` resolves.
    /// The bare key maps to the domain the *search path* would pick (first
    /// matching schema in search-path order); qualified keys are always present.
    by_name: HashMap<String, OID>,
}

impl DomainIndex {
    /// Load every domain in all non-system schemas, resolving bare names
    /// through the connection's effective search path.
    pub fn new(client: &mut Client) -> anyhow::Result<Self> {
        // Effective search path, in resolution order. `current_schemas(true)`
        // includes implicitly-searched schemas (e.g. pg_catalog) and reflects
        // any `SET search_path` on the connection — exactly what PostgreSQL
        // uses to resolve an unqualified type name.
        let search_path: Vec<String> = client
            .query_one("SELECT current_schemas(true) AS path", &[])
            .context("Failed to read search_path")?
            .get("path");

        // All domains outside the system schemas. A cast may target a domain in
        // any reachable schema, independent of which schemas we generate code
        // for, so this is deliberately *not* scoped to the codegen schema list.
        let rows = client
            .query(
                "SELECT t.oid AS oid, n.nspname AS schema, t.typname AS name \
                 FROM pg_type t \
                 JOIN pg_namespace n ON n.oid = t.typnamespace \
                 WHERE t.typtype = 'd' \
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
                   AND n.nspname NOT LIKE 'pg_temp_%' \
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'",
                &[],
            )
            .context("Domain introspection query failed")?;

        // schema -> (bare name -> oid), so bare-name resolution can consult
        // schemas in search-path order.
        let mut by_schema: HashMap<String, HashMap<String, OID>> = HashMap::new();
        let mut by_name: HashMap<String, OID> = HashMap::new();

        for row in &rows {
            let oid: OID = row.get("oid");
            let schema: String = row.get("schema");
            let name: String = row.get("name");

            // Qualified key is always unambiguous.
            by_name.insert(format!("{}.{}", schema, name), oid);
            by_schema.entry(schema).or_default().insert(name, oid);
        }

        // Bare name resolves to the domain in the earliest search-path schema
        // that defines it — mirroring PostgreSQL's own name resolution. A schema
        // on the search path that doesn't exist (or has no domains) is simply
        // skipped. Later schemas never override an earlier match.
        for schema in &search_path {
            if let Some(domains) = by_schema.get(schema) {
                for (name, &oid) in domains {
                    by_name.entry(name.clone()).or_insert(oid);
                }
            }
        }

        Ok(Self { by_name })
    }

    /// An empty index — resolves no domains. Useful for tests and for builds
    /// with no query introspection.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Look up a domain OID by a (possibly schema-qualified) type name.
    pub fn get(&self, type_name: &str) -> Option<OID> {
        self.by_name.get(type_name).copied()
    }
}
