//! In-memory map of domain type names to their OIDs.
//!
//! A cast to a domain (`expr::currency`) is reported by prepared-statement
//! metadata as the domain's *base* type, so query introspection needs to map a
//! cast's target type name back to the domain OID to preserve the domain
//! wrapper (matching `pg_typeof`). Loading every domain once (one cheap catalog
//! query, no correlated subqueries) lets the introspector resolve those casts
//! in-memory, without a per-query round-trip.

use crate::codegen::OID;
use anyhow::Context;
use postgres::Client;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct DomainIndex {
    /// Keyed by both the bare name (`currency`) and the schema-qualified name
    /// (`public.currency`) so either form from a cast's `TypeName` resolves.
    /// When two schemas define a domain with the same bare name, the bare key
    /// is ambiguous and is left out — only the qualified keys are kept for those.
    by_name: HashMap<String, OID>,
}

impl DomainIndex {
    /// Load all domains visible in the given schemas with a single query.
    pub fn new(client: &mut Client, schemas: &[String]) -> anyhow::Result<Self> {
        let rows = client
            .query(
                "SELECT t.oid AS oid, n.nspname AS schema, t.typname AS name \
                 FROM pg_type t \
                 JOIN pg_namespace n ON n.oid = t.typnamespace \
                 WHERE t.typtype = 'd' AND n.nspname = ANY($1)",
                &[&schemas],
            )
            .context("Domain introspection query failed")?;

        let mut by_name: HashMap<String, OID> = HashMap::new();
        // Track bare names that are defined in more than one schema; their bare
        // key is ambiguous, so we must not let one schema's domain shadow another.
        let mut ambiguous_bare: std::collections::HashSet<String> = std::collections::HashSet::new();

        for row in rows {
            let oid: OID = row.get("oid");
            let schema: String = row.get("schema");
            let name: String = row.get("name");

            by_name.insert(format!("{}.{}", schema, name), oid);

            if ambiguous_bare.contains(&name) {
                continue;
            }
            match by_name.insert(name.clone(), oid) {
                Some(prev) if prev != oid => {
                    // Same bare name, different domains in different schemas:
                    // remove the bare key and remember it's ambiguous.
                    by_name.remove(&name);
                    ambiguous_bare.insert(name);
                }
                _ => {}
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
