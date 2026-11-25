use crate::codegen::OID;
use crate::config::QueriesConfig;
use crate::query_introspector::{IntrospectedQuery, QueryIntrospector};
use crate::rel_index::RelIndex;
use crate::sql_parser::{ParsedQuery, SqlParser};
use crate::trigger_index::TriggerIndex;
use anyhow::Result;
use postgres::{Client, NoTls};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::Deref;

#[derive(Debug, Clone, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct QueryId {
    pub name: String,
}

#[derive(Debug)]
pub struct QueryIndex {
    queries: BTreeMap<QueryId, IntrospectedQuery>,
}

impl Deref for QueryIndex {
    type Target = BTreeMap<QueryId, IntrospectedQuery>;

    fn deref(&self) -> &Self::Target {
        &self.queries
    }
}

impl QueryIndex {
    /// Create a new QueryIndex by parsing and introspecting SQL files
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connection_string: &str,
        rel_index: &RelIndex,
        view_nullability_cache: &crate::view_nullability::ViewNullabilityCache,
        config: &QueriesConfig,
        trigger_index: Option<&TriggerIndex>,
        domain_index: &crate::domain_index::DomainIndex,
        composite_index: &crate::composite_index::CompositeIndex,
        fn_index: Option<&crate::fn_index::FunctionIndex>,
        errors_config: Option<&crate::config::ErrorsConfig>,
    ) -> Result<Self> {
        let parser = SqlParser::new();

        // Parse all SQL files
        let parsed_queries = parser.parse_files(&config.paths)?;
        reject_duplicate_names(&parsed_queries)?;

        let introspector = QueryIntrospector::new(
            rel_index,
            view_nullability_cache,
            trigger_index,
            domain_index,
            composite_index,
            fn_index,
            errors_config,
        );

        // Introspect queries in parallel using rayon with thread-local DB connections
        use rayon::prelude::*;

        let conn_str = connection_string.to_string();

        // Cache one client per rayon worker, keyed by the connection string so that
        // back-to-back QueryIndex::new calls with *different* targets (most commonly
        // the integration test suite, where each test gets its own database) don't
        // reuse a client pointed at the wrong database.
        thread_local! {
            static THREAD_CLIENT: RefCell<Option<(String, Client)>> = const { RefCell::new(None) };
        }

        let results: Vec<Result<(QueryId, IntrospectedQuery)>> = parsed_queries
            .par_iter()
            .map(|parsed| {
                THREAD_CLIENT.with(|cell| {
                    let mut borrow = cell.borrow_mut();
                    let needs_new = match borrow.as_ref() {
                        Some((cached_conn, _)) => cached_conn != &conn_str,
                        None => true,
                    };
                    if needs_new {
                        let mut c = Client::connect(&conn_str, NoTls)?;
                        c.execute("SET jit = off", &[])?;
                        *borrow = Some((conn_str.clone(), c));
                    }
                    let (_, client) = borrow.as_mut().unwrap();

                    let introspected = introspector.introspect(client, parsed)?;
                    let id = QueryId {
                        name: introspected.name.clone(),
                    };
                    Ok((id, introspected))
                })
            })
            .collect();

        let mut queries = BTreeMap::new();
        for result in results {
            let (id, introspected) = result?;
            queries.insert(id, introspected);
        }

        reject_duplicate_columns(&queries)?;

        Ok(Self { queries })
    }

    /// Get all type OIDs used by queries
    pub fn get_type_oids(&self) -> Vec<OID> {
        let mut oids = Vec::new();

        for query in self.queries.values() {
            // Add parameter types
            for param in &query.params {
                oids.push(param.type_oid);
            }

            // Add return column types
            if let Some(columns) = &query.return_columns {
                for col in columns {
                    oids.push(col.type_oid);
                }
            }
        }

        oids
    }
}

/// Fail the build if two queries share a name.
///
/// Each query becomes one Rust function named after it, so a second query of
/// the same name would silently replace the first. Every clash is reported at
/// once, with where each definition lives.
fn reject_duplicate_names(queries: &[ParsedQuery]) -> Result<()> {
    let mut by_name: BTreeMap<&str, Vec<&ParsedQuery>> = BTreeMap::new();
    for query in queries {
        by_name.entry(query.name.as_str()).or_default().push(query);
    }

    let problems: Vec<String> = by_name
        .iter()
        .filter(|(_, definitions)| definitions.len() > 1)
        .map(|(name, definitions)| {
            let mut places: Vec<String> = definitions
                .iter()
                .map(|q| format!("{}:{}", q.file_path.display(), q.line_number))
                .collect();
            places.sort();
            format!("  {}: {}", name, places.join(", "))
        })
        .collect();

    if problems.is_empty() {
        return Ok(());
    }
    anyhow::bail!(
        "query names must be unique, but these are defined more than once:\n{}",
        problems.join("\n")
    )
}

/// Fail the build if any query returns two columns with the same name.
///
/// Row structs locate their columns by name, so that a column added later
/// (through a `*` over a table or view that gained one) cannot shift the
/// others. Two columns with one name cannot be told apart that way, and
/// would be two struct fields with one name besides.
///
/// Every offending query is reported at once.
fn reject_duplicate_columns(queries: &BTreeMap<QueryId, IntrospectedQuery>) -> Result<()> {
    let problems: Vec<String> = queries
        .values()
        .filter_map(|q| {
            let duplicates = duplicate_names(
                q.return_columns.iter().flatten().map(|c| c.name.as_str()),
            );
            (!duplicates.is_empty()).then(|| {
                format!(
                    "  {} ({}:{}): {}",
                    q.name,
                    q.file_path,
                    q.line_number,
                    duplicates.join(", ")
                )
            })
        })
        .collect();

    if problems.is_empty() {
        return Ok(());
    }
    anyhow::bail!(
        "query result columns must have distinct names:\n{}\n\
         Row structs find their columns by name. Alias the duplicates with `as`.",
        problems.join("\n")
    )
}

/// Names that occur more than once, each listed once, in first-seen order.
fn duplicate_names<'a>(names: impl Iterator<Item = &'a str>) -> Vec<&'a str> {
    let mut seen: Vec<&str> = Vec::new();
    let mut duplicates: Vec<&str> = Vec::new();
    for name in names {
        if seen.contains(&name) {
            if !duplicates.contains(&name) {
                duplicates.push(name);
            }
        } else {
            seen.push(name);
        }
    }
    duplicates
}

#[cfg(test)]
mod tests {
    use super::{duplicate_names, reject_duplicate_names};
    use crate::sql_parser::SqlParser;
    use std::path::PathBuf;

    #[test]
    fn duplicate_query_names_are_an_error_naming_both_files() {
        let parser = SqlParser::new();
        let mut queries = parser
            .parse_content(
                "-- name: GetUser :one\nSELECT 1;\n-- name: ListUsers :many\nSELECT 2;\n",
                PathBuf::from("queries/users.sql"),
            )
            .unwrap();
        assert!(reject_duplicate_names(&queries).is_ok());

        queries.extend(
            parser
                .parse_content(
                    "\n-- name: GetUser :opt\nSELECT 3;\n",
                    PathBuf::from("queries/admin.sql"),
                )
                .unwrap(),
        );
        let err = reject_duplicate_names(&queries).unwrap_err().to_string();
        assert!(err.contains("GetUser"), "{err}");
        assert!(err.contains("queries/users.sql:1"), "{err}");
        assert!(err.contains("queries/admin.sql:2"), "{err}");
        assert!(!err.contains("ListUsers"), "{err}");
    }

    #[test]
    fn duplicates_are_listed_once_in_first_seen_order() {
        assert!(duplicate_names(["id", "name", "owner__id"].into_iter()).is_empty());
        assert_eq!(
            duplicate_names(["id", "b", "id", "a", "b", "id"].into_iter()),
            vec!["id", "b"]
        );
    }
}
