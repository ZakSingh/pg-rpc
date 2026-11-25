use crate::codegen::OID;
use crate::config::QueriesConfig;
use crate::query_introspector::{IntrospectedQuery, QueryIntrospector};
use crate::rel_index::RelIndex;
use crate::sql_parser::{QueryType, SqlParser};
use crate::trigger_index::TriggerIndex;
use anyhow::Result;
use postgres::{Client, NoTls};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
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
            if queries.contains_key(&id) {
                log::warn!(
                    "Duplicate query name '{}'. Previous definition will be overwritten.",
                    id.name
                );
            }
            queries.insert(id, introspected);
        }

        reject_duplicate_columns(&queries)?;

        Ok(Self { queries })
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
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

    /// Group queries by their query type for organized code generation
    pub fn group_by_type(&self) -> HashMap<QueryType, Vec<&IntrospectedQuery>> {
        let mut grouped: HashMap<QueryType, Vec<&IntrospectedQuery>> = HashMap::new();

        for query in self.queries.values() {
            grouped
                .entry(query.query_type.clone())
                .or_insert_with(Vec::new)
                .push(query);
        }

        grouped
    }
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
    use super::duplicate_names;

    #[test]
    fn duplicates_are_listed_once_in_first_seen_order() {
        assert!(duplicate_names(["id", "name", "owner__id"].into_iter()).is_empty());
        assert_eq!(
            duplicate_names(["id", "b", "id", "a", "b", "id"].into_iter()),
            vec!["id", "b"]
        );
    }
}
