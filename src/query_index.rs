use crate::codegen::OID;
use crate::config::QueriesConfig;
use crate::query_introspector::{IntrospectedQuery, QueryIntrospector};
use crate::rel_index::RelIndex;
use crate::sql_parser::{SqlParser, QueryType};
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
    ) -> Result<Self> {
        let parser = SqlParser::new();

        // Parse all SQL files
        let parsed_queries = parser.parse_files(&config.paths)?;

        let introspector = QueryIntrospector::new(rel_index, view_nullability_cache, trigger_index);

        // Introspect queries in parallel using rayon with thread-local DB connections
        use rayon::prelude::*;

        let conn_str = connection_string.to_string();

        thread_local! {
            static THREAD_CLIENT: RefCell<Option<Client>> = const { RefCell::new(None) };
        }

        let results: Vec<Result<(QueryId, IntrospectedQuery)>> = parsed_queries
            .par_iter()
            .map(|parsed| {
                THREAD_CLIENT.with(|cell| {
                    let mut borrow = cell.borrow_mut();
                    let client = match borrow.as_mut() {
                        Some(c) => c,
                        None => {
                            let mut c = Client::connect(&conn_str, NoTls)?;
                            c.execute("SET jit = off", &[])?;
                            *borrow = Some(c);
                            borrow.as_mut().unwrap()
                        }
                    };

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
