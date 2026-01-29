use crate::codegen::OID;
use crate::config::QueriesConfig;
use crate::query_introspector::{IntrospectedQuery, QueryIntrospector};
use crate::rel_index::RelIndex;
use crate::sql_parser::{SqlParser, QueryType};
use crate::trigger_index::TriggerIndex;
use crate::ty_index::TypeIndex;
use anyhow::Result;
use postgres::Client;
use std::collections::HashMap;
use std::ops::Deref;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct QueryId {
    pub name: String,
}

#[derive(Debug)]
pub struct QueryIndex {
    queries: HashMap<QueryId, IntrospectedQuery>,
}

impl Deref for QueryIndex {
    type Target = HashMap<QueryId, IntrospectedQuery>;

    fn deref(&self) -> &Self::Target {
        &self.queries
    }
}

impl QueryIndex {
    /// Create a new QueryIndex by parsing and introspecting SQL files
    pub fn new(
        client: &mut Client,
        rel_index: &RelIndex,
        type_index: &TypeIndex,
        view_nullability_cache: &crate::view_nullability::ViewNullabilityCache,
        config: &QueriesConfig,
        trigger_index: Option<&TriggerIndex>,
    ) -> Result<Self> {
        let parser = SqlParser::new();

        // Parse all SQL files
        let parsed_queries = parser.parse_files(&config.paths)?;

        log::info!("Found {} queries to introspect", parsed_queries.len());

        // Introspect each query
        let mut introspector =
            QueryIntrospector::new(client, rel_index, type_index, view_nullability_cache, trigger_index);

        let mut queries = HashMap::new();

        for parsed in parsed_queries {
            let introspected = introspector.introspect(&parsed)?;
            let id = QueryId {
                name: introspected.name.clone(),
            };

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
