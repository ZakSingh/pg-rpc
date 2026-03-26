use crate::codegen::OID;
use crate::config::QueriesConfig;
use crate::query_introspector::{IntrospectedQuery, QueryIntrospector};
use crate::rel_index::RelIndex;
use crate::sql_parser::{SqlParser, QueryType};
use crate::trigger_index::TriggerIndex;
use anyhow::Result;
use postgres::Client;
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
        client: &mut Client,
        rel_index: &RelIndex,
        view_nullability_cache: &crate::view_nullability::ViewNullabilityCache,
        config: &QueriesConfig,
        trigger_index: Option<&TriggerIndex>,
    ) -> Result<Self> {
        let parser = SqlParser::new();

        // Parse all SQL files
        let t = std::time::Instant::now();
        let parsed_queries = parser.parse_files(&config.paths)?;
        println!("cargo:warning=[pgrpc timing]   sql_parse: {:.2?} ({} queries)", t.elapsed(), parsed_queries.len());

        // Introspect each query
        let mut introspector =
            QueryIntrospector::new(client, rel_index, view_nullability_cache, trigger_index);

        let mut queries = BTreeMap::new();
        let mut total_prepare = std::time::Duration::ZERO;
        let mut total_analysis = std::time::Duration::ZERO;

        for parsed in parsed_queries {
            let t = std::time::Instant::now();
            let introspected = introspector.introspect(&parsed)?;
            let elapsed = t.elapsed();
            if elapsed > std::time::Duration::from_millis(100) {
                println!("cargo:warning=[pgrpc timing]   slow query '{}': {:.2?}", parsed.name, elapsed);
            }
            total_prepare += elapsed;
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

        println!("cargo:warning=[pgrpc timing]   total introspect: {:.2?} for {} queries", total_prepare, queries.len());
        println!("cargo:warning=[pgrpc timing]     cardinality: {:.2?}", introspector._t_cardinality);
        println!("cargo:warning=[pgrpc timing]     prepare: {:.2?}", introspector._t_prepare);
        println!("cargo:warning=[pgrpc timing]     param_info: {:.2?}", introspector._t_param_info);
        println!("cargo:warning=[pgrpc timing]     return_cols: {:.2?}", introspector._t_return_cols);
        println!("cargo:warning=[pgrpc timing]     constraints: {:.2?}", introspector._t_constraints);
        println!("cargo:warning=[pgrpc timing]     pg_query::parse: {:.2?} ({} calls)", introspector._t_pg_parse.get(), introspector._n_pg_parse.get());
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
