use crate::codegen::OID;
use crate::pg_type::PgType;
use crate::rel_index::RelIndex;
use crate::view_nullability::{ViewNullabilityAnalyzer, ViewNullabilityCache};
use anyhow::Context;
use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use postgres::Client;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};

// Force recompilation with updated SQL
const TYPES_INTROSPECTION_QUERY: &'static str = include_str!("./queries/type_introspection.sql");

#[derive(Debug)]
pub struct TypeIndex(HashMap<OID, PgType>);

impl Deref for TypeIndex {
    type Target = HashMap<OID, PgType>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TypeIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TypeIndex {
    pub fn new(db: &mut Client, type_oids: &[OID]) -> anyhow::Result<Self> {
        let types = db
            .query(TYPES_INTROSPECTION_QUERY, &[&Vec::from_iter(type_oids)])
            .context("Type introspection query failed")?
            .into_iter()
            .map(|row| (row.get("oid"), PgType::try_from(row).unwrap()))
            .collect();

        Ok(Self(types))
    }

    /// Apply view nullability inference to types that represent views
    pub fn apply_view_nullability(
        &mut self,
        rel_index: &RelIndex,
        infer_nullability: bool,
    ) -> anyhow::Result<()> {
        if !infer_nullability {
            return Ok(());
        }

        log::info!("apply_view_nullability: checking {} types", self.0.len());

        // First, collect all views and their schemas
        let mut views: HashMap<(Option<String>, String), (OID, String, Vec<String>)> =
            HashMap::new();
        let mut view_oids = HashSet::new();

        for (oid, pg_type) in self.0.iter() {
            if let PgType::Composite {
                fields,
                relkind,
                view_definition,
                name,
                schema,
                ..
            } = pg_type
            {
                if let Some(ref kind) = relkind {
                    if (kind == "v" || kind == "m") && view_definition.is_some() {
                        let schema_opt = if schema == "public" {
                            None
                        } else {
                            Some(schema.clone())
                        };
                        let column_names: Vec<String> =
                            fields.iter().map(|f| f.name.clone()).collect();
                        views.insert(
                            (schema_opt.clone(), name.clone()),
                            (
                                *oid,
                                view_definition.as_ref().unwrap().clone(),
                                column_names,
                            ),
                        );
                        view_oids.insert(*oid);
                    }
                }
            }
        }

        log::info!("Found {} views to analyze", views.len());

        if views.is_empty() {
            return Ok(());
        }

        // Build dependency graph
        let mut graph = DiGraph::new();
        let mut node_map: HashMap<(Option<String>, String), NodeIndex> = HashMap::new();
        let mut node_to_view: HashMap<NodeIndex, (Option<String>, String)> = HashMap::new();

        // Add nodes
        for view_key in views.keys() {
            let node = graph.add_node(view_key.clone());
            node_map.insert(view_key.clone(), node);
            node_to_view.insert(node, view_key.clone());
        }

        // Create a temporary cache for dependency extraction
        let temp_cache = ViewNullabilityCache::new();

        // Add edges based on dependencies
        for (view_key, (_oid, view_def, _columns)) in &views {
            let analyzer = ViewNullabilityAnalyzer::new(rel_index, &temp_cache);

            // Extract dependencies, but only consider views we're actually analyzing
            if let Ok(raw_deps) = analyzer.extract_view_dependencies(view_def) {
                // Filter to only include views that exist in our set
                let deps: HashSet<_> = raw_deps
                    .into_iter()
                    .filter(|dep| views.contains_key(dep))
                    .collect();

                log::info!("View {:?} depends on: {:?}", view_key, deps);

                let from_node = node_map[view_key];
                for dep in deps {
                    if let Some(&to_node) = node_map.get(&dep) {
                        // Add edge from dependency TO dependent (reversed for topological sort)
                        graph.add_edge(to_node, from_node, ());
                    }
                }
            }
        }

        // Perform topological sort
        let sorted_nodes = match toposort(&graph, None) {
            Ok(nodes) => nodes,
            Err(_) => {
                eprintln!("Warning: Circular dependency detected in views, falling back to unordered analysis");
                node_to_view.keys().cloned().collect()
            }
        };

        // Analyze views in topological order
        let mut nullability_cache = ViewNullabilityCache::new();

        for node in sorted_nodes {
            if let Some(view_key) = node_to_view.get(&node) {
                if let Some((oid, view_def, column_names)) = views.get(view_key) {
                    log::info!("Analyzing view {:?} in topological order", view_key);

                    let mut analyzer = ViewNullabilityAnalyzer::new(rel_index, &nullability_cache);

                    match analyzer.analyze_view(view_def, column_names) {
                        Ok(nullability_map) => {
                            log::info!(
                                "View nullability analysis results for {:?}: {:?}",
                                view_key,
                                nullability_map
                            );

                            // Store in cache for dependent views
                            nullability_cache.insert(view_key.clone(), nullability_map.clone());

                            // Update the type's field nullability
                            if let Some(pg_type) = self.0.get_mut(oid) {
                                if let PgType::Composite { fields, comment, .. } = pg_type {
                                    // Parse type-level bulk not null annotations
                                    let bulk_not_null_columns = crate::pg_type::parse_bulk_not_null_columns(comment);
                                    
                                    for field in fields.iter_mut() {
                                        // Check for column-level @pgrpc_not_null annotation (highest priority)
                                        let has_column_annotation = field.comment
                                            .as_ref()
                                            .is_some_and(|c| c.contains("@pgrpc_not_null"));
                                        
                                        // Check for type-level bulk annotation
                                        let has_bulk_annotation = bulk_not_null_columns.contains(&field.name);
                                        
                                        // Apply annotations or inferred nullability
                                        if has_column_annotation || has_bulk_annotation {
                                            // Annotations take precedence over inference
                                            if field.nullable {
                                                log::info!(
                                                    "Updating field {} to NOT NULL due to annotation",
                                                    field.name
                                                );
                                                field.nullable = false;
                                            }
                                        } else if let Some(&is_not_null) = nullability_map.get(&field.name) {
                                            // Use inferred nullability if no annotations
                                            if is_not_null && field.nullable {
                                                log::info!(
                                                    "Updating field {} to NOT NULL based on inference",
                                                    field.name
                                                );
                                                field.nullable = false;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "Warning: Failed to analyze view nullability for {:?}: {}",
                                view_key, e
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
