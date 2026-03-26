use crate::annotations;
use crate::codegen::OID;
use crate::pg_type::PgType;
use crate::rel_index::{CheckEnumTypeInfo, RelIndex};
use crate::view_nullability::{ViewNullabilityAnalyzer, ViewNullabilityCache};
use anyhow::Context;
use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use postgres::Client;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::{Deref, DerefMut};

// Force recompilation with updated SQL
const TYPES_INTROSPECTION_QUERY: &'static str = include_str!("./queries/type_introspection.sql");

/// Starting OID for synthetic types (CHECK-inferred enums).
/// Uses negative numbers to avoid collision with real PostgreSQL OIDs.
const SYNTHETIC_OID_START: i64 = -1;

/// Mapping from (schema, table, column) to synthetic OID for CHECK-inferred enums
pub type CheckEnumColumnMapping = HashMap<(String, String, String), OID>;

#[derive(Debug)]
pub struct TypeIndex {
    types: BTreeMap<OID, PgType>,
    /// Mapping from (schema, table, column) to synthetic OID for CHECK-inferred enums
    check_enum_columns: CheckEnumColumnMapping,
}

impl Deref for TypeIndex {
    type Target = BTreeMap<OID, PgType>;

    fn deref(&self) -> &Self::Target {
        &self.types
    }
}

impl DerefMut for TypeIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.types
    }
}

impl TypeIndex {
    pub fn new(db: &mut Client, type_oids: &[OID]) -> anyhow::Result<Self> {
        let deduped: Vec<OID> = {
            let mut s = std::collections::BTreeSet::new();
            type_oids.iter().for_each(|o| { s.insert(*o); });
            s.into_iter().collect()
        };
        println!("cargo:warning=[pgrpc timing]   ty_index: {} input oids, {} unique", type_oids.len(), deduped.len());
        println!("cargo:warning=[pgrpc timing]   ty_index oids: {:?}", &deduped);
        let types: BTreeMap<OID, PgType> = db
            .query(TYPES_INTROSPECTION_QUERY, &[&deduped])
            .context("Type introspection query failed")?
            .into_iter()
            .map(|row| (row.get("oid"), PgType::try_from(row).unwrap()))
            .collect();

        Ok(Self {
            types,
            check_enum_columns: HashMap::new(),
        })
    }

    /// Create a TypeIndex with CHECK-inferred enum types.
    /// This generates synthetic OIDs for enums inferred from CHECK constraints.
    pub fn new_with_check_enums(
        db: &mut Client,
        type_oids: &[OID],
        check_enums: &[CheckEnumTypeInfo],
    ) -> anyhow::Result<Self> {
        let mut index = Self::new(db, type_oids)?;
        index.add_check_enums(check_enums);
        Ok(index)
    }

    /// Add CHECK-inferred enum types to the index.
    /// Generates synthetic OIDs (negative numbers) for each enum type.
    pub fn add_check_enums(&mut self, check_enums: &[CheckEnumTypeInfo]) {
        let mut next_synthetic_oid = SYNTHETIC_OID_START;

        for info in check_enums {
            // Generate enum name: {table}_{column}
            let enum_name = format!("{}_{}", info.table_name, info.column_name);

            // Use negative OID to avoid collision with real PostgreSQL OIDs
            let synthetic_oid = next_synthetic_oid as OID;
            next_synthetic_oid -= 1;

            log::info!(
                "Creating CHECK-inferred enum {}.{} (OID: {}) from constraint {} with variants: {:?}",
                info.schema,
                enum_name,
                synthetic_oid,
                info.constraint_name,
                info.variants
            );

            // Create the enum type
            let pg_type = PgType::CheckInferredEnum {
                schema: info.schema.clone(),
                name: enum_name,
                variants: info.variants.clone(),
                constraint_name: info.constraint_name.clone(),
            };

            // Insert into type map
            self.types.insert(synthetic_oid, pg_type);

            // Create column mapping
            let key = (
                info.schema.clone(),
                info.table_name.clone(),
                info.column_name.clone(),
            );
            self.check_enum_columns.insert(key, synthetic_oid);
        }
    }

    /// Get the CHECK-inferred enum OID for a column, if any.
    pub fn get_check_enum_oid(
        &self,
        schema: &str,
        table: &str,
        column: &str,
    ) -> Option<OID> {
        self.check_enum_columns
            .get(&(schema.to_string(), table.to_string(), column.to_string()))
            .copied()
    }

    /// Get the column mapping for CHECK-inferred enums
    pub fn check_enum_columns(&self) -> &CheckEnumColumnMapping {
        &self.check_enum_columns
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

        log::info!("apply_view_nullability: checking {} types", self.types.len());

        // First, collect all views and their schemas
        let mut views: HashMap<(Option<String>, String), (OID, String, Vec<String>)> =
            HashMap::new();
        let mut view_oids = HashSet::new();

        for (oid, pg_type) in self.types.iter() {
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
                            if let Some(pg_type) = self.types.get_mut(oid) {
                                if let PgType::Composite { fields, comment, .. } = pg_type {
                                    // Parse type-level bulk not null annotations
                                    let bulk_not_null_columns = crate::pg_type::parse_bulk_not_null_columns(comment);

                                    for field in fields.iter_mut() {
                                        // Check for column-level @pgrpc_not_null annotation (highest priority)
                                        let has_column_annotation = field.comment
                                            .as_ref()
                                            .is_some_and(|c| annotations::has_not_null(c));

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
                                        } else if let Some(cn) = nullability_map.get(&field.name) {
                                            // Use inferred nullability if no annotations
                                            if cn.is_not_null() && field.nullable {
                                                log::info!(
                                                    "Updating field {} to NOT NULL based on inference",
                                                    field.name
                                                );
                                                field.nullable = false;
                                            } else {
                                                field.nullable = cn.is_nullable();
                                            }
                                            field.nullable_due_to_join = cn.nullable_due_to_join;
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

    /// Apply view nullability from a pre-built cache
    /// This is used when the cache has already been built separately (e.g., for sharing with queries)
    pub fn apply_view_nullability_from_cache(
        &mut self,
        nullability_cache: &ViewNullabilityCache,
    ) -> anyhow::Result<()> {
        log::info!("Applying view nullability from pre-built cache");

        // Find all view types
        for (oid, pg_type) in self.types.iter_mut() {
            if let PgType::Composite {
                fields,
                relkind,
                name,
                schema,
                comment,
                ..
            } = pg_type
            {
                if let Some(ref kind) = relkind {
                    if kind == "v" || kind == "m" {
                        // This is a view or materialized view
                        let schema_opt = if schema == "public" {
                            None
                        } else {
                            Some(schema.clone())
                        };

                        let view_key = (schema_opt, name.clone());

                        // Look up in cache
                        if let Some(nullability_map) = nullability_cache.get(&view_key) {
                            log::info!(
                                "Applying cached nullability for view {:?}: {:?}",
                                view_key,
                                nullability_map
                            );

                            // Parse type-level bulk not null annotations
                            let bulk_not_null_columns = crate::pg_type::parse_bulk_not_null_columns(comment);

                            // Update field nullability
                            for field in fields.iter_mut() {
                                // Check for column-level @pgrpc_not_null annotation (highest priority)
                                let has_column_annotation = field
                                    .comment
                                    .as_ref()
                                    .is_some_and(|c| annotations::has_not_null(c));

                                // Check for type-level bulk annotation
                                let has_bulk_annotation = bulk_not_null_columns.contains(&field.name);

                                // Apply annotations or inferred nullability
                                if has_column_annotation || has_bulk_annotation {
                                    // Annotations take precedence over inference
                                    if field.nullable {
                                        log::info!(
                                            "Updating field {}.{} to NOT NULL due to annotation",
                                            name,
                                            field.name
                                        );
                                        field.nullable = false;
                                    }
                                } else if let Some(cn) = nullability_map.get(&field.name) {
                                    // Use inferred nullability if no annotations
                                    if cn.is_not_null() && field.nullable {
                                        log::info!(
                                            "Updating field {}.{} to NOT NULL based on cached inference",
                                            name,
                                            field.name
                                        );
                                        field.nullable = false;
                                    } else {
                                        // Preserve the overall nullable status
                                        field.nullable = cn.is_nullable();
                                    }
                                    // Always propagate join-nullability info
                                    field.nullable_due_to_join = cn.nullable_due_to_join;
                                }
                            }
                        } else {
                            log::debug!("No cached nullability found for view {:?}", view_key);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
