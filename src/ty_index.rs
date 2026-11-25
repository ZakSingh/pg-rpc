use crate::annotations::Annotations;
use crate::codegen::OID;
use crate::ident::{sql_to_rs_string, CaseType};
use crate::pg_type::{unsupported_type, PgType};
use crate::rel_index::CheckEnumTypeInfo;
use crate::view_nullability::ViewNullabilityCache;
use anyhow::Context;
use postgres::Client;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::{Deref, DerefMut};

const TYPES_INTROSPECTION_QUERY: &str = include_str!("./queries/type_introspection.sql");

/// First OID handed to a synthetic type (a CHECK-inferred enum); later ones
/// count down from it.
///
/// Synthetic types have no catalog row, so they need OIDs that no real type
/// has. Postgres assigns OIDs upward from 16384 and only reaches the top of
/// the 32-bit range after wrapping, so the top is as good as unused; an OID
/// that the index nevertheless already holds is skipped.
const SYNTHETIC_OID_START: OID = OID::MAX;

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
    /// `type_overrides` is the `[types]` config: Postgres type name (bare, or
    /// `schema.name`) to the Rust type path to use for it.
    pub fn new(
        db: &mut Client,
        type_oids: &[OID],
        type_overrides: &HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        for (pg_name, rust) in type_overrides {
            syn::parse_str::<syn::Type>(rust).map_err(|e| {
                anyhow::anyhow!("[types] {pg_name} = \"{rust}\": not a Rust type ({e})")
            })?;
        }

        let deduped: Vec<OID> = {
            let mut s = std::collections::BTreeSet::new();
            type_oids.iter().for_each(|o| { s.insert(*o); });
            s.into_iter().collect()
        };
        let types: BTreeMap<OID, PgType> = db
            .query(TYPES_INTROSPECTION_QUERY, &[&deduped])
            .context("Type introspection query failed")?
            .into_iter()
            .map(|row| {
                let oid: OID = row.try_get("oid")?;
                let name: String = row.try_get("typname")?;
                let schema: String = row.try_get("schema_name")?;
                let mapped = type_overrides
                    .get(&format!("{schema}.{name}"))
                    .or_else(|| type_overrides.get(&name));
                if let Some(rust) = mapped {
                    return Ok((
                        oid,
                        PgType::Mapped {
                            name,
                            rust: rust.clone(),
                        },
                    ));
                }
                Ok((oid, PgType::try_from(row)?))
            })
            .collect::<Result<_, postgres::Error>>()
            .context("Failed to read a row of the type introspection query")?;

        let mut index = Self {
            types,
            check_enum_columns: HashMap::new(),
        };
        index.drop_unsupported();
        Ok(index)
    }

    /// Build an index from already-introspected types (tests).
    #[cfg(test)]
    pub fn from_types(types: BTreeMap<OID, PgType>) -> Self {
        let mut index = Self {
            types,
            check_enum_columns: HashMap::new(),
        };
        index.drop_unsupported();
        index
    }

    /// Removes the attributes of composites (tables, views, composite types)
    /// whose type has no Rust mapping, and reports them along with the
    /// domains over such types, which are not emitted either.
    ///
    /// Composites are read and written attribute by attribute, by name, so
    /// one without the attribute still decodes, and encodes it as NULL.
    /// Functions and queries that use an unmapped type directly are skipped
    /// by codegen, which reports them itself.
    fn drop_unsupported(&mut self) {
        let mut dropped: Vec<(OID, BTreeSet<String>)> = Vec::new();
        for (oid, pg_type) in &self.types {
            match pg_type {
                PgType::Composite {
                    schema,
                    name,
                    fields,
                    ..
                } => {
                    let mut names = BTreeSet::new();
                    for field in fields {
                        if let Some(type_name) = unsupported_type(field.type_oid, &self.types) {
                            log::warn!(
                                "{}.{}: column `{}` is left out, its type `{}` has no Rust mapping",
                                schema, name, field.name, type_name
                            );
                            names.insert(field.name.clone());
                        }
                    }
                    if !names.is_empty() {
                        dropped.push((*oid, names));
                    }
                }
                PgType::Domain {
                    schema,
                    name,
                    type_oid,
                    ..
                } => {
                    if let Some(type_name) = unsupported_type(*type_oid, &self.types) {
                        log::warn!(
                            "Domain {}.{} is not generated: its base type `{}` has no Rust mapping",
                            schema, name, type_name
                        );
                    }
                }
                _ => {}
            }
        }

        for (oid, names) in dropped {
            if let Some(PgType::Composite { fields, .. }) = self.types.get_mut(&oid) {
                fields.retain(|field| !names.contains(&field.name));
            }
        }
    }

    /// Create a TypeIndex with CHECK-inferred enum types.
    /// This generates synthetic OIDs for enums inferred from CHECK constraints.
    pub fn new_with_check_enums(
        db: &mut Client,
        type_oids: &[OID],
        check_enums: &[CheckEnumTypeInfo],
        type_overrides: &HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        let mut index = Self::new(db, type_oids, type_overrides)?;
        index.add_check_enums(check_enums);
        Ok(index)
    }

    /// Add CHECK-inferred enum types to the index, each under a synthetic
    /// OID (see [`SYNTHETIC_OID_START`]).
    ///
    /// The enums are emitted but no column is typed with them yet: the
    /// `(schema, table, column)` mapping recorded here is what a consumer
    /// with column provenance (which table column a view, query or function
    /// result column comes from) would look up.
    pub fn add_check_enums(&mut self, check_enums: &[CheckEnumTypeInfo]) {
        let mut next_synthetic_oid = SYNTHETIC_OID_START;

        // Rust names already taken in each schema's module, so an enum named
        // after `{table}_{column}` cannot shadow a real type of that name (or
        // another inferred enum: table `a_b` column `c`, table `a` column `b_c`).
        let mut taken: BTreeSet<(String, String)> = self
            .types
            .values()
            .filter(|t| {
                matches!(
                    t,
                    PgType::Composite { .. }
                        | PgType::Enum { .. }
                        | PgType::Domain { .. }
                        | PgType::CheckInferredEnum { .. }
                )
            })
            .map(|t| (t.schema(), sql_to_rs_string(t.name(), CaseType::Pascal)))
            .collect();

        for info in check_enums {
            // Generate enum name: {table}_{column}, then `_check`, `_check2`, …
            // until the Rust name is free.
            let base = format!("{}_{}", info.table_name, info.column_name);
            let enum_name = std::iter::once(base.clone())
                .chain(std::iter::once(format!("{base}_check")))
                .chain((2..).map(|n| format!("{base}_check{n}")))
                .find(|candidate| {
                    !taken.contains(&(
                        info.schema.clone(),
                        sql_to_rs_string(candidate, CaseType::Pascal),
                    ))
                })
                .expect("an unbounded range has an unused suffix");
            if enum_name != base {
                log::warn!(
                    "CHECK-inferred enum for {}.{}.{} is named `{}`: `{}` is already a type in that schema",
                    info.schema, info.table_name, info.column_name, enum_name, base
                );
            }
            taken.insert((
                info.schema.clone(),
                sql_to_rs_string(&enum_name, CaseType::Pascal),
            ));

            while self.types.contains_key(&next_synthetic_oid) {
                next_synthetic_oid -= 1;
            }
            let synthetic_oid = next_synthetic_oid;
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

    /// Apply view nullability from a pre-built cache
    /// This is used when the cache has already been built separately (e.g., for sharing with queries)
    pub fn apply_view_nullability_from_cache(
        &mut self,
        nullability_cache: &ViewNullabilityCache,
    ) -> anyhow::Result<()> {
        log::info!("Applying view nullability from pre-built cache");

        // Find all view types
        for pg_type in self.types.values_mut() {
            let PgType::Composite {
                fields,
                relkind: Some(kind),
                name,
                schema,
                comment,
                ..
            } = pg_type
            else {
                continue;
            };
            // Only views and materialized views
            if kind != "v" && kind != "m" {
                continue;
            }

            let Some(nullability_map) = nullability_cache.get_by_name(schema, name) else {
                log::debug!("No cached nullability found for view {}.{}", schema, name);
                continue;
            };
            log::debug!(
                "Applying cached nullability for view {}.{}: {:?}",
                schema,
                name,
                nullability_map
            );

            // Type-level `@pgrpc_not_null(a, b)`
            let type_annotations = Annotations::of(comment.as_deref());

            // Update field nullability
            for field in fields.iter_mut() {
                // The column's own `@pgrpc_not_null`, or its name in the
                // view's list
                let annotated = type_annotations
                    .child_not_null(&field.name, &Annotations::of(field.comment.as_deref()));

                // Apply annotations or inferred nullability
                if annotated {
                    // Inference already proves it: the annotation is dead
                    // weight, and hides a later change that makes it wrong.
                    if nullability_map.get(&field.name).is_some_and(|cn| cn.is_not_null()) {
                        crate::annotations::warn_redundant(
                            &format!("view {}.{}", schema, name),
                            &field.name,
                        );
                    }
                    // Annotations take precedence over inference. An outer
                    // join is the one cause of NULL the analysis can prove, so
                    // an annotation that contradicts it is almost certainly a
                    // mistake that will fail at decode time.
                    if nullability_map
                        .get(&field.name)
                        .is_some_and(|cn| cn.nullable_due_to_join)
                    {
                        log::warn!(
                            "{}.{}: @pgrpc_not_null on a column that comes from the nullable side \
                             of an outer join; rows without a match will fail to decode",
                            name,
                            field.name
                        );
                    }
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
        }

        Ok(())
    }
}
