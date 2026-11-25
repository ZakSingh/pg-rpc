use crate::annotations::{self, Annotations, Site};
use crate::codegen::OID;
use anyhow::Context;
use postgres::{Client, NoTls};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeSet;

pub struct Db {
    pub client: Client,
}

impl Db {
    pub fn new(connection_string: &str) -> anyhow::Result<Self> {
        let mut client = Client::connect(connection_string, NoTls)?;
        // Disable JIT for introspection queries — the compilation overhead
        // far exceeds any execution benefit for these short-lived queries.
        client.execute("SET jit = off", &[])?;

        Ok(Db { client })
    }
}

const COMPOSITE_SCHEMA_INTROSPECTION_QUERY: &str =
    include_str!("./queries/composite_schema_introspection.sql");

/// A standalone composite type (`create type ... as (...)`) from a schema
/// whose composites describe payloads: task types, error types.
#[derive(Debug, Clone)]
pub struct SchemaComposite {
    pub name: String,
    pub type_oid: OID,
    /// `comment on type`, carrying `@pgrpc_not_null(a, b)` and, for error
    /// types, `@pgrpc_message` / `@pgrpc_field`.
    pub comment: Option<String>,
    /// Attributes in declaration order. May be empty (`create type t as ()`).
    pub fields: Vec<CompositeField>,
}

/// One attribute of a [`SchemaComposite`].
#[derive(Debug, Clone, Deserialize)]
pub struct CompositeField {
    pub name: String,
    pub type_oid: OID,
    /// `format_type` rendering, for diagnostics.
    pub postgres_type: String,
    /// `comment on column`, carrying `@pgrpc_not_null`.
    pub comment: Option<String>,
}

impl SchemaComposite {
    /// Everything wrong with the annotations on this payload type and its
    /// fields, each prefixed with where it is. `message`: the type may carry
    /// `@pgrpc_message` / `@pgrpc_field` (error payloads).
    pub fn annotation_problems(&self, schema: &str, message: bool) -> Vec<String> {
        let children: Vec<&str> = self.fields.iter().map(|f| f.name.as_str()).collect();
        let mut problems = Vec::new();
        if let Some(comment) = &self.comment {
            let site = Site::Type { children: &children, all: true, message };
            for problem in annotations::check(comment, site) {
                problems.push(format!("type {}.{}: {}", schema, self.name, problem));
            }
        }
        for field in &self.fields {
            if let Some(comment) = &field.comment {
                for problem in annotations::check(comment, Site::Column) {
                    problems.push(format!(
                        "column {}.{}.{}: {}",
                        schema, self.name, field.name, problem
                    ));
                }
            }
        }
        problems
    }

    /// Whether a field's generated Rust type is an `Option`.
    ///
    /// Postgres cannot declare a composite attribute `NOT NULL`
    /// (`attnotnull` is always false for them), so every field is nullable
    /// unless annotated: `@pgrpc_not_null` in the field's own comment, the
    /// field named in `@pgrpc_not_null(a, b)` in the type's comment, or
    /// `@pgrpc_not_null(*)` there (less any `@pgrpc_nullable` exceptions).
    pub fn is_nullable(&self, field: &CompositeField) -> bool {
        !Annotations::of(self.comment.as_deref())
            .child_not_null(&field.name, &Annotations::of(field.comment.as_deref()))
    }
}

/// Fail on the first payload type in `schema` with a bad annotation,
/// listing every problem found.
pub fn check_schema_composites(
    types: &[SchemaComposite],
    schema: &str,
    message: bool,
) -> anyhow::Result<()> {
    let problems: Vec<String> = types
        .iter()
        .flat_map(|t| t.annotation_problems(schema, message))
        .collect();
    annotations::fail_on(problems)
}

/// Parse the `fields` JSON array the introspection query builds. A field
/// missing a key is an error, not a silently shorter struct.
fn parse_fields(fields: Value) -> serde_json::Result<Vec<CompositeField>> {
    if fields.is_null() {
        return Ok(Vec::new());
    }
    serde_json::from_value(fields)
}

/// Every standalone composite type in `schema`, ordered by name, with its
/// fields. Row types of tables and views in that schema are not included.
pub fn load_schema_composites(db: &mut Client, schema: &str) -> anyhow::Result<Vec<SchemaComposite>> {
    db.query(COMPOSITE_SCHEMA_INTROSPECTION_QUERY, &[&schema])
        .with_context(|| format!("Composite type introspection of schema '{schema}' failed"))?
        .into_iter()
        .map(|row| {
            let name: String = row.try_get("name")?;
            let fields = parse_fields(row.try_get("fields")?)
                .with_context(|| format!("Malformed field list for type {schema}.{name}"))?;
            Ok(SchemaComposite {
                type_oid: row.try_get("type_oid")?,
                comment: row.try_get("comment")?,
                name,
                fields,
            })
        })
        .collect()
}

/// The OIDs of the given composites and of every type their fields use,
/// deduplicated and sorted.
pub fn composite_type_oids(composites: &[SchemaComposite]) -> Vec<OID> {
    composites
        .iter()
        .flat_map(|c| std::iter::once(c.type_oid).chain(c.fields.iter().map(|f| f.type_oid)))
        .collect::<BTreeSet<OID>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// A field of type `type_oid` with an optional comment.
    pub fn field(name: &str, type_oid: OID, comment: Option<&str>) -> CompositeField {
        CompositeField {
            name: name.to_string(),
            type_oid,
            postgres_type: format!("oid {type_oid}"),
            comment: comment.map(str::to_string),
        }
    }

    pub fn composite(
        name: &str,
        type_oid: OID,
        comment: Option<&str>,
        fields: Vec<CompositeField>,
    ) -> SchemaComposite {
        SchemaComposite {
            name: name.to_string(),
            type_oid,
            comment: comment.map(str::to_string),
            fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use serde_json::json;

    #[test]
    fn fields_parse_in_order() {
        let fields = parse_fields(json!([
            {"name": "email", "type_oid": 1001, "postgres_type": "email", "comment": null},
            {"name": "code", "type_oid": 25, "postgres_type": "text", "comment": "@pgrpc_not_null"}
        ]))
        .unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!((fields[0].name.as_str(), fields[0].type_oid), ("email", 1001));
        assert_eq!(fields[1].comment.as_deref(), Some("@pgrpc_not_null"));
    }

    #[test]
    fn empty_and_null_field_lists_are_empty() {
        assert!(parse_fields(json!([])).unwrap().is_empty());
        assert!(parse_fields(Value::Null).unwrap().is_empty());
    }

    #[test]
    fn a_field_missing_a_key_is_an_error_not_dropped() {
        let result = parse_fields(json!([
            {"name": "email", "type_oid": 1001, "postgres_type": "email", "comment": null},
            {"name": "code", "postgres_type": "text", "comment": null}
        ]));
        assert!(result.is_err(), "{result:?}");
    }

    #[test]
    fn fields_are_nullable_unless_annotated() {
        let ty = composite(
            "t",
            1,
            Some("Payload. @pgrpc_not_null(by_type, also_by_type)"),
            vec![
                field("plain", 25, None),
                field("by_field", 25, Some("the id @pgrpc_not_null")),
                field("by_type", 25, None),
                field("also_by_type", 25, Some("unrelated comment")),
            ],
        );
        let nullable: Vec<bool> = ty.fields.iter().map(|f| ty.is_nullable(f)).collect();
        assert_eq!(nullable, [true, false, false, false]);
    }

    #[test]
    fn type_oids_cover_the_types_and_their_fields_once() {
        let composites = vec![
            composite("b", 200, None, vec![field("x", 25, None), field("y", 23, None)]),
            composite("a", 100, None, vec![field("x", 25, None)]),
            composite("empty", 300, None, vec![]),
        ];
        assert_eq!(composite_type_oids(&composites), [23, 25, 100, 200, 300]);
    }
}
