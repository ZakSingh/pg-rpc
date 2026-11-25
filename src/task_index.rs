use crate::codegen::OID;
use crate::config::{Config, TaskQueueConfig};
use crate::db::{
    check_schema_composites, composite_type_oids, load_schema_composites, SchemaComposite,
};
use crate::ident::{sql_to_rs_string, CaseType};
use crate::pg_type::PgType;
use crate::ty_index::TypeIndex;
use postgres::Client;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};

/// The composite types of the task schema, keyed (and emitted) by name.
#[derive(Debug, Default)]
pub struct TaskIndex(BTreeMap<String, SchemaComposite>);

impl Deref for TaskIndex {
    type Target = BTreeMap<String, SchemaComposite>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TaskIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TaskIndex {
    /// Construct the task index by introspecting the configured task schema
    pub fn new(db: &mut Client, task_config: &TaskQueueConfig) -> anyhow::Result<Self> {
        let types = load_schema_composites(db, &task_config.schema)?;
        check_schema_composites(&types, &task_config.schema, false)?;
        Ok(Self::from_types(types))
    }

    fn from_types(types: impl IntoIterator<Item = SchemaComposite>) -> Self {
        Self(types.into_iter().map(|t| (t.name.clone(), t)).collect())
    }

    /// Check if the index is empty (no task types found)
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Collect all type OIDs from task types in the database
pub fn collect_task_type_oids(
    db_client: &mut postgres::Client,
    task_config: &TaskQueueConfig,
) -> anyhow::Result<Vec<OID>> {
    Ok(composite_type_oids(&load_schema_composites(
        db_client,
        &task_config.schema,
    )?))
}

/// Generate task payload enum from task index
pub fn generate_task_enum(
    task_index: &TaskIndex,
    type_index: &TypeIndex,
    config: &Config,
) -> (TokenStream, BTreeSet<String>) {
    task_enum(task_index, type_index.deref(), config)
}

fn task_enum(
    task_index: &TaskIndex,
    type_index: &BTreeMap<OID, PgType>,
    config: &Config,
) -> (TokenStream, BTreeSet<String>) {
    let mut referenced_schemas = BTreeSet::new();

    if task_index.is_empty() {
        return (TokenStream::new(), referenced_schemas);
    }

    let Some(task_config) = config.task_queue.as_ref() else {
        log::error!(
            "Task types were introspected but no task_queue configuration is present; \
             no task payload code is generated"
        );
        return (TokenStream::new(), referenced_schemas);
    };

    // First, generate all payload structs
    let payload_structs: Vec<TokenStream> = task_index
        .values()
        .map(|task_type| {
            let (struct_def, schemas) = generate_payload_struct(task_type, type_index, config);
            referenced_schemas.extend(schemas);
            struct_def
        })
        .collect();

    // Then generate enum variants (which now reference the payload structs)
    let variants: Vec<TokenStream> = task_index
        .values()
        .map(generate_task_variant)
        .collect();

    let task_name_column = &task_config.task_name_column;
    let payload_column = &task_config.payload_column;

    // Generate match arms for from_database_row
    let from_arms: Vec<TokenStream> = task_index
        .values()
        .map(|task_type| {
            let task_name = &task_type.name;

            quote! {
                #task_name => {
                    let tagged_value = serde_json::json!({
                        #task_name_column: task_name,
                        #payload_column: payload
                    });
                    serde_json::from_value(tagged_value)
                }
            }
        })
        .collect();

    // Generate match arms for task_name method (now handling tuple variants)
    let name_arms: Vec<TokenStream> = task_index
        .values()
        .map(|task_type| {
            let task_name = &task_type.name;
            let variant_name = format_ident!("{}", sql_to_rs_string(task_name, CaseType::Pascal));

            quote! {
                TaskPayload::#variant_name(_) => #task_name
            }
        })
        .collect();
    let full_table_name = task_config.get_full_table_name();
    let task_schema = &task_config.schema;

    let enum_doc = format!(
        "Task payload enum generated from PostgreSQL composite types in the '{}' schema.\n\nThis enum is designed to work with a task queue table '{}' with the following structure:\n- {}: TEXT (task type identifier)\n- {}: JSONB (task payload data)\n\nTask types are defined as composite types in the '{}' schema and automatically\nconverted to strongly-typed Rust enum variants.",
        task_schema, full_table_name, task_name_column, payload_column, task_schema
    );

    let enum_code = quote! {
        // Generate all payload structs first
        #(#payload_structs)*

        #[doc = #enum_doc]
        #[non_exhaustive]
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        #[serde(tag = #task_name_column, content = #payload_column)]
        pub enum TaskPayload {
            #(#variants),*
        }

        impl TaskPayload {
            /// Get the task type name for this payload
            pub fn task_name(&self) -> &'static str {
                match self {
                    #(#name_arms),*
                }
            }

            /// Deserialize a task payload from database row data
            pub fn from_database_row(task_name: &str, payload: serde_json::Value) -> Result<Self, serde_json::Error> {
                match task_name {
                    #(#from_arms),*,
                    _ => {
                        // Create a JSON object that will fail to deserialize into TaskPayload
                        let error_json = serde_json::json!({
                            #task_name_column: format!("__unknown_task_{}", task_name),
                            #payload_column: {}
                        });
                        serde_json::from_value(error_json)
                    }
                }
            }

            /// Get the configured task queue table name
            pub const fn table_name() -> &'static str {
                #full_table_name
            }

            /// Get the configured task name column
            pub const fn task_name_column() -> &'static str {
                #task_name_column
            }

            /// Get the configured payload column
            pub const fn payload_column() -> &'static str {
                #payload_column
            }
        }
    };

    (enum_code, referenced_schemas)
}

/// Generate a payload struct for a task type
fn generate_payload_struct(
    task_type: &SchemaComposite,
    type_index: &BTreeMap<OID, PgType>,
    config: &Config,
) -> (TokenStream, BTreeSet<String>) {
    let mut referenced_schemas = BTreeSet::new();
    let struct_name = format_ident!(
        "{}Payload",
        sql_to_rs_string(&task_type.name, CaseType::Pascal)
    );

    // Get the task queue schema from config
    let task_schema = config
        .task_queue
        .as_ref()
        .map(|tc| tc.schema.as_str())
        .unwrap_or("tasks");

    let fields: Vec<TokenStream> = task_type
        .fields
        .iter()
        .map(|field| {
            let field_name = format_ident!("{}", sql_to_rs_string(&field.name, CaseType::Snake));
            let pg_field_name = &field.name;

            // Fields are nullable unless annotated `@pgrpc_not_null`, on the
            // field or by name on the type (see `SchemaComposite::is_nullable`).
            let is_nullable = task_type.is_nullable(field);

            let (base_type, datetime_serde_attr) = match type_index.get(&field.type_oid) {
                Some(pg_type) => {
                    let (type_tokens, schemas) = pg_type.to_rust_ident_with_schemas(type_index);
                    referenced_schemas.extend(schemas);
                    (
                        type_tokens,
                        crate::pg_type::generate_datetime_serde_attr(pg_type, is_nullable),
                    )
                }
                // Every field type is collected by `collect_task_type_oids`,
                // so this means type introspection skipped it. Guessing a
                // Rust type from the type's name would produce a payload that
                // fails to (de)serialize; `serde_json::Value` round-trips
                // whatever JSON the task carries.
                None => {
                    log::warn!(
                        "Task type {}.{}: field `{}` has type {} which was not introspected; \
                         generating it as serde_json::Value",
                        task_schema,
                        task_type.name,
                        field.name,
                        field.postgres_type
                    );
                    (quote! { serde_json::Value }, None)
                }
            };

            let rust_type = if is_nullable {
                quote! { Option<#base_type> }
            } else {
                base_type
            };
            let serde_attr = if is_nullable {
                quote! { #[serde(rename = #pg_field_name, default)] }
            } else {
                quote! { #[serde(rename = #pg_field_name)] }
            };

            quote! {
                #serde_attr
                #datetime_serde_attr
                pub #field_name: #rust_type
            }
        })
        .collect();

    let deserialize_derive =
        if config.should_disable_deserialize(task_schema, &struct_name.to_string()) {
            quote! {}
        } else {
            quote! { serde::Deserialize, }
        };

    let payload_struct = quote! {
        #[derive(Debug, Clone, serde::Serialize, #deserialize_derive)]
        pub struct #struct_name {
            #(#fields),*
        }
    };

    (payload_struct, referenced_schemas)
}

/// Generate a single task variant: a tuple variant wrapping the payload
/// struct (which exists, empty, even for a task with no fields).
fn generate_task_variant(task_type: &SchemaComposite) -> TokenStream {
    let variant_name = format_ident!("{}", sql_to_rs_string(&task_type.name, CaseType::Pascal));
    let payload_struct_name = format_ident!(
        "{}Payload",
        sql_to_rs_string(&task_type.name, CaseType::Pascal)
    );
    let task_name = &task_type.name;

    quote! {
        #[serde(rename = #task_name)]
        #variant_name(#payload_struct_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_support::{composite, field};

    const TEXT: OID = 25;
    const INT4: OID = 23;
    const TIMESTAMPTZ: OID = 1184;
    const DATE: OID = 1082;

    fn types() -> BTreeMap<OID, PgType> {
        BTreeMap::from([
            (TEXT, PgType::Text),
            (INT4, PgType::Int32),
            (TIMESTAMPTZ, PgType::Timestamptz),
            (DATE, PgType::Date),
        ])
    }

    fn config() -> Config {
        Config {
            schemas: vec!["api".to_string()],
            task_queue: Some(TaskQueueConfig {
                schema: "tasks".to_string(),
                table_schema: Some("mq".to_string()),
                table_name: Some("task".to_string()),
                task_name_column: "task_name".to_string(),
                payload_column: "payload".to_string(),
            }),
            ..Default::default()
        }
    }

    /// Token text with all whitespace removed, so assertions do not depend on
    /// how `quote` spaces tokens.
    fn squash(tokens: &TokenStream) -> String {
        tokens.to_string().split_whitespace().collect()
    }

    fn payload_struct(task_type: &SchemaComposite) -> String {
        squash(&generate_payload_struct(task_type, &types(), &config()).0)
    }

    #[test]
    fn payload_fields_are_optional_unless_annotated() {
        let task = composite(
            "create_authorization",
            1,
            Some("Creates the authorization. @pgrpc_not_null(payment_method_id, stripe_customer_id)"),
            vec![
                field("payment_method_id", TEXT, None),
                field("stripe_customer_id", TEXT, None),
                field("attempt", INT4, Some("@pgrpc_not_null")),
                field("optional_field", TEXT, None),
            ],
        );
        let generated = payload_struct(&task);

        assert!(generated.contains("pubstructCreateAuthorizationPayload{"), "{generated}");
        for expected in [
            r#"#[serde(rename="payment_method_id")]pubpayment_method_id:String"#,
            r#"#[serde(rename="stripe_customer_id")]pubstripe_customer_id:String"#,
            r#"#[serde(rename="attempt")]pubattempt:i32"#,
            r#"#[serde(rename="optional_field",default)]puboptional_field:Option<String>"#,
        ] {
            assert!(generated.contains(expected), "missing {expected} in {generated}");
        }
    }

    #[test]
    fn datetime_fields_get_serde_with_attributes() {
        let task = composite(
            "schedule",
            1,
            Some("@pgrpc_not_null(run_at, on_day)"),
            vec![
                field("run_at", TIMESTAMPTZ, None),
                field("maybe_at", TIMESTAMPTZ, None),
                field("on_day", DATE, None),
                field("maybe_day", DATE, None),
            ],
        );
        let generated = payload_struct(&task);

        for expected in [
            r#"#[serde(rename="run_at")]#[serde(with="time::serde::rfc3339")]pubrun_at:"#,
            r#"#[serde(rename="maybe_at",default)]#[serde(with="time::serde::rfc3339::option")]pubmaybe_at:Option<"#,
            r#"#[serde(rename="on_day")]#[serde(with="date_serde")]pubon_day:"#,
            r#"#[serde(rename="maybe_day",default)]#[serde(with="date_serde::option")]pubmaybe_day:Option<"#,
        ] {
            assert!(generated.contains(expected), "missing {expected} in {generated}");
        }
    }

    #[test]
    fn field_of_unintrospected_type_is_json_value_not_a_guess() {
        // `timestamp` used to be guessed as `OffsetDateTime` and anything
        // unrecognised as `String`; both fail on real payloads.
        let task = composite(
            "odd",
            1,
            None,
            vec![field("when_local", 999_998, None), field("thing", 999_999, Some("@pgrpc_not_null"))],
        );
        let generated = payload_struct(&task);
        assert!(
            generated.contains("pubwhen_local:Option<serde_json::Value>"),
            "{generated}"
        );
        assert!(generated.contains("pubthing:serde_json::Value"), "{generated}");
        assert!(!generated.contains("String") && !generated.contains("OffsetDateTime"));
    }

    #[test]
    fn task_without_fields_gets_an_empty_payload_struct_and_a_tuple_variant() {
        let task = composite("shipment_created", 1, None, vec![]);
        assert!(payload_struct(&task).contains("pubstructShipmentCreatedPayload{}"));
        assert_eq!(
            squash(&generate_task_variant(&task)),
            r#"#[serde(rename="shipment_created")]ShipmentCreated(ShipmentCreatedPayload)"#
        );
    }

    #[test]
    fn disable_deserialize_drops_the_derive() {
        let task = composite("send_email", 1, None, vec![field("to", TEXT, None)]);
        let mut config = config();
        assert!(squash(&generate_payload_struct(&task, &types(), &config).0)
            .contains("#[derive(Debug,Clone,serde::Serialize,serde::Deserialize,)]"));
        config.disable_deserialize = vec!["tasks.SendEmailPayload".to_string()];
        assert!(squash(&generate_payload_struct(&task, &types(), &config).0)
            .contains("#[derive(Debug,Clone,serde::Serialize,)]"));
    }

    #[test]
    fn enum_covers_every_task_in_name_order() {
        let index = TaskIndex::from_types([
            composite("send_verification_code", 2, None, vec![field("email", TEXT, None)]),
            composite("cleanup_files", 1, None, vec![]),
        ]);
        let (tokens, _) = task_enum(&index, &types(), &config());
        let generated = squash(&tokens);

        // Payload structs, variants, and both match-arm lists.
        for expected in [
            "pubstructCleanupFilesPayload{}",
            "pubstructSendVerificationCodePayload{",
            r#"#[serde(tag="task_name",content="payload")]pubenumTaskPayload{"#,
            r#"#[serde(rename="cleanup_files")]CleanupFiles(CleanupFilesPayload),#[serde(rename="send_verification_code")]SendVerificationCode(SendVerificationCodePayload)"#,
            r#"TaskPayload::CleanupFiles(_)=>"cleanup_files",TaskPayload::SendVerificationCode(_)=>"send_verification_code""#,
            r#""cleanup_files"=>{lettagged_value=serde_json::json!({"task_name":task_name,"payload":payload});"#,
            r#"pubconstfntable_name()->&'staticstr{"mq.task"}"#,
        ] {
            assert!(generated.contains(expected), "missing {expected} in {generated}");
        }
    }

    #[test]
    fn enum_reports_schemas_of_user_defined_field_types() {
        let mut types = types();
        types.insert(
            5000,
            PgType::Enum {
                schema: "billing".to_string(),
                name: "currency".to_string(),
                variants: vec!["usd".to_string()],
                comment: None,
            },
        );
        let index = TaskIndex::from_types([composite(
            "charge",
            1,
            None,
            vec![field("currency", 5000, None)],
        )]);
        let (tokens, schemas) = task_enum(&index, &types, &config());
        assert!(squash(&tokens).contains("pubcurrency:Option<billing::Currency>"));
        assert_eq!(schemas, BTreeSet::from(["billing".to_string()]));
    }

    #[test]
    fn nothing_is_generated_without_tasks_or_without_config() {
        let (tokens, schemas) = task_enum(&TaskIndex::default(), &types(), &config());
        assert!(tokens.is_empty() && schemas.is_empty());

        // Task types but no `task_queue` section: skipped, not a panic.
        let index = TaskIndex::from_types([composite("t", 1, None, vec![])]);
        let (tokens, _) = task_enum(&index, &types(), &Config::default());
        assert!(tokens.is_empty());
    }
}
