// use crate::PgFn;
// use anyhow::anyhow;
// use convert_case::{Case, Casing};
// use indoc::indoc;
// use serde::{Deserialize, Serialize};
// use serde_json::Value;
// use serde_json_path::JsonPath;
// use std::collections::HashMap;
// use tokio_postgres::{Client, NoTls};
//
// #[derive(Debug)]
// pub struct PgEnum {
//     schema: String,
//     name: String,
//     values: Vec<String>,
// }
//
// impl PgEnum {
//     pub fn to_rust_code(&self) -> String {
//         let enum_name = self.name.to_case(Case::Pascal);
//         let variants: Vec<String> = self
//             .values
//             .iter()
//             .map(|v| v.to_case(Case::Pascal))
//             .collect();
//
//         format!(
//             indoc! {r###"
//             #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
//             #[serde(rename_all = \"snake_case\")]
//             pub enum {} {{
//                 {},
//             }}
//
//             impl postgres_types::ToSql for {} {{
//                 fn to_sql(&self, ty: &postgres_types::Type, out: &mut postgres_types::private::BytesMut) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {{
//                     use postgres_types::private::bytes::BufMut;
//                     match self {{
//                         {}
//                     }}
//                     Ok(postgres_types::IsNull::No)
//                 }}
//
//                 fn accepts(ty: &postgres_types::Type) -> bool {{
//                     ty.name() == \"{}\" && ty.schema() == \"{}\"
//                 }}
//
//                 postgres_types::to_sql_checked!();
//             }}
//
//             impl<'a> postgres_types::FromSql<'a> for {} {{
//                 fn from_sql(ty: &postgres_types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {{
//                     let s = String::from_utf8(raw.to_vec())?;
//                     match s.as_str() {{
//                         {}
//                         _ => Err(\"Invalid enum value\".into())
//                     }}
//                 }}
//
//                 fn accepts(ty: &postgres_types::Type) -> bool {{
//                     ty.name() == \"{}\" && ty.schema() == \"{}\"
//                 }}
//             }}
//         "###},
//             enum_name,
//             variants.join(",\n    "),
//             enum_name,
//             self.values
//                 .iter()
//                 .enumerate()
//                 .map(|(i, v)| {
//                     format!(
//                         "Self::{} => out.put_slice(\"{}\".as_bytes()),",
//                         v.to_case(Case::Pascal),
//                         v
//                     )
//                 })
//                 .collect::<Vec<_>>()
//                 .join("\n        "),
//             self.name,
//             self.schema,
//             enum_name,
//             self.values
//                 .iter()
//                 .map(|v| { format!("\"{}\" => Ok(Self::{}),", v, v.to_case(Case::Pascal)) })
//                 .collect::<Vec<_>>()
//                 .join("\n            "),
//             self.name,
//             self.schema
//         )
//     }
// }
//
// pub async fn generate_enums(client: &Client) -> anyhow::Result<()> {
//     // First get all custom enums
//     let enums: Vec<PgEnum> = client
//         .query(
//             indoc! {"
//         SELECT
//             n.nspname as schema_name,
//             t.typname as enum_name,
//             array_agg(e.enumlabel ORDER BY e.enumsortorder) as enum_values
//         FROM pg_type t
//         JOIN pg_enum e ON t.oid = e.enumtypid
//         JOIN pg_namespace n ON n.oid = t.typnamespace
//         GROUP BY schema_name, enum_name;
//     "},
//             &[],
//         )
//         .await?
//         .iter()
//         .map(|row| PgEnum {
//             schema: row.get("schema_name"),
//             name: row.get("enum_name"),
//             values: row.get("enum_values"),
//         })
//         .collect();
//
//     // Generate enum code
//     for pg_enum in &enums {
//         println!(
//             "\n// Generated Rust enum for {}.{}",
//             pg_enum.schema, pg_enum.name
//         );
//         println!("{}", pg_enum.to_rust_code());
//     }
//
//     Ok(())
// }
//
// pub fn generate_rust_function(pg_fn: &PgFn) -> String {
//     let fn_name = pg_fn.name.to_case(Case::Snake);
//     let args = generate_args(&pg_fn.args);
//     let return_type = map_pg_to_rust_type(&pg_fn.return_type);
//     let is_setof =
//         pg_fn.return_type.starts_with("SETOF ") || pg_fn.return_type.starts_with("TABLE(");
//
//     let query_method = if is_setof { "query" } else { "query_one" };
//     let result_handling = if is_setof {
//         "rows.into_iter().map(|row| row.get(0)).collect()"
//     } else {
//         "row.get(0)"
//     };
//
//     format!(
//         indoc! {"
//             /// Generated from PostgreSQL function {}.{}
//             ///
//             /// Returns: {}
//             pub async fn {}({}) -> anyhow::Result<{}> {{
//                 let client = get_client().await?;
//
//                 let {} = client
//                     .{}(
//                         \"{}\",
//                         &[{}]
//                     )
//                     .await?;
//
//                 Ok({})
//             }}
//         "},
//         pg_fn.schema,
//         pg_fn.name,
//         pg_fn.return_type,
//         fn_name,
//         args.0,
//         return_type,
//         if is_setof { "rows" } else { "row" },
//         query_method,
//         generate_query(&pg_fn.name, &pg_fn.args),
//         args.1,
//         result_handling
//     )
// }
//
// fn generate_args(args: &[PgFnArg]) -> (String, String) {
//     let mut rust_args = Vec::new();
//     let mut query_args = Vec::new();
//
//     for (i, arg) in args.iter().enumerate() {
//         let arg_name = arg.name.trim_start_matches("p_").to_case(Case::Snake);
//         let arg_type = map_pg_to_rust_type(&arg.arg_type);
//
//         rust_args.push(format!("{}: {}", arg_name, arg_type));
//         query_args.push(format!("&{}", arg_name));
//     }
//
//     (rust_args.join(", "), query_args.join(", "))
// }
//
// fn generate_query(fn_name: &str, args: &[PgFnArg]) -> String {
//     let params: Vec<String> = (1..=args.len()).map(|i| format!("${}", i)).collect();
//     format!("SELECT {}({})", fn_name, params.join(", "))
// }
//
// fn map_pg_to_rust_type(pg_type: &str) -> String {
//     if let Some(setof_type) = pg_type.strip_prefix("SETOF ") {
//         return format!("Vec<{}>", map_pg_to_rust_type(setof_type));
//     }
//
//     // Handle table types which are often used with RETURNS TABLE(...)
//     if pg_type.starts_with("TABLE(") {
//         // TODO: Parse the table definition and create a struct
//         return format!("// TODO: Create struct for table type: {}", pg_type);
//     }
//
//     match pg_type {
//         "integer" | "int4" => "i32".to_string(),
//         "bigint" | "int8" => "i64".to_string(),
//         "text" | "varchar" | "character varying" => "String".to_string(),
//         "boolean" | "bool" => "bool".to_string(),
//         "jsonb" | "json" => "serde_json::Value".to_string(),
//         "timestamp with time zone" | "timestamptz" => "chrono::DateTime<chrono::Utc>".to_string(),
//         "uuid" => "uuid::Uuid".to_string(),
//         "numeric" | "decimal" => "rust_decimal::Decimal".to_string(),
//         // Add more type mappings as needed
//         _ => format!("// TODO: Map PostgreSQL type: {}", pg_type),
//     }
// }
//
// #[tokio::main]
// async fn main() -> anyhow::Result<()> {
//     let schema = "api";
//
//     let (client, connection) = tokio_postgres::connect(
//         "host=localhost port=5432 user=postgres password=password dbname=miniswap",
//         NoTls,
//     )
//     .await?;
//
//     tokio::spawn(async move {
//         if let Err(e) = connection.await {
//             eprintln!("connection error: {}", e);
//         }
//     });
//
//     // First get all custom enums
//     let enums: Vec<PgEnum> = client
//         .query(
//             indoc! {"
//         SELECT
//             n.nspname as schema_name,
//             t.typname as enum_name,
//             array_agg(e.enumlabel ORDER BY e.enumsortorder) as enum_values
//         FROM pg_type t
//         JOIN pg_enum e ON t.oid = e.enumtypid
//         JOIN pg_namespace n ON n.oid = t.typnamespace
//         WHERE n.nspname = $1
//         GROUP BY schema_name, enum_name;
//     "},
//             &[&schema],
//         )
//         .await?
//         .iter()
//         .map(|row| PgEnum {
//             schema: row.get("schema_name"),
//             name: row.get("enum_name"),
//             values: row.get("enum_values"),
//         })
//         .collect();
//
//     // Generate enum code
//     for pg_enum in &enums {
//         println!(
//             "\n// Generated Rust enum for {}.{}",
//             pg_enum.schema, pg_enum.name
//         );
//         println!("{}", pg_enum.to_rust_code());
//     }
//
//     // After collecting functions, generate Rust code
//     // for f in &fns {
//     //   println!("\n// Generated Rust function for {}.{}", f.schema, f.name);
//     //   println!("{}", codegen::generate_rust_function(f));
//     // }
//
//     Ok(())
// }
//
// // Keep existing test modules...
