// mod codegen;
// mod exceptions;
// mod infer_types;
// mod parse_explicit_exceptions;
//
// use indoc::indoc;
// use serde::{Deserialize, Serialize};
// use serde_json::Value;
// use tokio_postgres::{GenericClient, NoTls};
//
// #[derive(Debug)]
// struct PgFn {
//     schema: String,
//     name: String,
//     body: PgFnBody,
//     // args: Vec<PgFnArg>,
// }
//
// #[derive(Debug)]
// enum PgFnBody {
//     PgSQL(Value),
//     SQL(pg_query::ParseResult),
// }
//
// // #[derive(Debug)]
// // struct PgFnArg {
// //     name: String,
// //     arg_type: String,
// //     type_category: String,
// // }
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
//     // The connection object performs the actual communication with the database,
//     // so spawn it off to run on its own.
//     tokio::spawn(async move {
//         if let Err(e) = connection.await {
//             eprintln!("connection error: {}", e);
//         }
//     });
//
//     let fns: Vec<PgFn> = client
//         .query(
//             indoc! {"
// select p.proname                                as function_name,
//        pg_get_functiondef(p.oid)                as function_definition,
//        obj_description(p.oid, 'pg_proc')        as function_comment,
//        pg_catalog.pg_get_function_result(p.oid) as return_type,
//        t.typtype                                as return_type_category,  -- 'b', 'c', 'e', etc.
//        t.typname                                as return_base_type_name, -- Underlying type name
//        t.typrelid                               as return_relation_oid,   -- For tables or views
//        t.typelem                                as return_element_oid,     -- For array element types
//        nt.nspname                               as return_type_schema     -- Schema name of the return type
// from pg_proc p
//          join pg_namespace n on p.pronamespace = n.oid
//          join pg_type t on p.prorettype = t.oid
//          join pg_namespace nt on t.typnamespace = nt.oid
// where n.nspname = 'api';
//   "},
//             &[],
//         )
//         .await?
//         .iter()
//         .map(|row| {
//             // let arg_names: Vec<String> = row.get("arg_names");
//             // let arg_types: Vec<String> = row.get("arg_types");
//             // let type_categories: Vec<String> = row.get("type_categories");
//             //
//             // let args: Vec<PgFnArg> = arg_names
//             //     .into_iter()
//             //     .zip(arg_types)
//             //     .zip(type_categories)
//             //     .map(|((name, arg_type), type_category)| PgFnArg {
//             //         name,
//             //         arg_type,
//             //         type_category,
//             //     })
//             //     .collect();
//
//             let return_type = row.get("return_type");
//             let return_type_base = row.get("return_base_type_name");
//             let return_relation_oid = row.get("return_relation_oid");
//             let return_type_category = row.get("return_type_category");
//             let return_element_oid = row.get("return_element_oid");
//             let return_type_schema = row.get("return_type_schema");
//
//             enum RustType {
//                 Json,
//                 Int32,
//                 Int64,
//                 Float64,
//                 String,
//                 Bool,
//                 Timestamp,
//             }
//
//             match return_type_category {
//                 "b" => {
//                     if return_element_oid != 0 {
//                         // It's an array... Have to look it up.
//                     } else {
//                         // It's a built-in
//                         match return_type_base {
//                             "jsonb" => RustType::Json,
//                             "int4" => RustType::Int32,
//                             "int8" => RustType::Int64,
//                             "float8" => RustType::Float64,
//                             "text" => RustType::String,
//                             "bool" => RustType::Bool,
//                             "timestamptz" => RustType::Timestamp,
//                             _ => unimplemented!(),
//                         }
//                     }
//                 }
//                 "c" => {
//                     // composite
//                 }
//                 "e" => {
//                     // enum
//                 }
//                 "p" => {
//                     // meta
//                     match return_type_base {
//                         "void" => None,
//                         "record" => todo!(),
//                     }
//                 }
//                 _ => {}
//             }
//
//             PgFn {
//                 schema: row.get("schema_name"),
//                 name: row.get("function_name"),
//                 // args,
//                 return_type: row.get("return_type"),
//                 body: match row.get("function_language") {
//                     "plpgsql" => PgFnBody::PgSQL(
//                         pg_query::parse_plpgsql(row.get("function_definition")).unwrap(),
//                     ),
//                     "sql" => {
//                         PgFnBody::SQL(pg_query::parse(row.get("function_definition")).unwrap())
//                     }
//                     _ => unimplemented!(),
//                 },
//             }
//         })
//         .collect();
//
//     for f in fns {
//         dbg!(&f.body);
//     }
//
//     Ok(())
// }

mod infer_types;
