// use std::collections::HashMap;
// use anyhow::anyhow;
// use indoc::indoc;
// use serde_json::Value;
// use crate::PgFnBody;
//
// #[derive(Debug)]
// pub struct JsonSpan {
//   start: usize,  // Start of the actual JSON
//   end: usize,
// }
//
// #[derive(Debug)]
// pub struct JSONSchemas {
//   param_schemas: HashMap<String, Value>,
//   return_schema: Option<Value>,
// }
//
// pub fn parse_docstring(input: &str) -> anyhow::Result<JSONSchemas> {
//   let mut spans = Vec::new();
//   let mut depth = 0;
//   let mut in_string = false;
//   let mut escape_next = false;
//   let mut potential_start = None;
//   let mut schema_start = None;
//   let prefix = "@JSONSchema";
//
//   // Helper function to check if we're at a schema prefix
//   let is_schema_prefix_at = |pos: usize| -> bool {
//     if pos + prefix.len() > input.len() {
//       return false;
//     }
//     input[pos..pos + prefix.len()] == *prefix
//   };
//
//   let mut chars = input.char_indices().peekable();
//   while let Some((i, c)) = chars.next() {
//     if in_string {
//       if escape_next {
//         escape_next = false;
//         continue;
//       }
//
//       match c {
//         '\\' => escape_next = true,
//         '"' => in_string = false,
//         _ => {}
//       }
//       continue;
//     }
//
//     // Check for schema prefix if we're not tracking any JSON
//     if depth == 0 && is_schema_prefix_at(i) {
//       schema_start = Some(i);
//       // Skip the rest of the prefix
//       for _ in 0..prefix.len() - 1 {
//         chars.next();
//       }
//       continue;
//     }
//
//     match c {
//       '{' | '[' => {
//         if depth == 0 && schema_start.is_some() {
//           potential_start = Some(i);
//         }
//         depth += 1;
//       }
//       '}' | ']' => {
//         if depth > 0 {
//           depth -= 1;
//           if depth == 0 && potential_start.is_some() && schema_start.is_some() {
//             spans.push(JsonSpan {
//               start: potential_start.unwrap(),
//               end: i + 1,
//             });
//             potential_start = None;
//             schema_start = None;
//           }
//         }
//       }
//       '"' => in_string = true,
//       _ => {}
//     }
//   }
//
//   let return_start = input.find("Returns:");
//
//   let mut param_schemas: HashMap<String, Value> = HashMap::default();
//   let mut return_schema: Option<Value> = None;
//
//   for span in spans {
//     let json = serde_json::from_str(&input[span.start..span.end])?;
//
//     if let Err(e) = jsonschema::meta::validate(&json) {
//       return Err(anyhow!("Invalid JSON Schema at {} to {}: {}", span.start, span.end, e));
//     }
//
//     if return_start.is_some_and(|rs| rs < span.start) {
//       // must be the return
//       return_schema = Some(json);
//     } else {
//       let param_name_end = input[..span.start].rfind(" - ").unwrap();
//       let param_name_start = input[..param_name_end].rfind(char::is_whitespace).unwrap();
//       let param_name = &input[param_name_start + 1..param_name_end];
//
//       param_schemas.insert(param_name.to_owned(), json);
//     }
//   }
//
//   Ok(JSONSchemas {
//     return_schema,
//     param_schemas,
//   })
// }
//
//
// // Collect error codes...
// #[cfg(test)]
// mod tests {
//   use super::*;
//
//   #[test]
//   fn test_docstring_json_extraction() -> anyhow::Result<()> {
//     let doc_string = indoc! {r##"
//       Creates a new password reset request and queues a notification email.
//       Deletes any existing pending reset requests for the email address.
//
//       Parameters:
//         p_email - Email address to reset password for
//         p_reset_code - Plain text reset code to send in email
//         p_reset_code_hash - Hashed reset code to store
//         p_ip - The ip address
//         p_not_real - test note @JSONSchema {
//
//         }
//
//       Raises:
//         P0002 NO_DATA_FOUND - If email doesn't exist or doesn't have an internal login
//
//       Returns:
//         output - @JSONSchema {
//           "$id": "https://miniswap.gg/schemas/jwt-claims",
//           "type": "object",
//           "properties": {
//             "roles": {
//               "type": "array",
//               "items": {
//                 "enum": ["moderator", "admin"]
//               }
//             },
//             "suspendedRoles": {
//               "type": "array",
//               "items": {
//                 "enum": ["buying", "seller"]
//               }
//             }
//           },
//           "required": ["roles", "suspendedRoles"]
//         }
//     "##};
//
//     dbg!(parse_docstring(doc_string).unwrap());
//
//     Ok(())
//   }
// }