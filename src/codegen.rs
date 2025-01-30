use crate::fn_index::FunctionIndex;
use crate::ident::{sql_to_rs_ident, CaseType};
use crate::pg_type::PgType;
use crate::ty_index::TypeIndex;
use itertools::Itertools;
use quote::__private::TokenStream;
use quote::quote;
use std::collections::HashMap;

/// Postgres object ID
/// Uniquely identifies database objects
pub type OID = u32;

/// Postgres schema identifier
pub type SchemaName = String;

/// Postgres function identifier
pub type FunctionName = String;

pub trait ToRust {
    fn to_rust(&self, types: &HashMap<OID, PgType>) -> TokenStream;
}

/// Generate the output file
pub async fn codegen(fn_index: &FunctionIndex, ty_index: &TypeIndex) -> anyhow::Result<String> {
    let type_def_code = codegen_types(&ty_index);
    let fn_code = codegen_fns(&fn_index, &ty_index);

    let out: String = type_def_code
        .into_iter()
        .chain(fn_code) // Combine both maps into one iterator
        .into_grouping_map()
        .fold(TokenStream::new(), |acc, _, ts| quote! { #acc #ts })
        .iter()
        .map(|(schema, tokens)| {
            let s = sql_to_rs_ident(schema.as_str(), CaseType::Snake);
            prettyplease::unparse(
                &syn::parse2::<syn::File>(quote! {
                    mod #s {
                        #tokens
                    }
                })
                .expect("generated code to parse"),
            )
        })
        .collect();

    Ok(out)
}

fn codegen_types(type_index: &TypeIndex) -> HashMap<SchemaName, TokenStream> {
    type_index
        .values()
        .map(|t| (t.schema(), t.to_rust(type_index)))
        .filter(|(_, tokens)| !tokens.is_empty())
        .into_group_map()
        .into_iter()
        .map(|(schema, token_streams)| {
            (
                schema,
                quote! {
                    #(#token_streams)*
                },
            )
        })
        .collect()
}

/// Returns map of schema to token stream containing all fn definitions for that schema
fn codegen_fns(fns: &FunctionIndex, types: &TypeIndex) -> HashMap<SchemaName, TokenStream> {
    fns.fn_index
        .iter()
        .map(|(schema, fn_map)| {
            let fn_rust: Vec<TokenStream> = fn_map
                .values()
                .into_iter()
                .map(|f| f.to_rust(types))
                .collect();
            (
                schema.clone(),
                quote! {
                    #(#fn_rust)*
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::load_ddl::load_ddl;

    #[tokio::test]
    async fn hello() -> anyhow::Result<()> {
        let db = Db::new(
            r#"
                create type role as enum ('admin', 'user');

                create table account (
                    account_id int primary key generated always as identity,
                    name text not null,
                    age int,
                    role role not null
                );

                create table post (
                    post_id int primary key generated always as identity,
                    author_id int not null references account(account_id),
                    content text
                );

                create domain account_not_null as account not null;

                create view post_with_author as (
                    select post.*, row(account.*)::account_not_null as author from post
                    left join account on post.author_id = account.account_id
                );

                comment on table account is 'post with author comment';

                comment on column post_with_author.author is '@pgrpc_not_null';

                create schema api;

                create function api.optional_argument(required int, opt_1 int , opt_2 bool)
                returns post_with_author as $$
                begin
                   return 3;
                end;
                $$ language plpgsql;

                comment on function api.optional_argument(int, int, bool) is 'fn comment';
        "#,
        )
        .await;

        let fn_index = FunctionIndex::new(&db.client).await?;
        let ty_index = TypeIndex::new(&db.client, &fn_index.type_oids).await?;

        let code = codegen(&fn_index, &ty_index).await?;

        println!("{}", code);

        Ok(())
    }
}

// TODO:
// 1. Table return types
// 5. Exception tracking / error enum generation

// Need to test with arrays again.
// If it's a `returns setof composite_type` it expands each to a row
// If it's `returns table(name text, age int, email text)`, it expands to rows
// If it's `returns record` it's the same thing
