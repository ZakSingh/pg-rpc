// mod api {
//     #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
//     #[postgres(name = "account_with_posts")]
//     pub struct AccountWithPosts {
//         #[postgres(name = "account_id")]
//         pub account_id: i32,
//         #[postgres(name = "name")]
//         pub name: String,
//         #[postgres(name = "age")]
//         pub age: Option<i32>,
//         #[postgres(name = "role")]
//         pub role: super::public::Role,
//         #[postgres(name = "posts")]
//         pub posts: Vec<super::public::Post>,
//     }
//     impl TryFrom<tokio_postgres::Row> for AccountWithPosts {
//         type Error = tokio_postgres::Error;
//         fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
//             Ok(Self {
//                 account_id: row.try_get("account_id")?,
//                 name: row.try_get("name")?,
//                 age: row.try_get("age")?,
//                 role: row.try_get("role")?,
//                 posts: row.try_get("posts")?,
//             })
//         }
//     }
//     #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
//     #[postgres(name = "post_with_author_view")]
//     pub struct PostWithAuthorView {
//         #[postgres(name = "content")]
//         pub content: Option<String>,
//         #[postgres(name = "post_id")]
//         pub post_id: i32,
//         #[postgres(name = "a")]
//         pub a: Option<super::public::Account>,
//     }
//     impl TryFrom<tokio_postgres::Row> for PostWithAuthorView {
//         type Error = tokio_postgres::Error;
//         fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
//             Ok(Self {
//                 content: row.try_get("content")?,
//                 post_id: row.try_get("post_id")?,
//                 a: row.try_get("a")?,
//             })
//         }
//     }
//     #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
//     #[postgres(name = "_account_with_posts")]
//     pub struct _AccountWithPosts {
//         #[postgres(name = "account_id")]
//         pub account_id: Option<i32>,
//         #[postgres(name = "name")]
//         pub name: Option<String>,
//         #[postgres(name = "age")]
//         pub age: Option<i32>,
//         #[postgres(name = "role")]
//         pub role: Option<super::public::Role>,
//         #[postgres(name = "posts")]
//         pub posts: Option<Vec<super::public::Post>>,
//     }
//     impl TryFrom<tokio_postgres::Row> for _AccountWithPosts {
//         type Error = tokio_postgres::Error;
//         fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
//             Ok(Self {
//                 account_id: row.try_get("account_id")?,
//                 name: row.try_get("name")?,
//                 age: row.try_get("age")?,
//                 role: row.try_get("role")?,
//                 posts: row.try_get("posts")?,
//             })
//         }
//     }
//     pub async fn create_account(
//         client: &tokio_postgres::Client,
//         name: &str,
//         age: Option<i32>,
//         role: Option<super::public::Role>,
//     ) -> Result<super::public::Account, tokio_postgres::Error> {
//         client
//             .query_one(
//                 "select api.create_account($1, role := $3);",
//                 &[&name, &role],
//             )
//             .await
//             .and_then(|r| r.try_get(0));
//
//         client
//             .query_one(
//                 "select api.create_account($1, $2, $3);",
//                 &[&name, &age, &role],
//             )
//             .await
//             .and_then(|r| r.try_get(0))
//     }
//     pub async fn find_account_by_id(
//         client: &tokio_postgres::Client,
//         p_account_id: i32,
//     ) -> Result<Vec<super::public::Account>, tokio_postgres::Error> {
//         client
//             .query(
//                 "select * from api.find_account_by_id($1);",
//                 &[&p_account_id],
//             )
//             .await
//             .and_then(|rows| rows.into_iter().map(TryInto::try_into).collect())
//     }
//     pub async fn find_post_by_id(
//         client: &tokio_postgres::Client,
//         p_post_id: i32,
//     ) -> Result<Vec<super::api::PostWithAuthorView>, tokio_postgres::Error> {
//         client
//             .query("select * from api.find_post_by_id($1);", &[&p_post_id])
//             .await
//             .and_then(|rows| rows.into_iter().map(TryInto::try_into).collect())
//     }
//     pub async fn get_account_with_posts(
//         client: &tokio_postgres::Client,
//         p_account_id: i32,
//     ) -> Result<super::api::AccountWithPosts, tokio_postgres::Error> {
//         client
//             .query_one("select api.get_account_with_posts($1);", &[&p_account_id])
//             .await
//             .and_then(|r| r.try_get(0))
//     }
// }
// mod public {
//     #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
//     #[postgres(name = "account")]
//     pub struct Account {
//         #[postgres(name = "account_id")]
//         pub account_id: i32,
//         #[postgres(name = "name")]
//         pub name: String,
//         #[postgres(name = "age")]
//         pub age: Option<i32>,
//         #[postgres(name = "role")]
//         pub role: super::public::Role,
//     }
//     impl TryFrom<tokio_postgres::Row> for Account {
//         type Error = tokio_postgres::Error;
//         fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
//             Ok(Self {
//                 account_id: row.try_get("account_id")?,
//                 name: row.try_get("name")?,
//                 age: row.try_get("age")?,
//                 role: row.try_get("role")?,
//             })
//         }
//     }
//     #[derive(Debug, Clone, Copy, postgres_types::FromSql, postgres_types::ToSql)]
//     #[postgres(name = "role")]
//     pub enum Role {
//         #[postgres(name = "admin")]
//         Admin,
//         #[postgres(name = "user")]
//         User,
//     }
//     #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
//     #[postgres(name = "post")]
//     pub struct Post {
//         #[postgres(name = "post_id")]
//         pub post_id: i32,
//         #[postgres(name = "author_id")]
//         pub author_id: i32,
//         #[postgres(name = "content")]
//         pub content: Option<String>,
//     }
//     impl TryFrom<tokio_postgres::Row> for Post {
//         type Error = tokio_postgres::Error;
//         fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
//             Ok(Self {
//                 post_id: row.try_get("post_id")?,
//                 author_id: row.try_get("author_id")?,
//                 content: row.try_get("content")?,
//             })
//         }
//     }
// }
//
// mod test {
//     use super::*;
//     use crate::dump::public::Role;
//     use crate::tests::setup_test_db;
//
//     #[tokio::test]
//     async fn t() -> anyhow::Result<()> {
//         let db = setup_test_db().await;
//
//         db.client
//             .simple_query(
//                 r#"
//                 create type role as enum ('admin', 'user');
//
//                 create table account (
//                     account_id int primary key generated always as identity,
//                     name text not null,
//                     age int,
//                     role role not null
//                 );
//
//                 select create_account('Zak', age := 10)
//
//                 create function create_account(name text, age int default 20, role role default 'user')
//                 returns account as $$
//                 declare
//                     result account;
//                 begin
//                     insert into account (name, age, role)
//                     values (name, age, role)
//                     returning * into result;
//
//                     return result;
//                 end;
//                 $$ language plpgsql;
//         "#,
//             )
//             .await?;
//
//         let x = db
//             .client
//             .query_one(
//                 "select create_account(name := $1, role := $2);",
//                 &[&"Zak", &Role::User],
//             )
//             .await?;
//
//         Ok(())
//     }
// }
