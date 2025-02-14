#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]

pub mod public {
    #[derive(
        Clone,
        Debug,
        postgres_types::ToSql,
        postgres_types::FromSql,
        serde::Serialize,
        serde::Deserialize
    )]
    #[postgres(name = "account")]
    pub struct Account {
        #[postgres(name = "account_id")]
        pub account_id: i32,
        #[postgres(name = "email")]
        pub email: String,
        #[postgres(name = "name")]
        pub name: String,
        #[postgres(name = "role")]
        pub role: super::public::Role,
        #[postgres(name = "created_at")]
        pub created_at: time::OffsetDateTime,
    }
    impl TryFrom<tokio_postgres::Row> for Account {
        type Error = tokio_postgres::Error;
        fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
            Ok(Self {
                account_id: row.try_get("account_id")?,
                email: row.try_get("email")?,
                name: row.try_get("name")?,
                role: row.try_get("role")?,
                created_at: row.try_get("created_at")?,
            })
        }
    }
    #[derive(
        Debug,
        Clone,
        Copy,
        postgres_types::FromSql,
        postgres_types::ToSql,
        serde::Deserialize,
        serde::Serialize
    )]
    #[postgres(name = "role")]
    pub enum Role {
        #[postgres(name = "admin")]
        Admin,
        #[postgres(name = "user")]
        User,
    }
}
pub mod api {
    /**
    @pgrpc_return_null
*/
    pub async fn get_account_by_email(
        client: &impl deadpool_postgres::GenericClient,
        p_email: &str,
    ) -> Result<Option<super::public::Account>, GetAccountByEmailError> {
        let mut params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![& p_email];
        let mut query = concat!("select api.get_account_by_email", "(", "$1")
            .to_string();
        query.push_str(")");
        client
            .query_one(&query, &params)
            .await
            .and_then(|r| r.try_get(0))
            .map_err(Into::into)
    }
    #[derive(Debug, thiserror::Error)]
    pub enum GetAccountByEmailError {
        #[error(transparent)]
        Other(tokio_postgres::Error),
    }
    impl From<tokio_postgres::Error> for GetAccountByEmailError {
        fn from(e: tokio_postgres::Error) -> Self {
            let Some(db_error) = e.as_db_error() else {
                return GetAccountByEmailError::Other(e);
            };
            match db_error.code() {
                _ => GetAccountByEmailError::Other(e),
            }
        }
    }
}
