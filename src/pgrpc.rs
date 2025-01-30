mod api {
    ///fn comment
    pub async fn optional_argument(
        client: &tokio_postgres::Client,
        un: i32,
    ) -> Result<i32, OptionalArgumentError> {
        let mut params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![& un];
        let mut query = concat!("select api.optional_argument", "(", "$1").to_string();
        query.push_str(")");
        client
            .query_one(&query, &params)
            .await
            .and_then(|r| r.try_get(0))
            .map_err(Into::into)
    }
    #[derive(Debug)]
    pub enum OptionalArgumentError {
        Other(tokio_postgres::Error),
    }
    impl From<tokio_postgres::Error> for OptionalArgumentError {
        fn from(e: tokio_postgres::Error) -> Self {
            match e {
                e => OptionalArgumentError::Other(e),
            }
        }
    }
    pub async fn deppy(
        client: &tokio_postgres::Client,
        un: i32,
    ) -> Result<i32, DeppyError> {
        let mut params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![& un];
        let mut query = concat!("select api.deppy", "(", "$1").to_string();
        query.push_str(")");
        client
            .query_one(&query, &params)
            .await
            .and_then(|r| r.try_get(0))
            .map_err(Into::into)
    }
    #[derive(Debug)]
    pub enum DeppyError {
        Other(tokio_postgres::Error),
    }
    impl From<tokio_postgres::Error> for DeppyError {
        fn from(e: tokio_postgres::Error) -> Self {
            match e {
                e => DeppyError::Other(e),
            }
        }
    }
    pub async fn create_account(
        client: &tokio_postgres::Client,
        email: &str,
        name: &str,
        role: super::public::Role,
    ) -> Result<super::public::Account, CreateAccountError> {
        let mut params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![
            & email, & name, & role
        ];
        let mut query = concat!("select api.create_account", "(", "$1, $2, $3")
            .to_string();
        query.push_str(")");
        client
            .query_one(&query, &params)
            .await
            .and_then(|r| r.try_get(0))
            .map_err(Into::into)
    }
    #[derive(Debug)]
    pub enum CreateAccountError {
        Other(tokio_postgres::Error),
    }
    impl From<tokio_postgres::Error> for CreateAccountError {
        fn from(e: tokio_postgres::Error) -> Self {
            match e {
                e => CreateAccountError::Other(e),
            }
        }
    }
    pub async fn create_post(
        client: &tokio_postgres::Client,
        account_id: i32,
        title: &str,
        content: Option<&str>,
    ) -> Result<super::public::PostWithAuthor, CreatePostError> {
        let mut params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![
            & account_id, & title
        ];
        let mut query = concat!("select api.create_post", "(", "$1, $2").to_string();
        if let Some(ref value) = content {
            params.push(value);
            query.push_str(concat!(", ", "content", ":= $"));
            query.push_str(&params.len().to_string());
        }
        query.push_str(")");
        client
            .query_one(&query, &params)
            .await
            .and_then(|r| r.try_get(0))
            .map_err(Into::into)
    }
    #[derive(Debug)]
    pub enum CreatePostError {
        Other(tokio_postgres::Error),
    }
    impl From<tokio_postgres::Error> for CreatePostError {
        fn from(e: tokio_postgres::Error) -> Self {
            match e {
                e => CreatePostError::Other(e),
            }
        }
    }
    pub async fn middle(
        client: &tokio_postgres::Client,
        un: i32,
    ) -> Result<i32, MiddleError> {
        let mut params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![& un];
        let mut query = concat!("select api.middle", "(", "$1").to_string();
        query.push_str(")");
        client
            .query_one(&query, &params)
            .await
            .and_then(|r| r.try_get(0))
            .map_err(Into::into)
    }
    #[derive(Debug)]
    pub enum MiddleError {
        Other(tokio_postgres::Error),
    }
    impl From<tokio_postgres::Error> for MiddleError {
        fn from(e: tokio_postgres::Error) -> Self {
            match e {
                e => MiddleError::Other(e),
            }
        }
    }
}
mod public {
    #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
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
    #[derive(Debug, Clone, Copy, postgres_types::FromSql, postgres_types::ToSql)]
    #[postgres(name = "role")]
    pub enum Role {
        #[postgres(name = "admin")]
        Admin,
        #[postgres(name = "user")]
        User,
    }
    #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
    #[postgres(name = "post_with_author")]
    pub struct PostWithAuthor {
        #[postgres(name = "post_id")]
        pub post_id: i32,
        #[postgres(name = "account_id")]
        pub account_id: i32,
        #[postgres(name = "title")]
        pub title: String,
        #[postgres(name = "content")]
        pub content: String,
        #[postgres(name = "created_at")]
        pub created_at: Option<time::OffsetDateTime>,
        ///@pgrpc_not_null
        #[postgres(name = "author")]
        pub author: super::public::Account,
    }
    impl TryFrom<tokio_postgres::Row> for PostWithAuthor {
        type Error = tokio_postgres::Error;
        fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
            Ok(Self {
                post_id: row.try_get("post_id")?,
                account_id: row.try_get("account_id")?,
                title: row.try_get("title")?,
                content: row.try_get("content")?,
                created_at: row.try_get("created_at")?,
                author: row.try_get("author")?,
            })
        }
    }
}
