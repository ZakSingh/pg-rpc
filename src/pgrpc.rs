mod api {
    pub async fn middle(
        client: &impl tokio_postgres::GenericClient,
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
        SampleError(tokio_postgres::error::DbError),
        Other(tokio_postgres::Error),
    }
    impl From<tokio_postgres::Error> for MiddleError {
        fn from(e: tokio_postgres::Error) -> Self {
            let Some(db_error) = e.as_db_error() else {
                return MiddleError::Other(e);
            };
            match db_error.code() {
                code if code == &tokio_postgres::error::SqlState::from_code("P0001") => {
                    MiddleError::SampleError(db_error.to_owned())
                }
                _ => MiddleError::Other(e),
            }
        }
    }
    ///fn comment
    pub async fn optional_argument(
        client: &impl tokio_postgres::GenericClient,
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
        SampleError(tokio_postgres::error::DbError),
        Other(tokio_postgres::Error),
    }
    impl From<tokio_postgres::Error> for OptionalArgumentError {
        fn from(e: tokio_postgres::Error) -> Self {
            let Some(db_error) = e.as_db_error() else {
                return OptionalArgumentError::Other(e);
            };
            match db_error.code() {
                code if code == &tokio_postgres::error::SqlState::from_code("P0001") => {
                    OptionalArgumentError::SampleError(db_error.to_owned())
                }
                _ => OptionalArgumentError::Other(e),
            }
        }
    }
    pub async fn deppy(
        client: &impl tokio_postgres::GenericClient,
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
        SampleError(tokio_postgres::error::DbError),
        Other(tokio_postgres::Error),
    }
    impl From<tokio_postgres::Error> for DeppyError {
        fn from(e: tokio_postgres::Error) -> Self {
            let Some(db_error) = e.as_db_error() else {
                return DeppyError::Other(e);
            };
            match db_error.code() {
                code if code == &tokio_postgres::error::SqlState::from_code("P0001") => {
                    DeppyError::SampleError(db_error.to_owned())
                }
                _ => DeppyError::Other(e),
            }
        }
    }
    pub async fn create_account(
        client: &impl tokio_postgres::GenericClient,
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
        AccountNameCheck(tokio_postgres::error::DbError),
        AccountEmailKey(tokio_postgres::error::DbError),
        EmailNotNull(tokio_postgres::error::DbError),
        NameNotNull(tokio_postgres::error::DbError),
        RoleNotNull(tokio_postgres::error::DbError),
        SampleError(tokio_postgres::error::DbError),
        Other(tokio_postgres::Error),
    }
    impl From<tokio_postgres::Error> for CreateAccountError {
        fn from(e: tokio_postgres::Error) -> Self {
            let Some(db_error) = e.as_db_error() else {
                return CreateAccountError::Other(e);
            };
            match db_error.code() {
                code if code == &tokio_postgres::error::SqlState::from_code("P0001") => {
                    CreateAccountError::SampleError(db_error.to_owned())
                }
                &tokio_postgres::error::SqlState::CHECK_VIOLATION => {
                    match db_error.constraint().unwrap() {
                        "account_name_check" => {
                            CreateAccountError::AccountNameCheck(db_error.to_owned())
                        }
                        _ => CreateAccountError::Other(e),
                    }
                }
                &tokio_postgres::error::SqlState::NOT_NULL_VIOLATION => {
                    match db_error.column().unwrap() {
                        "email" => CreateAccountError::EmailNotNull(db_error.to_owned()),
                        "name" => CreateAccountError::NameNotNull(db_error.to_owned()),
                        "role" => CreateAccountError::RoleNotNull(db_error.to_owned()),
                        _ => CreateAccountError::Other(e),
                    }
                }
                &tokio_postgres::error::SqlState::UNIQUE_VIOLATION => {
                    match db_error.constraint().unwrap() {
                        "account_email_key" => {
                            CreateAccountError::AccountEmailKey(db_error.to_owned())
                        }
                        _ => CreateAccountError::Other(e),
                    }
                }
                _ => CreateAccountError::Other(e),
            }
        }
    }
    pub async fn create_post(
        client: &impl tokio_postgres::GenericClient,
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
        AccountIdNotNull(tokio_postgres::error::DbError),
        PostAccountIdFkey(tokio_postgres::error::DbError),
        TitleNotNull(tokio_postgres::error::DbError),
        SampleError(tokio_postgres::error::DbError),
        Strict(tokio_postgres::error::DbError),
        Other(tokio_postgres::Error),
    }
    impl From<tokio_postgres::Error> for CreatePostError {
        fn from(e: tokio_postgres::Error) -> Self {
            let Some(db_error) = e.as_db_error() else {
                return CreatePostError::Other(e);
            };
            match db_error.code() {
                code if code == &tokio_postgres::error::SqlState::from_code("P0001") => {
                    CreatePostError::SampleError(db_error.to_owned())
                }
                &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION => {
                    match db_error.constraint().unwrap() {
                        "post_account_id_fkey" => {
                            CreatePostError::PostAccountIdFkey(db_error.to_owned())
                        }
                        _ => CreatePostError::Other(e),
                    }
                }
                &tokio_postgres::error::SqlState::NOT_NULL_VIOLATION => {
                    match db_error.column().unwrap() {
                        "account_id" => {
                            CreatePostError::AccountIdNotNull(db_error.to_owned())
                        }
                        "title" => CreatePostError::TitleNotNull(db_error.to_owned()),
                        _ => CreatePostError::Other(e),
                    }
                }
                _ => CreatePostError::Other(e),
            }
        }
    }
}
mod public {
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
    #[derive(Debug, Clone, Copy, postgres_types::FromSql, postgres_types::ToSql)]
    #[postgres(name = "role")]
    pub enum Role {
        #[postgres(name = "admin")]
        Admin,
        #[postgres(name = "user")]
        User,
    }
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
}
