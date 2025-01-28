use std::{borrow::Cow, collections::HashMap};
use testcontainers::core::{AccessMode, Mount};
use testcontainers::{core::WaitFor, CopyDataSource, CopyToContainer, Image, ImageExt};

const NAME: &str = "zaksingh/postgres-plpgsql-check";
const TAG: &str = "16";

/// Module to work with [`Postgres`] inside of tests.
///
/// Starts an instance of Postgres.
/// This module is based on the official [`Postgres docker image`].
///
/// Default db name, user and password is `postgres`.
///
/// # Example
/// ```
/// use testcontainers_modules::{postgres, testcontainers::runners::SyncRunner};
///
/// let postgres_instance = postgres::Postgres::default().start().unwrap();
///
/// let connection_string = format!(
///     "postgres://postgres:postgres@{}:{}/postgres",
///     postgres_instance.get_host().unwrap(),
///     postgres_instance.get_host_port_ipv4(5432).unwrap()
/// );
/// ```
///
/// [`Postgres`]: https://www.postgresql.org/
/// [`Postgres docker image`]: https://hub.docker.com/_/postgres
#[derive(Debug, Clone)]
pub struct Postgres {
    env_vars: HashMap<String, String>,
    copy_to_sources: Vec<CopyToContainer>,
}

impl Postgres {
    /// Enables the Postgres instance to be used without authentication on host.
    /// For more information see the description of `POSTGRES_HOST_AUTH_METHOD` in official [docker image](https://hub.docker.com/_/postgres)
    pub fn with_host_auth(mut self) -> Self {
        self.env_vars
            .insert("POSTGRES_HOST_AUTH_METHOD".to_owned(), "trust".to_owned());
        self
    }

    /// Sets the db name for the Postgres instance.
    pub fn with_db_name(mut self, db_name: &str) -> Self {
        self.env_vars
            .insert("POSTGRES_DB".to_owned(), db_name.to_owned());
        self
    }

    /// Sets the user for the Postgres instance.
    pub fn with_user(mut self, user: &str) -> Self {
        self.env_vars
            .insert("POSTGRES_USER".to_owned(), user.to_owned());
        self
    }

    /// Sets the password for the Postgres instance.
    pub fn with_password(mut self, password: &str) -> Self {
        self.env_vars
            .insert("POSTGRES_PASSWORD".to_owned(), password.to_owned());
        self
    }

    /// Registers sql to be executed automatically when the container starts.
    /// Can be called multiple times to add (not override) scripts.
    ///
    /// # Example
    ///
    /// ```
    /// # use testcontainers_modules::postgres::Postgres;
    /// let postgres_image = Postgres::default().with_init_sql(
    ///     "CREATE EXTENSION IF NOT EXISTS hstore;"
    ///         .to_string()
    ///         .into_bytes(),
    /// );
    /// ```
    ///
    /// ```rust,ignore
    /// # use testcontainers_modules::postgres::Postgres;
    /// let postgres_image = Postgres::default()
    ///                                .with_init_sql(include_str!("path_to_init.sql").to_string().into_bytes());
    /// ```
    pub fn with_init_sql(mut self, init_sql: impl Into<CopyDataSource>) -> Self {
        let target = format!(
            "/docker-entrypoint-initdb.d/init_{i}.sql",
            i = self.copy_to_sources.len()
        );
        self.copy_to_sources
            .push(CopyToContainer::new(init_sql.into(), target));
        self
    }
}

impl Default for Postgres {
    fn default() -> Self {
        let mut env_vars = HashMap::new();
        env_vars.insert("POSTGRES_DB".to_owned(), "postgres".to_owned());
        env_vars.insert("POSTGRES_USER".to_owned(), "postgres".to_owned());
        env_vars.insert("POSTGRES_PASSWORD".to_owned(), "postgres".to_owned());

        Self {
            env_vars,
            copy_to_sources: Vec::new(),
        }
    }
}

impl Image for Postgres {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![
            WaitFor::message_on_stderr("database system is ready to accept connections"),
            WaitFor::message_on_stdout("database system is ready to accept connections"),
        ]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn copy_to_sources(&self) -> impl IntoIterator<Item = &CopyToContainer> {
        &self.copy_to_sources
    }
}
