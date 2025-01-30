use crate::pg_image::Postgres;
use testcontainers::core::{AccessMode, Mount};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use tokio_postgres::{Client, NoTls};

pub struct Db {
    pub client: Client,
    #[allow(dead_code)]
    container: ContainerAsync<Postgres>,
    #[allow(dead_code)]
    connection_handle: tokio::task::JoinHandle<Result<(), tokio_postgres::error::Error>>,
}

impl Db {
    pub async fn new(init_sql: &str) -> Self {
        let container = Postgres::default()
            .with_init_sql(init_sql.as_bytes().to_owned())
            .with_mount(
                Mount::tmpfs_mount("/var/lib/postgresql/data")
                    .with_access_mode(AccessMode::ReadWrite),
            )
            .start()
            .await
            .expect("container to start");

        let connection_string = &format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            container.get_host().await.expect("host to be present"),
            container
                .get_host_port_ipv4(5432)
                .await
                .expect("port to be present")
        );

        let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
            .expect("connection to be established");

        let connection_handle = tokio::spawn(connection);

        Db {
            client,
            container,
            connection_handle,
        }
    }
}
