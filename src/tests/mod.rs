use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio_postgres::{Client, NoTls};

pub struct TestDb {
    pub client: Client,
    // Store these to keep them in scope
    #[allow(dead_code)]
    container: ContainerAsync<Postgres>,
    #[allow(dead_code)]
    connection_handle: tokio::task::JoinHandle<Result<(), tokio_postgres::error::Error>>,
}

pub async fn setup_test_db() -> TestDb {
    let container = Postgres::default()
        .with_tag("16-alpine")
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

    TestDb {
        client,
        container,
        connection_handle,
    }
}
