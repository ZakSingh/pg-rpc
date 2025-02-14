use crate::pg_image::Postgres;
use testcontainers::core::{AccessMode, Mount};
use testcontainers::runners::SyncRunner;
use testcontainers::core::{ImageExt};
use postgres::{Client, NoTls};
use testcontainers::core::Container;

pub struct Db {
    pub client: Client,
    #[allow(dead_code)]
    container: Container<Postgres>,
}

impl Db {
    pub fn new(init_sql: &str) -> Self {
        let container = Postgres::default()
            .with_init_sql(init_sql.as_bytes().to_owned())
            .with_mount(
                Mount::tmpfs_mount("/var/lib/postgresql/data")
                    .with_access_mode(AccessMode::ReadWrite),
            )
            .start()
            .expect("container to start");

        let connection_string = &format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            container.get_host().expect("host to be present"),
            container
                .get_host_port_ipv4(5432)
                .expect("port to be present")
        );

        let client = Client::connect(connection_string, NoTls)
            .expect("connection to be established");

        Db {
            client,
            container,
        }
    }
}
