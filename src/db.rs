use postgres::{Client, NoTls};

pub struct Db {
    pub client: Client,
}

impl Db {
    pub fn new(connection_string: &str) -> anyhow::Result<Self> {
        let mut client = Client::connect(connection_string, NoTls)?;
        // Disable JIT for introspection queries — the compilation overhead
        // far exceeds any execution benefit for these short-lived queries.
        client.execute("SET jit = off", &[])?;

        Ok(Db { client })
    }
}
