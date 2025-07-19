use postgres::{Client, NoTls};

pub struct Db {
    pub client: Client,
}

impl Db {
    pub fn new(connection_string: &str) -> anyhow::Result<Self> {
        let client = Client::connect(connection_string, NoTls)?;
        
        Ok(Db {
            client,
        })
    }
}
