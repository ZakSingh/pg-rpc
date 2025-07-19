// Example showing how to include the generated code in your library/application

// In your lib.rs or main.rs:

// Include the generated modules
include!(concat!(env!("OUT_DIR"), "/pgrpc/mod.rs"));

use tokio_postgres::{NoTls, Client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the database
    let (client, connection) = tokio_postgres::connect(
        "postgres://postgres:postgres@localhost:5432/mydb",
        NoTls
    ).await?;
    
    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Use the generated functions
    // Assuming you have a function called 'get_user' in the 'api' schema
    // let user = api::get_user(&client, 123).await?;
    // println!("User: {:?}", user);
    
    Ok(())
}