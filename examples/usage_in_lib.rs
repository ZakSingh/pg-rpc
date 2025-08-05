// Example showing how to include the generated code in your library/application

// In your lib.rs or main.rs:

// Include the generated modules
// Note: You need to generate the code first using pgrpc CLI
// and then include it from a known path, e.g.:
// include!("../generated/mod.rs");

// For this example, we'll just show the structure without actually including
// use api;  // Assuming you have an 'api' schema module
// use errors::PgRpcError;  // The unified error type

use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the database
    let (_client, connection) = tokio_postgres::connect(
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
    // After generating code with pgrpc CLI, you would use it like:
    // let user = api::get_user(&client, 123).await?;
    // match user {
    //     Ok(user) => println!("User: {:?}", user),
    //     Err(PgRpcError::UserNotFound(_)) => println!("User not found"),
    //     Err(e) => return Err(e.into()),
    // }
    
    println!("This is just an example of how to structure your code.");
    println!("Run 'pgrpc generate' first to generate the actual modules.");
    
    Ok(())
}