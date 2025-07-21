use postgres::{Client, NoTls};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Try to connect to the miniswap database
    let mut client = Client::connect("postgres://postgres@localhost/miniswap", NoTls)?;
    
    println!("=== Checking for tasks schema ===");
    let rows = client.query("SELECT nspname FROM pg_namespace WHERE nspname = 'tasks'", &[])?;
    println!("Found {} rows", rows.len());
    for row in &rows {
        let nspname: String = row.get(0);
        println!("Schema: {}", nspname);
    }
    
    println!("\n=== Checking composite types in tasks schema ===");
    let rows = client.query(
        "SELECT t.typname, t.oid, t.typrelid
         FROM pg_type t
         WHERE t.typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tasks')
           AND t.typtype = 'c'
         ORDER BY t.typname",
        &[]
    )?;
    
    println!("Found {} composite types", rows.len());
    for row in &rows {
        let typname: String = row.get(0);
        let oid: u32 = row.get(1);
        let typrelid: u32 = row.get(2);
        println!("Type: {} (OID: {}, typrelid: {})", typname, oid, typrelid);
    }
    
    println!("\n=== Checking attributes of composite types ===");
    let rows = client.query(
        "SELECT 
            t.typname as composite_type,
            a.attname as field_name,
            a.atttypid as type_oid,
            format_type(a.atttypid, a.atttypmod) as postgres_type,
            a.attnum as position,
            a.attnotnull as not_null
        FROM pg_type t
        JOIN pg_attribute a ON a.attrelid = t.typrelid
        WHERE t.typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tasks')
          AND t.typtype = 'c'
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY t.typname, a.attnum",
        &[]
    )?;
    
    println!("Found {} fields total", rows.len());
    for row in &rows {
        let composite_type: String = row.get(0);
        let field_name: String = row.get(1);
        let type_oid: u32 = row.get(2);
        let postgres_type: String = row.get(3);
        let position: i32 = row.get(4);
        let not_null: bool = row.get(5);
        
        println!("  {}.{} - {} (OID: {}, pos: {}, not_null: {})", 
                 composite_type, field_name, postgres_type, type_oid, position, not_null);
    }
    
    println!("\n=== Running the actual introspection query ===");
    let rows = client.query(
        "SELECT 
            t.typname as task_name,
            t.oid as type_oid,
            json_agg(
                json_build_object(
                    'name', a.attname,
                    'type_oid', a.atttypid,
                    'postgres_type', format_type(a.atttypid, a.atttypmod),
                    'position', a.attnum,
                    'not_null', a.attnotnull,
                    'comment', d.description
                ) ORDER BY a.attnum
            ) as fields
        FROM pg_type t
        JOIN pg_attribute a ON a.attrelid = t.typrelid  
        LEFT JOIN pg_description d ON d.objoid = t.typrelid AND d.objsubid = a.attnum
        WHERE t.typnamespace = (
            SELECT oid FROM pg_namespace WHERE nspname = 'tasks'
        )
          AND t.typtype = 'c'
          AND a.attnum > 0
          AND NOT a.attisdropped
        GROUP BY t.typname, t.oid
        ORDER BY t.typname",
        &[]
    )?;
    
    println!("Found {} task types", rows.len());
    for row in &rows {
        let task_name: String = row.get(0);
        let type_oid: u32 = row.get(1);
        let fields_json: serde_json::Value = row.get(2);
        
        println!("\nTask: {} (OID: {})", task_name, type_oid);
        println!("Fields JSON: {}", serde_json::to_string_pretty(&fields_json)?);
    }
    
    Ok(())
}