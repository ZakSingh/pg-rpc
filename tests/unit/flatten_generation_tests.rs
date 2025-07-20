use pgrpc::pg_type::*;
use std::collections::HashMap;

#[test]
fn test_flatten_code_generation() {
    let mut types = HashMap::new();
    
    // Create address type
    let address_type = PgType::Composite {
        schema: "public".to_string(),
        name: "address".to_string(),
        fields: vec![
            PgField {
                name: "street".to_string(),
                type_oid: 1, // text
                nullable: false,
                comment: None,
                flatten: false,
            },
            PgField {
                name: "city".to_string(),
                type_oid: 1, // text
                nullable: false,
                comment: None,
                flatten: false,
            },
        ],
        comment: None,
    };
    types.insert(100, address_type);
    
    // Create person type with flattened address
    let person_type = PgType::Composite {
        schema: "public".to_string(),
        name: "person".to_string(),
        fields: vec![
            PgField {
                name: "name".to_string(),
                type_oid: 1, // text
                nullable: false,
                comment: None,
                flatten: false,
            },
            PgField {
                name: "addr".to_string(),
                type_oid: 100, // address
                nullable: false,
                comment: Some("@pgrpc_flatten".to_string()),
                flatten: true,
            },
        ],
        comment: None,
    };
    
    // Add text type
    types.insert(1, PgType::Text);
    
    let config = pgrpc::config::Config {
        connection_string: None,
        schemas: vec![],
        types: HashMap::new(),
        exceptions: HashMap::new(),
    };
    
    // Generate Rust code
    let generated = person_type.to_rust(&types, &config);
    
    // Print the generated code for inspection
    println!("Generated code:\n{}", generated);
    
    // Verify that the generated code contains flattened fields
    let generated_str = generated.to_string();
    assert!(generated_str.contains("addr_street"));
    assert!(generated_str.contains("addr_city"));
    assert!(generated_str.contains("TryFrom<tokio_postgres::Row>"));
}