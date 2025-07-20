use crate::codegen::OID;
use crate::pg_type::{PgType, PgField};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone)]
pub struct FlattenedField {
    pub name: String,
    pub original_path: Vec<String>,
    pub type_oid: OID,
    pub nullable: bool,
    pub comment: Option<String>,
}

#[derive(Debug)]
pub struct FlattenAnalysis {
    pub flattened_fields: Vec<FlattenedField>,
    pub dependencies: HashSet<OID>,
}

#[derive(Debug)]
pub enum FlattenError {
    CyclicDependency(Vec<String>),
    InvalidFlattenTarget(String),
    TypeNotFound(OID),
}

/// Analyzes a composite type and returns the flattened field structure
pub fn analyze_flatten_dependencies(
    composite_type: &PgType,
    types: &HashMap<OID, PgType>,
) -> Result<FlattenAnalysis, FlattenError> {
    let mut visited = HashSet::new();
    let mut path = Vec::new();
    let mut dependencies = HashSet::new();
    
    let flattened_fields = flatten_composite_recursive(
        composite_type,
        types,
        &mut visited,
        &mut path,
        &mut dependencies,
    )?;
    
    Ok(FlattenAnalysis {
        flattened_fields,
        dependencies,
    })
}

fn flatten_composite_recursive(
    composite_type: &PgType,
    types: &HashMap<OID, PgType>,
    visited: &mut HashSet<String>,
    path: &mut Vec<String>,
    dependencies: &mut HashSet<OID>,
) -> Result<Vec<FlattenedField>, FlattenError> {
    let type_name = match composite_type {
        PgType::Composite { name, .. } => name.clone(),
        _ => return Err(FlattenError::InvalidFlattenTarget(
            "Only composite types can be flattened".to_string()
        )),
    };
    
    // Cycle detection
    if visited.contains(&type_name) {
        return Err(FlattenError::CyclicDependency(path.clone()));
    }
    
    visited.insert(type_name.clone());
    path.push(type_name.clone());
    
    let mut result = Vec::new();
    
    if let PgType::Composite { fields, .. } = composite_type {
        for field in fields {
            if field.flatten {
                // This field should be flattened
                let field_type = types.get(&field.type_oid)
                    .ok_or(FlattenError::TypeNotFound(field.type_oid))?;
                
                dependencies.insert(field.type_oid);
                
                // Recursively flatten this field
                let sub_fields = flatten_composite_recursive(
                    field_type,
                    types,
                    visited,
                    path,
                    dependencies,
                )?;
                
                // Add sub-fields with updated paths and names
                for sub_field in sub_fields {
                    let mut new_path = vec![field.name.clone()];
                    new_path.extend(sub_field.original_path);
                    
                    // Generate field name with conflict resolution
                    let field_name = generate_field_name(&field.name, &sub_field.name);
                    
                    result.push(FlattenedField {
                        name: field_name,
                        original_path: new_path,
                        type_oid: sub_field.type_oid,
                        nullable: field.nullable || sub_field.nullable,
                        comment: sub_field.comment,
                    });
                }
            } else {
                // This field should not be flattened, include as-is
                result.push(FlattenedField {
                    name: field.name.clone(),
                    original_path: vec![field.name.clone()],
                    type_oid: field.type_oid,
                    nullable: field.nullable,
                    comment: field.comment.clone(),
                });
            }
        }
    }
    
    path.pop();
    visited.remove(&type_name);
    
    Ok(result)
}

/// Generate a field name, handling potential conflicts
fn generate_field_name(parent_name: &str, field_name: &str) -> String {
    // For now, simple concatenation with underscore
    // TODO: Implement more sophisticated conflict resolution
    if parent_name.is_empty() {
        field_name.to_string()
    } else {
        format!("{}_{}", parent_name, field_name)
    }
}

/// Check if a field name would conflict with existing fields
pub fn has_field_conflict(new_name: &str, existing_fields: &[FlattenedField]) -> bool {
    existing_fields.iter().any(|f| f.name == new_name)
}

/// Resolve field name conflicts by adding prefixes
pub fn resolve_field_conflicts(fields: &mut [FlattenedField]) {
    let mut name_counts: HashMap<String, usize> = HashMap::new();
    
    for field in fields.iter_mut() {
        let count = name_counts.entry(field.name.clone()).or_insert(0);
        *count += 1;
        
        if *count > 1 {
            // Add suffix for conflicts
            field.name = format!("{}_{}", field.name, count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_type::{PgType, PgField};
    
    fn create_test_field(name: &str, type_oid: OID, flatten: bool) -> PgField {
        PgField {
            name: name.to_string(),
            type_oid,
            nullable: true,
            comment: None,
            flatten,
        }
    }
    
    fn create_test_composite(name: &str, fields: Vec<PgField>) -> PgType {
        PgType::Composite {
            schema: "public".to_string(),
            name: name.to_string(),
            fields,
            comment: None,
        }
    }
    
    #[test]
    fn test_simple_flatten() {
        let mut types = HashMap::new();
        
        // Create address type
        let address_type = create_test_composite("address", vec![
            create_test_field("street", 1, false),
            create_test_field("city", 2, false),
        ]);
        types.insert(100, address_type);
        
        // Create person type with flattened address
        let person_type = create_test_composite("person", vec![
            create_test_field("name", 3, false),
            create_test_field("addr", 100, true), // Flatten this
        ]);
        
        let analysis = analyze_flatten_dependencies(&person_type, &types).unwrap();
        
        assert_eq!(analysis.flattened_fields.len(), 3);
        assert_eq!(analysis.flattened_fields[0].name, "name");
        assert_eq!(analysis.flattened_fields[1].name, "addr_street");
        assert_eq!(analysis.flattened_fields[2].name, "addr_city");
        
        assert!(analysis.dependencies.contains(&100));
    }
    
    #[test]
    fn test_nested_flatten() {
        let mut types = HashMap::new();
        
        // Create contact_info type
        let contact_type = create_test_composite("contact_info", vec![
            create_test_field("phone", 1, false),
            create_test_field("email", 2, false),
        ]);
        types.insert(200, contact_type);
        
        // Create address type with flattened contact
        let address_type = create_test_composite("address", vec![
            create_test_field("street", 3, false),
            create_test_field("city", 4, false),
            create_test_field("contact", 200, true), // Flatten this
        ]);
        types.insert(100, address_type);
        
        // Create person type with flattened address (which contains flattened contact)
        let person_type = create_test_composite("person", vec![
            create_test_field("name", 5, false),
            create_test_field("addr", 100, true), // Flatten this
        ]);
        
        let analysis = analyze_flatten_dependencies(&person_type, &types).unwrap();
        
        assert_eq!(analysis.flattened_fields.len(), 5);
        assert_eq!(analysis.flattened_fields[0].name, "name");
        assert_eq!(analysis.flattened_fields[1].name, "addr_street");
        assert_eq!(analysis.flattened_fields[2].name, "addr_city");
        assert_eq!(analysis.flattened_fields[3].name, "addr_contact_phone");
        assert_eq!(analysis.flattened_fields[4].name, "addr_contact_email");
        
        assert!(analysis.dependencies.contains(&100));
        assert!(analysis.dependencies.contains(&200));
    }
    
    #[test]
    fn test_cycle_detection() {
        let mut types = HashMap::new();
        
        // Create circular dependency: A -> B -> A
        let type_a = create_test_composite("type_a", vec![
            create_test_field("field_b", 200, true),
        ]);
        types.insert(100, type_a);
        
        let type_b = create_test_composite("type_b", vec![
            create_test_field("field_a", 100, true),
        ]);
        types.insert(200, type_b);
        
        let result = analyze_flatten_dependencies(&types[&100], &types);
        
        assert!(matches!(result, Err(FlattenError::CyclicDependency(_))));
    }
    
    #[test]
    fn test_field_name_generation() {
        assert_eq!(generate_field_name("addr", "street"), "addr_street");
        assert_eq!(generate_field_name("", "name"), "name");
        assert_eq!(generate_field_name("contact", "phone"), "contact_phone");
    }
    
    #[test]
    fn test_conflict_resolution() {
        let mut fields = vec![
            FlattenedField {
                name: "name".to_string(),
                original_path: vec!["name".to_string()],
                type_oid: 1,
                nullable: false,
                comment: None,
            },
            FlattenedField {
                name: "name".to_string(),
                original_path: vec!["contact".to_string(), "name".to_string()],
                type_oid: 2,
                nullable: false,
                comment: None,
            },
        ];
        
        resolve_field_conflicts(&mut fields);
        
        assert_eq!(fields[0].name, "name");
        assert_eq!(fields[1].name, "name_2");
    }
}