use pg_query::protobuf::BoolExprType;
use pg_query::{Node, NodeEnum};

/// Information about an enum inferred from a CHECK constraint
#[derive(Debug, Clone)]
pub struct CheckEnumInfo {
    pub column: String,
    pub variants: Vec<String>,
}

/// Parse a CHECK expression to extract enum variants if it matches known patterns.
///
/// Supported patterns:
/// - `CHECK (col IN ('a', 'b', 'c'))`
/// - `CHECK (col = 'a' OR col = 'b' OR col = 'c')`
///
/// Only single-column constraints are supported.
/// Returns None for complex constraints that don't match these patterns.
pub fn parse_check_as_enum(check_expression: &str, columns: &[&str]) -> Option<CheckEnumInfo> {
    // Only process single-column CHECK constraints
    if columns.len() != 1 {
        return None;
    }

    let column = columns[0];

    // Parse the CHECK expression as a SQL statement
    // pg_get_constraintdef returns "CHECK (expr)" so we wrap it in a CREATE TABLE
    let parse_result = pg_query::parse(&format!(
        "CREATE TABLE _t (_c TEXT {})",
        check_expression
    ));

    let parsed = match parse_result {
        Ok(p) => p,
        Err(_) => return None,
    };

    // Navigate to the CHECK constraint expression
    // The constraint is inside a ColumnDef, not at the CreateStmt level
    let constraint_expr = parsed
        .protobuf
        .stmts
        .get(0)
        .and_then(|stmt| stmt.stmt.as_ref())
        .and_then(|stmt| stmt.node.as_ref())
        .and_then(|node| {
            if let pg_query::protobuf::node::Node::CreateStmt(create) = node {
                // Find the ColumnDef and then the CHECK constraint within it
                create.table_elts.iter().find_map(|elt| {
                    if let Some(NodeEnum::ColumnDef(col_def)) = &elt.node {
                        // Look for CHECK constraint in the column's constraints
                        col_def.constraints.iter().find_map(|constraint_node| {
                            if let Some(NodeEnum::Constraint(c)) = &constraint_node.node {
                                if c.contype() == pg_query::protobuf::ConstrType::ConstrCheck {
                                    c.raw_expr.clone()
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                    } else if let Some(NodeEnum::Constraint(c)) = &elt.node {
                        // Also check for table-level constraints
                        if c.contype() == pg_query::protobuf::ConstrType::ConstrCheck {
                            c.raw_expr.clone()
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
            } else {
                None
            }
        })?;

    // Try to extract enum variants from the expression
    extract_enum_variants(&constraint_expr, column)
}

/// Extract enum variants from a parsed expression
fn extract_enum_variants(expr: &Node, expected_column: &str) -> Option<CheckEnumInfo> {
    match &expr.node {
        // Handle IN expression: col IN ('a', 'b', 'c')
        Some(NodeEnum::AExpr(a_expr)) => {
            // Check if this is an IN expression
            if a_expr.kind() == pg_query::protobuf::AExprKind::AexprIn {
                return extract_in_variants(a_expr, expected_column);
            }
            // Check if this is an equality: col = 'value'
            if a_expr.kind() == pg_query::protobuf::AExprKind::AexprOp {
                if is_equality_op(&a_expr.name) {
                    return extract_single_equality(a_expr, expected_column)
                        .map(|v| CheckEnumInfo {
                            column: expected_column.to_string(),
                            variants: vec![v],
                        });
                }
            }
            None
        }
        // Handle OR expression: col = 'a' OR col = 'b'
        Some(NodeEnum::BoolExpr(bool_expr)) => {
            if bool_expr.boolop() == BoolExprType::OrExpr {
                extract_or_chain_variants(bool_expr, expected_column)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Check if an operator name list represents an equality operator
fn is_equality_op(name: &[Node]) -> bool {
    name.iter().any(|n| {
        if let Some(NodeEnum::String(s)) = &n.node {
            s.sval == "="
        } else {
            false
        }
    })
}

/// Extract variants from an IN expression
fn extract_in_variants(
    a_expr: &pg_query::protobuf::AExpr,
    expected_column: &str,
) -> Option<CheckEnumInfo> {
    // Get the column name from lexpr
    let column_name = extract_column_name(a_expr.lexpr.as_ref()?)?;
    if column_name != expected_column {
        return None;
    }

    // Get the list of values from rexpr
    let rexpr = a_expr.rexpr.as_ref()?;
    let values = extract_string_list(rexpr)?;

    if values.is_empty() {
        return None;
    }

    Some(CheckEnumInfo {
        column: column_name,
        variants: values,
    })
}

/// Extract a single value from an equality expression
fn extract_single_equality(
    a_expr: &pg_query::protobuf::AExpr,
    expected_column: &str,
) -> Option<String> {
    let column_name = extract_column_name(a_expr.lexpr.as_ref()?)?;
    if column_name != expected_column {
        return None;
    }

    extract_string_value(a_expr.rexpr.as_ref()?)
}

/// Extract variants from OR chain: col = 'a' OR col = 'b' OR col = 'c'
fn extract_or_chain_variants(
    bool_expr: &pg_query::protobuf::BoolExpr,
    expected_column: &str,
) -> Option<CheckEnumInfo> {
    let mut variants = Vec::new();

    for arg in &bool_expr.args {
        match &arg.node {
            Some(NodeEnum::AExpr(a_expr)) => {
                if a_expr.kind() == pg_query::protobuf::AExprKind::AexprOp && is_equality_op(&a_expr.name) {
                    if let Some(value) = extract_single_equality(a_expr, expected_column) {
                        variants.push(value);
                    } else {
                        // If any equality doesn't match, this isn't a valid enum pattern
                        return None;
                    }
                } else {
                    return None;
                }
            }
            Some(NodeEnum::BoolExpr(inner_bool)) => {
                // Handle nested OR expressions
                if inner_bool.boolop() == BoolExprType::OrExpr {
                    if let Some(info) = extract_or_chain_variants(inner_bool, expected_column) {
                        variants.extend(info.variants);
                    } else {
                        return None;
                    }
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }

    if variants.is_empty() {
        return None;
    }

    Some(CheckEnumInfo {
        column: expected_column.to_string(),
        variants,
    })
}

/// Extract a column name from a node (handles ColumnRef)
fn extract_column_name(node: &Node) -> Option<String> {
    if let Some(NodeEnum::ColumnRef(col_ref)) = &node.node {
        // Get the last field (in case of schema.table.column)
        col_ref.fields.last().and_then(|field| {
            if let Some(NodeEnum::String(s)) = &field.node {
                Some(s.sval.clone())
            } else {
                None
            }
        })
    } else {
        None
    }
}

/// Extract a list of string values from a List node
fn extract_string_list(node: &Node) -> Option<Vec<String>> {
    if let Some(NodeEnum::List(list)) = &node.node {
        let values: Vec<String> = list
            .items
            .iter()
            .filter_map(|item| extract_string_value(item))
            .collect();

        if values.len() == list.items.len() {
            Some(values)
        } else {
            // Not all items were string constants
            None
        }
    } else {
        None
    }
}

/// Extract a string value from a constant node
fn extract_string_value(node: &Node) -> Option<String> {
    match &node.node {
        Some(NodeEnum::AConst(a_const)) => {
            if let Some(val) = &a_const.val {
                if let pg_query::protobuf::a_const::Val::Sval(s) = val {
                    return Some(s.sval.clone());
                }
            }
            None
        }
        Some(NodeEnum::String(s)) => Some(s.sval.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_expression() {
        let expr = "CHECK (status IN ('pending', 'shipped', 'delivered'))";
        let result = parse_check_as_enum(expr, &["status"]);

        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.column, "status");
        assert_eq!(info.variants, vec!["pending", "shipped", "delivered"]);
    }

    #[test]
    fn test_or_chain() {
        let expr = "CHECK (status = 'pending' OR status = 'shipped' OR status = 'delivered')";
        let result = parse_check_as_enum(expr, &["status"]);

        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.column, "status");
        assert_eq!(info.variants, vec!["pending", "shipped", "delivered"]);
    }

    #[test]
    fn test_multi_column_constraint_returns_none() {
        let expr = "CHECK (status IN ('pending', 'shipped'))";
        let result = parse_check_as_enum(expr, &["status", "other"]);

        assert!(result.is_none());
    }

    #[test]
    fn test_complex_constraint_returns_none() {
        let expr = "CHECK (status IN ('pending', 'shipped') AND amount > 0)";
        let result = parse_check_as_enum(expr, &["status"]);

        assert!(result.is_none());
    }

    #[test]
    fn test_wrong_column_returns_none() {
        let expr = "CHECK (other_col IN ('a', 'b'))";
        let result = parse_check_as_enum(expr, &["status"]);

        assert!(result.is_none());
    }

    #[test]
    fn test_numeric_values_return_none() {
        // We only support string enums
        let expr = "CHECK (status IN (1, 2, 3))";
        let result = parse_check_as_enum(expr, &["status"]);

        assert!(result.is_none());
    }
}
