use itertools::Itertools;
use pg_query::protobuf::{BoolExprType, NullTestType};
use pg_query::{Node, NodeEnum};
use std::collections::HashSet;

/// Retrieve the set of non-null column names from a given vec of check constraints in PGSQL DDL, i.e.
/// `[check (...), check (...), check (...)]`
/// Fails if the check constraints fail to parse. This should (probably) never happen, as the check
/// constraints come directly from an existing postgres DB which already validated them.
pub fn non_null_cols_from_checks(check_strs: &[&str]) -> anyhow::Result<HashSet<String>> {
    let constraints = get_constraints(check_strs)?;
    Ok(collect_non_null_columns(&constraints))
}

/// Extract the array of parsed constraint nodes from a vector of check constraint DDL strings
fn get_constraints(check_strs: &[&str]) -> anyhow::Result<Vec<Node>> {
    pg_query::parse(&format!(
        "create domain test as _test {}",
        check_strs.iter().join("")
    ))?
    .protobuf
    .stmts
    .get(0) // get the 'create domain' statement
    .and_then(|stmt| stmt.stmt.as_ref())
    .and_then(|stmt| stmt.node.as_ref())
    .and_then(|node| {
        if let pg_query::protobuf::node::Node::CreateDomainStmt(d) = node {
            Some(d.constraints.clone())
        } else {
            None
        }
    })
    .ok_or_else(|| anyhow::anyhow!("Failed to parse domain with constraints"))
}

fn collect_non_null_columns(constraints: &[Node]) -> HashSet<String> {
    let mut columns = HashSet::new();

    for constraint in constraints {
        if let Some(NodeEnum::Constraint(constraint)) = &constraint.node {
            if let Some(expr) = &constraint.raw_expr {
                collect_from_node(expr, &mut columns);
            }
        }
    }

    columns
}

/// Collect non-null columns recursively through the DDL AST.
/// Limitations:
///   Only supports `is not null` tests upon the `value`'s direct fields.
fn collect_from_node(node: &Node, columns: &mut HashSet<String>) {
    match &node.node {
        Some(NodeEnum::BoolExpr(bool_expr)) => {
            if bool_expr.boolop() == BoolExprType::AndExpr {
                // For AND, all conditions must be satisfied
                for arg in &bool_expr.args {
                    collect_from_node(arg, columns);
                }
            } else if bool_expr.boolop() == BoolExprType::OrExpr {
                // For OR, we can only infer not-null constraints in specific cases

                // Check if any branch is testing if value is null
                let null_branches: Vec<_> = bool_expr.args.iter()
                                                     .enumerate()
                                                     .filter(|(_, arg)| is_value_null_test(arg))
                                                     .map(|(idx, _)| idx)
                                                     .collect();

                // If exactly one branch tests value is null, we can analyze other branches
                if null_branches.len() == 1 {
                    let null_branch_idx = null_branches[0];

                    // For all other branches, collect ONLY direct not-null tests
                    // We cannot infer anything about fields in complex expressions
                    let mut direct_not_nulls = HashSet::new();

                    for (idx, arg) in bool_expr.args.iter().enumerate() {
                        if idx != null_branch_idx {
                            // Only collect fields that are directly tested with is not null
                            collect_direct_not_nulls(arg, &mut direct_not_nulls);
                        }
                    }

                    // Only add these fields if every non-null branch has a direct not-null test
                    // If there are other conditions, we can't make the inference
                    if direct_not_nulls.len() > 0 &&
                      bool_expr.args.len() - 1 == bool_expr.args.iter().enumerate()
                                                           .filter(|(idx, _)| *idx != null_branch_idx)
                                                           .filter(|(_, arg)| is_direct_not_null_test(arg))
                                                           .count() {
                        columns.extend(direct_not_nulls);
                    }
                }
            }
        },
        Some(NodeEnum::NullTest(null_test)) => {
            if null_test.nulltesttype() == NullTestType::IsNotNull {
                if let Some(arg) = &null_test.arg {
                    collect_from_node(arg, columns);
                }
            }
        },
        Some(NodeEnum::AIndirection(a_ind)) => {
            // e.g. extract `field` from (value).field
            if let Some(arg) = &a_ind.arg {
                if let Some(NodeEnum::ColumnRef(col_ref)) = &arg.node {
                    if let Some(NodeEnum::String(value)) = &col_ref.fields[0].node {
                        if value.sval.to_lowercase() == "value" {
                            if let Some(NodeEnum::String(field)) = &a_ind.indirection[0].node {
                                columns.insert(field.sval.clone());
                            }
                        }
                    }
                }
            }
        },
        _ => {}
    }
}

// Check if this is a "value is null" test
fn is_value_null_test(node: &Node) -> bool {
    if let Some(NodeEnum::NullTest(null_test)) = &node.node {
        if null_test.nulltesttype() == NullTestType::IsNull {
            if let Some(arg) = &null_test.arg {
                if let Some(NodeEnum::ColumnRef(col_ref)) = &arg.node {
                    if col_ref.fields.len() == 1 {
                        if let Some(NodeEnum::String(value)) = &col_ref.fields[0].node {
                            return value.sval.to_lowercase() == "value";
                        }
                    }
                }
            }
        }
    }
    false
}

// Check if this is a direct "(value).field is not null" test
fn is_direct_not_null_test(node: &Node) -> bool {
    if let Some(NodeEnum::NullTest(null_test)) = &node.node {
        if null_test.nulltesttype() == NullTestType::IsNotNull {
            if let Some(arg) = &null_test.arg {
                if let Some(NodeEnum::AIndirection(a_ind)) = &arg.node {
                    if let Some(inner_arg) = &a_ind.arg {
                        if let Some(NodeEnum::ColumnRef(col_ref)) = &inner_arg.node {
                            if col_ref.fields.len() == 1 {
                                if let Some(NodeEnum::String(value)) = &col_ref.fields[0].node {
                                    return value.sval.to_lowercase() == "value";
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    false
}

// Collect only fields that are directly tested with "(value).field is not null"
fn collect_direct_not_nulls(node: &Node, columns: &mut HashSet<String>) {
    match &node.node {
        Some(NodeEnum::NullTest(null_test)) => {
            if null_test.nulltesttype() == NullTestType::IsNotNull {
                if let Some(arg) = &null_test.arg {
                    if let Some(NodeEnum::AIndirection(a_ind)) = &arg.node {
                        if let Some(inner_arg) = &a_ind.arg {
                            if let Some(NodeEnum::ColumnRef(col_ref)) = &inner_arg.node {
                                if col_ref.fields.len() == 1 {
                                    if let Some(NodeEnum::String(value)) = &col_ref.fields[0].node {
                                        if value.sval.to_lowercase() == "value" {
                                            if let Some(NodeEnum::String(field)) = &a_ind.indirection[0].node {
                                                columns.insert(field.sval.clone());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        Some(NodeEnum::BoolExpr(bool_expr)) => {
            if bool_expr.boolop() == BoolExprType::AndExpr {
                // For AND expressions within an OR, we can still collect direct not-nulls
                for arg in &bool_expr.args {
                    collect_direct_not_nulls(arg, columns);
                }
            }
            // Ignore nested OR expressions
        },
        _ => {},
    }
}

mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let check_strs = vec![
            "check (
              (value).post_id is not null and
              (((value).author is not null) or ((value).age is not null))
              and ((value).title is not null))",
            "check ((value).description is not null)",
        ];

        let constraint_nodes = get_constraints(&check_strs).unwrap();
        let non_null_cols = collect_non_null_columns(&constraint_nodes);

        let mut set = HashSet::new();
        set.insert("post_id".to_string());
        set.insert("title".to_string());
        set.insert("description".to_string());
        assert_eq!(non_null_cols, set);
    }

    #[test]
    fn nested_works() {
        let check_strs = vec![
            "check (
              (value).author.post.id is not null
             )",
        ];

        let constraint_nodes = get_constraints(&check_strs).unwrap();
        let non_null_cols = collect_non_null_columns(&constraint_nodes);

        let mut set = HashSet::new();
        set.insert("author".to_string());
        assert_eq!(non_null_cols, set);
    }

    #[test]
    fn nullable_value_works() {
        let check_strs = vec![
            "check (
              value is null or
              (value).author is not null
             )",
        ];

        let constraint_nodes = get_constraints(&check_strs).unwrap();
        let non_null_cols = collect_non_null_columns(&constraint_nodes);

        let mut set = HashSet::new();
        set.insert("author".to_string());
        assert_eq!(non_null_cols, set);
    }

    #[test]
    fn nullable_value_works_2() {
        let check_strs = vec![
            "check (value is null or (value).product_type <> 'miniature' or (value).mini_is_terrain is not null)",
        ];

        let constraint_nodes = get_constraints(&check_strs).unwrap();
        let non_null_cols = collect_non_null_columns(&constraint_nodes);

        let set = HashSet::new();
        assert_eq!(non_null_cols, set);
    }
}
