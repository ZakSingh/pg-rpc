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
                for arg in &bool_expr.args {
                    collect_from_node(arg, columns);
                }
            }
        }
        Some(NodeEnum::NullTest(null_test)) => {
            if null_test.nulltesttype() == NullTestType::IsNotNull {
                if let Some(arg) = &null_test.arg {
                    collect_from_node(arg, columns);
                }
            }
        }
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
        }
        _ => {}
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
}
