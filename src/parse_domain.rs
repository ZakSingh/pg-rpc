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
                // A disjunction proves only what *every* branch proves. A
                // `VALUE IS NULL` branch is vacuous here: attribute nullability
                // only matters for non-NULL values, and for those the remaining
                // branches must hold. So `VALUE IS NULL OR (a IS NOT NULL AND
                // b IS NOT NULL)` proves both `a` and `b`, while
                // `VALUE IS NULL OR a IS NOT NULL OR b IS NOT NULL` proves
                // neither on its own.
                let mut proven: Option<HashSet<String>> = None;
                for arg in bool_expr.args.iter().filter(|arg| !is_value_null_test(arg)) {
                    let mut branch = HashSet::new();
                    collect_from_node(arg, &mut branch);
                    proven = Some(match proven {
                        None => branch,
                        Some(acc) => acc.intersection(&branch).cloned().collect(),
                    });
                }
                if let Some(proven) = proven {
                    columns.extend(proven);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

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

    fn names(cols: &[&str]) -> HashSet<String> {
        cols.iter().map(|c| c.to_string()).collect()
    }

    /// `VALUE IS NULL OR (a IS NOT NULL AND b IS NOT NULL)`, as
    /// `pg_get_constraintdef` renders it: for a non-NULL value the conjunction
    /// must hold, so both attributes are proven.
    #[test]
    fn nullable_value_with_conjunction_proves_every_conjunct() {
        let check_strs = vec![
            "CHECK (((VALUE IS NULL) OR (((VALUE).amount IS NOT NULL) AND ((VALUE).unit IS NOT NULL))))",
        ];

        let constraint_nodes = get_constraints(&check_strs).unwrap();
        let non_null_cols = collect_non_null_columns(&constraint_nodes);

        assert_eq!(non_null_cols, names(&["amount", "unit"]));
    }

    /// `VALUE IS NULL OR a IS NOT NULL OR b IS NOT NULL` only proves
    /// "a or b", never either one alone.
    #[test]
    fn disjunction_of_tests_proves_nothing_individually() {
        let check_strs = vec![
            "check (value is null or (value).author is not null or (value).editor is not null)",
        ];

        let constraint_nodes = get_constraints(&check_strs).unwrap();
        let non_null_cols = collect_non_null_columns(&constraint_nodes);

        assert_eq!(non_null_cols, HashSet::new());
    }

    /// A disjunction proves what every branch proves:
    /// `(a AND b) OR (a AND c)` proves `a`.
    #[test]
    fn disjunction_proves_common_attributes() {
        let check_strs = vec![
            "check (((value).a is not null and (value).b is not null) \
                 or ((value).a is not null and (value).c is not null))",
        ];

        let constraint_nodes = get_constraints(&check_strs).unwrap();
        let non_null_cols = collect_non_null_columns(&constraint_nodes);

        assert_eq!(non_null_cols, names(&["a"]));
    }
}
