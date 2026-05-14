//! Extract atomic predicates from a WHERE clause expression tree.
//!
//! Returns a flat list of [`crate::parse_check_facts::Predicate`] values
//! representing the conjuncts that pgrpc currently knows how to reason about
//! (today: `col = string_literal`). Anything we don't recognize is silently
//! dropped — callers treat the absence of a predicate as "no extra knowledge",
//! never as a false claim.
//!
//! The contract: every returned predicate is something that *must hold* for any
//! row in the result. AND-conjuncts compose naturally; OR-branches are ignored
//! entirely (we'd have to track per-arm refinements, and the safe default is to
//! drop the disjunction rather than over-claim).

use crate::parse_check_facts::{Literal, Predicate};
use pg_query::protobuf::{AExprKind, BoolExprType, NullTestType};
use pg_query::{Node, NodeEnum};

/// Walk a WHERE expression tree and return the atomic predicates it AND-implies.
///
/// `qualified_alias` is the table alias the WHERE predicate is being attributed
/// to. If a column reference is qualified (`alias.col`), only references whose
/// qualifier matches will produce a predicate. Unqualified references always
/// match — at the call site, the SELECT must have a single FROM table for this
/// to be safe (otherwise the column could come from anywhere).
pub fn extract_predicates(where_clause: &Node, qualified_alias: Option<&str>) -> Vec<Predicate> {
    let mut out = Vec::new();
    walk(where_clause, qualified_alias, &mut out);
    out
}

fn walk(node: &Node, qualified_alias: Option<&str>, out: &mut Vec<Predicate>) {
    if let Some(NodeEnum::BoolExpr(b)) = &node.node {
        match b.boolop() {
            BoolExprType::AndExpr => {
                for arg in &b.args {
                    walk(arg, qualified_alias, out);
                }
                return;
            }
            // OR / NOT branches don't yield definite facts. Drop and move on.
            _ => return,
        }
    }

    if let Some(pred) = recognize_equality(node, qualified_alias) {
        out.push(pred);
        return;
    }

    if let Some(pred) = recognize_is_not_null(node, qualified_alias) {
        out.push(pred);
    }
}

/// Match `col IS NOT NULL`. Symmetric to [`recognize_equality`] for the NULL-
/// keyed predicate family.
fn recognize_is_not_null(node: &Node, qualified_alias: Option<&str>) -> Option<Predicate> {
    let nt = match &node.node {
        Some(NodeEnum::NullTest(nt)) => nt,
        _ => return None,
    };
    if nt.nulltesttype() != NullTestType::IsNotNull {
        return None;
    }
    let arg = nt.arg.as_deref()?;
    let (col_name, col_qualifier) = extract_column_ref(arg)?;
    if let (Some(want), Some(got)) = (qualified_alias, col_qualifier.as_deref()) {
        if want != got {
            return None;
        }
    }
    Some(Predicate::IsNotNull { column: col_name })
}

fn recognize_equality(node: &Node, qualified_alias: Option<&str>) -> Option<Predicate> {
    let a = match &node.node {
        Some(NodeEnum::AExpr(a)) => a,
        _ => return None,
    };
    if a.kind() != AExprKind::AexprOp || !is_equality_op(&a.name) {
        return None;
    }

    // The literal can be on either side: `col = 'x'` or `'x' = col`.
    let l = a.lexpr.as_deref()?;
    let r = a.rexpr.as_deref()?;

    if let Some(p) = build_predicate(l, r, qualified_alias) {
        return Some(p);
    }
    build_predicate(r, l, qualified_alias)
}

fn build_predicate(
    col_side: &Node,
    lit_side: &Node,
    qualified_alias: Option<&str>,
) -> Option<Predicate> {
    let (col_name, col_qualifier) = extract_column_ref(col_side)?;
    if let (Some(want), Some(got)) = (qualified_alias, col_qualifier.as_deref()) {
        if want != got {
            return None;
        }
    }
    let lit = extract_string_literal(lit_side)?;
    Some(Predicate::Eq {
        column: col_name,
        value: Literal::Text(lit),
    })
}

fn is_equality_op(name: &[Node]) -> bool {
    name.iter().any(|n| {
        if let Some(NodeEnum::String(s)) = &n.node {
            s.sval == "="
        } else {
            false
        }
    })
}

/// Returns `(column_name, qualifier)`. Qualifier is the alias/table name if the
/// reference was `alias.col`; None for bare `col`. Strips TypeCast wrappers.
fn extract_column_ref(node: &Node) -> Option<(String, Option<String>)> {
    let inner = match &node.node {
        Some(NodeEnum::TypeCast(tc)) => tc.arg.as_deref()?,
        _ => node,
    };
    let col_ref = if let Some(NodeEnum::ColumnRef(c)) = &inner.node {
        c
    } else {
        return None;
    };
    let mut names: Vec<String> = col_ref
        .fields
        .iter()
        .filter_map(|f| {
            if let Some(NodeEnum::String(s)) = &f.node {
                Some(s.sval.clone())
            } else {
                None
            }
        })
        .collect();
    let column = names.pop()?;
    let qualifier = names.pop(); // schema is below this; we ignore schema-qualified refs
    Some((column, qualifier))
}

fn extract_string_literal(node: &Node) -> Option<String> {
    match &node.node {
        Some(NodeEnum::AConst(c)) => {
            if let Some(val) = &c.val {
                if let pg_query::protobuf::a_const::Val::Sval(s) = val {
                    return Some(s.sval.clone());
                }
            }
            None
        }
        Some(NodeEnum::TypeCast(tc)) => tc.arg.as_deref().and_then(extract_string_literal),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_where(sql: &str) -> Node {
        // Use a dummy SELECT to surface a where_clause Node.
        let parsed = pg_query::parse(sql).expect("parse");
        let stmt = parsed.protobuf.stmts.first().expect("stmt");
        let stmt_node = stmt.stmt.as_ref().expect("inner");
        let select = match stmt_node.node.as_ref().expect("node") {
            pg_query::protobuf::node::Node::SelectStmt(s) => s,
            _ => panic!("not a select"),
        };
        *select.where_clause.clone().expect("where").clone()
    }

    #[test]
    fn extracts_single_equality() {
        let w = parse_where("SELECT * FROM t WHERE product_type = 'paint'");
        let preds = extract_predicates(&w, None);
        assert_eq!(preds.len(), 1);
        match &preds[0] {
            Predicate::Eq { column, value } => {
                assert_eq!(column, "product_type");
                assert_eq!(value, &Literal::Text("paint".into()));
            }
            other => panic!("expected Eq predicate, got {:?}", other),
        }
    }

    #[test]
    fn handles_literal_on_left() {
        let w = parse_where("SELECT * FROM t WHERE 'paint' = product_type");
        let preds = extract_predicates(&w, None);
        assert_eq!(preds.len(), 1);
    }

    #[test]
    fn splits_and_conjuncts() {
        let w = parse_where("SELECT * FROM t WHERE a = 'x' AND b = 'y' AND c > 0");
        let preds = extract_predicates(&w, None);
        // c > 0 is dropped (not equality)
        assert_eq!(preds.len(), 2);
    }

    #[test]
    fn drops_or_branches() {
        let w = parse_where("SELECT * FROM t WHERE a = 'x' OR a = 'y'");
        let preds = extract_predicates(&w, None);
        assert!(preds.is_empty(), "OR must not yield definite facts");
    }

    #[test]
    fn respects_qualifier() {
        let w = parse_where("SELECT * FROM t p WHERE p.product_type = 'paint'");
        let matched = extract_predicates(&w, Some("p"));
        assert_eq!(matched.len(), 1);
        let mismatched = extract_predicates(&w, Some("other"));
        assert!(mismatched.is_empty());
    }

    #[test]
    fn unqualified_column_accepted_with_any_alias() {
        let w = parse_where("SELECT * FROM t WHERE product_type = 'paint'");
        let with_alias = extract_predicates(&w, Some("anything"));
        assert_eq!(with_alias.len(), 1);
    }

    #[test]
    fn handles_typecast_literals() {
        // pg_query usually leaves user input alone; this just verifies the cast peeler.
        let w = parse_where("SELECT * FROM t WHERE product_type = 'paint'::text");
        let preds = extract_predicates(&w, None);
        assert_eq!(preds.len(), 1);
        match &preds[0] {
            Predicate::Eq { value, .. } => {
                assert_eq!(value, &Literal::Text("paint".into()));
            }
            other => panic!("expected Eq predicate, got {:?}", other),
        }
    }

    #[test]
    fn extracts_is_not_null() {
        let w = parse_where("SELECT * FROM t WHERE sync_src IS NOT NULL");
        let preds = extract_predicates(&w, None);
        assert_eq!(preds.len(), 1);
        match &preds[0] {
            Predicate::IsNotNull { column } => assert_eq!(column, "sync_src"),
            other => panic!("expected IsNotNull predicate, got {:?}", other),
        }
    }

    #[test]
    fn extracts_is_not_null_with_qualifier() {
        let w = parse_where("SELECT * FROM t i WHERE i.sync_src IS NOT NULL");
        let preds = extract_predicates(&w, Some("i"));
        assert_eq!(preds.len(), 1);
        let no_match = extract_predicates(&w, Some("other"));
        assert!(no_match.is_empty());
    }

    #[test]
    fn drops_is_null_predicates() {
        // We don't currently emit "IS NULL" as a fact-antecedent shape — only
        // IS NOT NULL. Adding it would require modeling "column IS NULL" facts
        // (e.g., "given product_type = 'paint', mini_material IS NULL").
        let w = parse_where("SELECT * FROM t WHERE sync_src IS NULL");
        let preds = extract_predicates(&w, None);
        assert!(preds.is_empty());
    }

    #[test]
    fn mixes_eq_and_is_not_null_under_and() {
        let w = parse_where(
            "SELECT * FROM t WHERE product_type = 'paint' AND sync_src IS NOT NULL",
        );
        let preds = extract_predicates(&w, None);
        assert_eq!(preds.len(), 2);
    }
}
