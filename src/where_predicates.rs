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

/// A predicate together with the table qualifier its column was written with
/// (`alias.col` → `Some("alias")`, bare `col` → `None`).
///
/// Deciding which table reference the predicate is about is the caller's job:
/// it knows what is in scope. A qualified column belongs to the relation with
/// that alias and no other; an unqualified one to whichever relation in scope
/// has a column of that name.
#[derive(Debug, Clone)]
pub struct ScopedPredicate {
    pub qualifier: Option<String>,
    pub predicate: Predicate,
}

/// Walk a WHERE expression tree and return the atomic predicates it AND-implies.
pub fn extract_predicates(where_clause: &Node) -> Vec<ScopedPredicate> {
    let mut out = Vec::new();
    walk(where_clause, &mut out);
    out
}

fn walk(node: &Node, out: &mut Vec<ScopedPredicate>) {
    if let Some(NodeEnum::BoolExpr(b)) = &node.node {
        // OR / NOT branches don't yield definite facts. Drop and move on.
        if b.boolop() == BoolExprType::AndExpr {
            for arg in &b.args {
                walk(arg, out);
            }
        }
        return;
    }

    if let Some(pred) = recognize_equality(node).or_else(|| recognize_is_not_null(node)) {
        out.push(pred);
    }
}

/// Match `col IS NOT NULL`. Symmetric to [`recognize_equality`] for the NULL-
/// keyed predicate family.
fn recognize_is_not_null(node: &Node) -> Option<ScopedPredicate> {
    let nt = match &node.node {
        Some(NodeEnum::NullTest(nt)) => nt,
        _ => return None,
    };
    if nt.nulltesttype() != NullTestType::IsNotNull {
        return None;
    }
    let (column, qualifier) = extract_column_ref(nt.arg.as_deref()?)?;
    Some(ScopedPredicate {
        qualifier,
        predicate: Predicate::IsNotNull { column },
    })
}

fn recognize_equality(node: &Node) -> Option<ScopedPredicate> {
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

    build_predicate(l, r).or_else(|| build_predicate(r, l))
}

fn build_predicate(col_side: &Node, lit_side: &Node) -> Option<ScopedPredicate> {
    let (column, qualifier) = extract_column_ref(col_side)?;
    let lit = extract_string_literal(lit_side)?;
    Some(ScopedPredicate {
        qualifier,
        predicate: Predicate::Eq {
            column,
            value: Literal::Text(lit),
        },
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
///
/// Longer references are rejected: `schema.table.col` and `alias.col.attr`
/// look alike here, and reading the latter as a column of a table named
/// `col` would be wrong.
fn extract_column_ref(node: &Node) -> Option<(String, Option<String>)> {
    let inner = match &node.node {
        Some(NodeEnum::TypeCast(tc)) => tc.arg.as_deref()?,
        _ => node,
    };
    let Some(NodeEnum::ColumnRef(col_ref)) = &inner.node else {
        return None;
    };
    let name = |f: &Node| match &f.node {
        Some(NodeEnum::String(s)) => Some(s.sval.clone()),
        _ => None,
    };
    match col_ref.fields.as_slice() {
        [column] => Some((name(column)?, None)),
        [qualifier, column] => Some((name(column)?, Some(name(qualifier)?))),
        _ => None,
    }
}

fn extract_string_literal(node: &Node) -> Option<String> {
    match &node.node {
        Some(NodeEnum::AConst(c)) => {
            if let Some(pg_query::protobuf::a_const::Val::Sval(s)) = &c.val {
                return Some(s.sval.clone());
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
        let preds = extract_predicates(&w);
        assert_eq!(preds.len(), 1);
        assert_eq!(preds[0].qualifier, None);
        match &preds[0].predicate {
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
        let preds = extract_predicates(&w);
        assert_eq!(preds.len(), 1);
    }

    #[test]
    fn splits_and_conjuncts() {
        let w = parse_where("SELECT * FROM t WHERE a = 'x' AND b = 'y' AND c > 0");
        let preds = extract_predicates(&w);
        // c > 0 is dropped (not equality)
        assert_eq!(preds.len(), 2);
    }

    #[test]
    fn drops_or_and_not_branches() {
        let w = parse_where("SELECT * FROM t WHERE a = 'x' OR a = 'y'");
        assert!(
            extract_predicates(&w).is_empty(),
            "OR must not yield definite facts"
        );
        let w = parse_where("SELECT * FROM t WHERE NOT (a = 'x')");
        assert!(extract_predicates(&w).is_empty());
        let w = parse_where("SELECT * FROM t WHERE b = 'y' AND (a = 'x' OR a IS NOT NULL)");
        assert_eq!(extract_predicates(&w).len(), 1);
    }

    #[test]
    fn reports_qualifier() {
        let w = parse_where(
            "SELECT * FROM t p JOIN u ON true \
             WHERE p.product_type = 'paint' AND u.x IS NOT NULL AND y IS NOT NULL",
        );
        let qualifiers: Vec<Option<String>> = extract_predicates(&w)
            .into_iter()
            .map(|p| p.qualifier)
            .collect();
        assert_eq!(
            qualifiers,
            vec![Some("p".to_string()), Some("u".to_string()), None]
        );
    }

    #[test]
    fn rejects_references_longer_than_alias_dot_column() {
        // `a.b.c` is either schema.table.column or alias.column.attribute.
        let w = parse_where("SELECT * FROM t WHERE a.b.c IS NOT NULL AND a.b.c = 'x'");
        assert!(extract_predicates(&w).is_empty());
    }

    #[test]
    fn handles_typecast_literals() {
        // pg_query usually leaves user input alone; this just verifies the cast peeler.
        let w = parse_where("SELECT * FROM t WHERE product_type = 'paint'::text");
        let preds = extract_predicates(&w);
        assert_eq!(preds.len(), 1);
        match &preds[0].predicate {
            Predicate::Eq { value, .. } => {
                assert_eq!(value, &Literal::Text("paint".into()));
            }
            other => panic!("expected Eq predicate, got {:?}", other),
        }
    }

    #[test]
    fn extracts_is_not_null() {
        let w = parse_where("SELECT * FROM t WHERE sync_src IS NOT NULL");
        let preds = extract_predicates(&w);
        assert_eq!(preds.len(), 1);
        match &preds[0].predicate {
            Predicate::IsNotNull { column } => assert_eq!(column, "sync_src"),
            other => panic!("expected IsNotNull predicate, got {:?}", other),
        }
    }

    #[test]
    fn drops_is_null_predicates() {
        // We don't currently emit "IS NULL" as a fact-antecedent shape — only
        // IS NOT NULL. Adding it would require modeling "column IS NULL" facts
        // (e.g., "given product_type = 'paint', mini_material IS NULL").
        let w = parse_where("SELECT * FROM t WHERE sync_src IS NULL");
        let preds = extract_predicates(&w);
        assert!(preds.is_empty());
    }

    #[test]
    fn mixes_eq_and_is_not_null_under_and() {
        let w = parse_where(
            "SELECT * FROM t WHERE product_type = 'paint' AND sync_src IS NOT NULL",
        );
        let preds = extract_predicates(&w);
        assert_eq!(preds.len(), 2);
    }
}
