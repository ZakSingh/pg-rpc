//! Helpers shared by the CHECK-constraint readers
//! ([`crate::parse_check_enum`], [`crate::parse_check_facts`]).

use pg_query::protobuf::ConstrType;
use pg_query::{Node, NodeEnum};

/// Parse the text `pg_get_constraintdef` gives for a CHECK constraint
/// (`CHECK (expr)`) and return `expr`.
///
/// The text is not a statement on its own, so it is parsed as a column
/// constraint of a throwaway `CREATE TABLE`. `None` if it does not parse or
/// holds no CHECK constraint.
pub(crate) fn parse_check_expr(check_expression: &str) -> Option<Node> {
    let parsed =
        pg_query::parse(&format!("CREATE TABLE _t (_c TEXT {})", check_expression)).ok()?;
    let NodeEnum::CreateStmt(create) = parsed.protobuf.stmts.first()?.stmt.as_ref()?.node.as_ref()?
    else {
        return None;
    };

    let check_expr = |node: &Node| match &node.node {
        Some(NodeEnum::Constraint(c)) if c.contype() == ConstrType::ConstrCheck => {
            c.raw_expr.as_deref().cloned()
        }
        _ => None,
    };

    create.table_elts.iter().find_map(|elt| match &elt.node {
        // The constraint hangs off the column definition...
        Some(NodeEnum::ColumnDef(col_def)) => col_def.constraints.iter().find_map(check_expr),
        // ...or, written differently, sits at table level.
        _ => check_expr(elt),
    })
}

/// Whether an operator name (`AExpr.name`) is `=`.
pub(crate) fn is_equality_op(name: &[Node]) -> bool {
    name.iter()
        .any(|n| matches!(&n.node, Some(NodeEnum::String(s)) if s.sval == "="))
}

/// The value of a string literal, looking through casts:
/// `pg_get_constraintdef` writes every literal as `'x'::text` or
/// `'x'::some_enum`.
pub(crate) fn string_literal(node: &Node) -> Option<String> {
    match &node.node {
        Some(NodeEnum::AConst(c)) => match &c.val {
            Some(pg_query::protobuf::a_const::Val::Sval(s)) => Some(s.sval.clone()),
            _ => None,
        },
        Some(NodeEnum::TypeCast(tc)) => tc.arg.as_deref().and_then(string_literal),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_check_expression_out_of_constraintdef_text() {
        let expr = parse_check_expr("CHECK ((status = 'a'::text))").unwrap();
        assert!(matches!(expr.node, Some(NodeEnum::AExpr(_))));

        assert!(parse_check_expr("CHECK (").is_none());
        assert!(parse_check_expr("NOT NULL").is_none());
    }

    #[test]
    fn string_literal_sees_through_casts() {
        let expr = parse_check_expr("CHECK ((status = ('a'::character varying)::text))").unwrap();
        let Some(NodeEnum::AExpr(a)) = &expr.node else {
            panic!("expected AExpr");
        };
        assert!(is_equality_op(&a.name));
        assert_eq!(string_literal(a.rexpr.as_deref().unwrap()).as_deref(), Some("a"));
        assert_eq!(string_literal(a.lexpr.as_deref().unwrap()), None);
    }
}
