//! Extract "predicate facts" from CHECK constraints.
//!
//! A fact has the form: *under predicate P, column C is guaranteed NOT NULL*.
//! Today we recognize the biconditional shape that STI-style schemas use:
//!
//! ```sql
//! CHECK ((discriminator = 'X') = (target_column IS NOT NULL))
//! ```
//!
//! pg_get_constraintdef emits literals as `'X'::text` (or `'X'::enum_type`) and
//! often wraps subexpressions in extra parens. We accept those normalizations.
//!
//! This is intentionally a pattern-matcher, not a theorem prover — anything we
//! don't recognize is silently dropped, mirroring the philosophy of
//! [`crate::parse_check_enum`].

use pg_query::protobuf::{AExprKind, BoolExprType, NullTestType};
use pg_query::{Node, NodeEnum};

/// A literal value extracted from a CHECK or WHERE clause. Today we only handle
/// string literals (the form `pg_get_constraintdef` emits for enums and text),
/// which covers the STI discriminator case. Numeric literals would be easy to
/// add later but aren't useful for current targets.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Literal {
    Text(String),
}

/// Antecedent of a fact: a conjunction of atomic predicates that must all hold
/// for the fact's consequent to fire. Today we only build single-atom
/// antecedents from CHECKs, but the type is structured to handle conjunctions
/// once we add more patterns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Predicate {
    /// `column = literal`
    Eq { column: String, value: Literal },
    /// `column IS NOT NULL`
    IsNotNull { column: String },
}

/// A derived nullability fact: under [`Predicate`], `column` is provably NOT NULL.
#[derive(Debug, Clone)]
pub struct NotNullFact {
    pub when: Predicate,
    pub column: String,
}

/// Parse a single CHECK expression (the `pg_get_constraintdef` text) and emit
/// any [`NotNullFact`]s it implies. Returns an empty vec for unrecognized shapes.
pub fn parse_check_as_facts(check_expression: &str) -> Vec<NotNullFact> {
    // Wrap in CREATE TABLE so pg_query parses it as a constraint expression,
    // same trick parse_check_enum uses.
    let parse_result =
        pg_query::parse(&format!("CREATE TABLE _t (_c TEXT {})", check_expression));
    let parsed = match parse_result {
        Ok(p) => p,
        Err(_) => return Vec::new(),
    };

    let expr = parsed
        .protobuf
        .stmts
        .get(0)
        .and_then(|stmt| stmt.stmt.as_ref())
        .and_then(|stmt| stmt.node.as_ref())
        .and_then(|node| {
            if let pg_query::protobuf::node::Node::CreateStmt(create) = node {
                create.table_elts.iter().find_map(|elt| {
                    if let Some(NodeEnum::ColumnDef(col_def)) = &elt.node {
                        col_def.constraints.iter().find_map(|c| {
                            if let Some(NodeEnum::Constraint(c)) = &c.node {
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
        });

    let expr = match expr {
        Some(e) => e,
        None => return Vec::new(),
    };

    let mut facts = Vec::new();
    collect_facts(&expr, &mut facts);
    facts
}

fn collect_facts(node: &Node, out: &mut Vec<NotNullFact>) {
    // AND nodes split into independent fact sources — each conjunct is its own
    // candidate constraint as far as we're concerned.
    if let Some(NodeEnum::BoolExpr(b)) = &node.node {
        if b.boolop() == BoolExprType::AndExpr {
            for arg in &b.args {
                collect_facts(arg, out);
            }
            return;
        }
    }

    // `CASE discriminator WHEN 'X' THEN (col IS NOT NULL AND …)` — produces
    // one fact per IS NOT NULL conjunct per arm. Treated first because the
    // CASE shape is a top-level constraint by itself, not something to be
    // decomposed via the other recognizers.
    let case_facts = recognize_case_facts(node);
    if !case_facts.is_empty() {
        out.extend(case_facts);
        return;
    }

    // `(eq) = (col IS NOT NULL)` — STI-style biconditional, equality-keyed.
    if let Some(fact) = recognize_eq_biconditional(node) {
        out.push(fact);
        return;
    }

    // `(a IS NULL) = (b IS NULL)` — NULL-keyed biconditional. Emits a fact in
    // each direction since either column being non-NULL implies the other is.
    let null_biconditional = recognize_null_biconditional(node);
    if !null_biconditional.is_empty() {
        out.extend(null_biconditional);
        return;
    }

    // `NOT (eq) OR (col IS NOT NULL)` — one-way implication, equality-keyed.
    if let Some(fact) = recognize_eq_implication(node) {
        out.push(fact);
        return;
    }

    // `(a IS NULL) OR (b IS NOT NULL)` — one-way implication, NULL-keyed.
    if let Some(fact) = recognize_null_implication(node) {
        out.push(fact);
    }
}

/// Match `(col = literal) = (other IS NOT NULL)` — pg_get_constraintdef's
/// normalized form for the STI biconditional. The "=" operator here is
/// boolean-equality, which `pg_query` represents as `AExpr` with op `=`.
fn recognize_eq_biconditional(node: &Node) -> Option<NotNullFact> {
    let (l, r) = split_top_equality(node)?;

    // Either side can be the equality and the other the IS NOT NULL — try both
    // orderings before giving up.
    if let Some(fact) = try_eq_biconditional_arms(l, r) {
        return Some(fact);
    }
    try_eq_biconditional_arms(r, l)
}

/// Match `(a IS NULL) = (b IS NULL)`. Returns *both* directional facts since
/// the biconditional means each column being NOT NULL implies the other is.
///
/// We deliberately only recognize the symmetric `IS NULL = IS NULL` form, not
/// the mixed `IS NULL = IS NOT NULL` form. The mixed form is semantically
/// "exactly one of these is non-NULL" — a different (XOR) shape that needs a
/// different fact model to express correctly.
fn recognize_null_biconditional(node: &Node) -> Vec<NotNullFact> {
    let Some((l, r)) = split_top_equality(node) else {
        return Vec::new();
    };
    let Some(a) = extract_is_null_column(l) else {
        return Vec::new();
    };
    let Some(b) = extract_is_null_column(r) else {
        return Vec::new();
    };
    vec![
        NotNullFact {
            when: Predicate::IsNotNull { column: a.clone() },
            column: b.clone(),
        },
        NotNullFact {
            when: Predicate::IsNotNull { column: b },
            column: a,
        },
    ]
}

fn try_eq_biconditional_arms(eq_side: &Node, nulltest_side: &Node) -> Option<NotNullFact> {
    let pred = extract_equality_predicate(eq_side)?;
    let nullable_col = extract_is_not_null_column(nulltest_side)?;
    Some(NotNullFact {
        when: pred,
        column: nullable_col,
    })
}

/// Return (lexpr, rexpr) iff `node` is an AExpr with the `=` operator —
/// the shape pg_query gives biconditionals at any level of nesting.
fn split_top_equality(node: &Node) -> Option<(&Node, &Node)> {
    let a = match &node.node {
        Some(NodeEnum::AExpr(a)) => a,
        _ => return None,
    };
    if a.kind() != AExprKind::AexprOp || !is_equality_op(&a.name) {
        return None;
    }
    Some((a.lexpr.as_deref()?, a.rexpr.as_deref()?))
}

/// Match `col IS NULL`, returning the column name. Mirrors
/// [`extract_is_not_null_column`] for the negated test.
fn extract_is_null_column(node: &Node) -> Option<String> {
    let node = unwrap_paren(node);
    let nt = match node.node.as_ref()? {
        NodeEnum::NullTest(nt) => nt,
        _ => return None,
    };
    if nt.nulltesttype() != NullTestType::IsNull {
        return None;
    }
    extract_column_name(nt.arg.as_deref()?)
}

/// Match `col = literal` (or `col IS NOT DISTINCT FROM literal`) as an atomic
/// equality predicate.
fn extract_equality_predicate(node: &Node) -> Option<Predicate> {
    let a = match unwrap_paren(node).node.as_ref()? {
        NodeEnum::AExpr(a) => a,
        _ => return None,
    };
    if a.kind() != AExprKind::AexprOp || !is_equality_op(&a.name) {
        return None;
    }
    let col = extract_column_name(a.lexpr.as_deref()?)
        .or_else(|| extract_column_name(a.rexpr.as_deref()?))?;
    // The literal can be on either side.
    let lit = extract_string_literal(a.rexpr.as_deref()?)
        .or_else(|| extract_string_literal(a.lexpr.as_deref()?))?;
    Some(Predicate::Eq {
        column: col,
        value: Literal::Text(lit),
    })
}

/// Match `col IS NOT NULL`, returning the column name.
fn extract_is_not_null_column(node: &Node) -> Option<String> {
    let node = unwrap_paren(node);
    let nt = match node.node.as_ref()? {
        NodeEnum::NullTest(nt) => nt,
        _ => return None,
    };
    if nt.nulltesttype() != NullTestType::IsNotNull {
        return None;
    }
    extract_column_name(nt.arg.as_deref()?)
}

/// Recognize the CASE-keyed CHECK pattern:
///
/// ```sql
/// CASE discriminator
///   WHEN 'X' THEN (col1 IS NOT NULL AND col2 IS NOT NULL)
///   WHEN 'Y' THEN (col3 IS NOT NULL)
///   ELSE true
/// END
/// ```
///
/// Each WHEN arm yields one fact per IS NOT NULL conjunct: the antecedent is
/// `discriminator = WHEN_value`, the consequent is the conjunct column.
///
/// Two surface syntaxes:
///   * **Simple CASE** — `CASE col WHEN 'a' THEN …`. pg_query exposes the
///     discriminator at `CaseExpr.arg` and each `CaseWhen.expr` is the value
///     literal directly.
///   * **Searched CASE** — `CASE WHEN col = 'a' THEN …`. No `CaseExpr.arg`;
///     each `CaseWhen.expr` is a full predicate we can parse with our
///     existing equality-extractor.
///
/// Arms whose THEN body isn't a clean AND-chain of `IS NOT NULL` tests
/// (e.g. the OR-mixed `captured` arm, or `IS NULL` arms) contribute nothing.
/// We never weaken: an unparseable arm simply produces no facts, and the
/// other arms' facts remain valid because each fact's antecedent stands on
/// its own.
fn recognize_case_facts(node: &Node) -> Vec<NotNullFact> {
    let case = match &node.node {
        Some(NodeEnum::CaseExpr(c)) => c,
        _ => return Vec::new(),
    };

    // Extract the discriminator column for the simple-CASE form. None means
    // we're in searched-CASE territory and each WHEN arm carries its own
    // full predicate.
    let simple_discriminator = case
        .arg
        .as_deref()
        .and_then(|n| extract_column_name(n));

    let mut facts = Vec::new();
    for arm_node in &case.args {
        let arm = match &arm_node.node {
            Some(NodeEnum::CaseWhen(w)) => w,
            _ => continue,
        };

        // Antecedent: build a `Predicate::Eq` for this arm.
        let antecedent = if let Some(disc) = &simple_discriminator {
            // Simple CASE: arm.expr is the literal compared against the
            // discriminator. Accept any form we already know how to peel
            // (AConst, TypeCast'd literal).
            let value = arm.expr.as_deref().and_then(extract_string_literal);
            match value {
                Some(v) => Predicate::Eq {
                    column: disc.clone(),
                    value: Literal::Text(v),
                },
                None => continue,
            }
        } else {
            // Searched CASE: arm.expr is a full predicate. We only handle
            // the equality form for now — `WHEN col = 'literal' THEN …`.
            match arm.expr.as_deref().and_then(extract_equality_predicate) {
                Some(pred) => pred,
                None => continue,
            }
        };

        // Consequent(s): walk the THEN body. Only `IS NOT NULL` columns
        // surface as facts; anything else in the body is silently dropped.
        let result = match arm.result.as_deref() {
            Some(r) => r,
            None => continue,
        };
        for col in collect_is_not_null_columns(result) {
            facts.push(NotNullFact {
                when: antecedent.clone(),
                column: col,
            });
        }
    }

    facts
}

/// Walk an expression tree under an AND-chain and collect every column that
/// appears in an `IS NOT NULL` test at the top of a conjunct. Stops at the
/// first non-AND / non-IS-NOT-NULL node within a branch, but doesn't fail —
/// other branches of the AND can still yield facts.
///
/// Crucially, **OR-branches are skipped entirely**. `(a IS NOT NULL OR
/// b IS NOT NULL)` does not prove either column non-NULL; we only emit a
/// fact when the column is non-NULL on *every* satisfying row, which means
/// the conjunct must hold unconditionally.
fn collect_is_not_null_columns(node: &Node) -> Vec<String> {
    let mut out = Vec::new();
    collect_is_not_null_into(node, &mut out);
    out
}

fn collect_is_not_null_into(node: &Node, out: &mut Vec<String>) {
    if let Some(NodeEnum::BoolExpr(b)) = &node.node {
        if b.boolop() == BoolExprType::AndExpr {
            for arg in &b.args {
                collect_is_not_null_into(arg, out);
            }
            return;
        }
        // OR / NOT — drop. Neither yields a definite per-row consequent.
        return;
    }

    if let Some(col) = extract_is_not_null_column(node) {
        out.push(col);
    }
}

/// Recognize the equality-keyed implication form:
/// `NOT (col = literal) OR (other IS NOT NULL)`. Less common than the
/// biconditional, but cheap to support.
fn recognize_eq_implication(node: &Node) -> Option<NotNullFact> {
    let args = or_two_args(node)?;
    for (i, j) in [(0usize, 1usize), (1, 0)] {
        let not_arm = &args[i];
        let positive_arm = &args[j];
        if let Some(NodeEnum::BoolExpr(inner)) = &not_arm.node {
            if inner.boolop() == BoolExprType::NotExpr && inner.args.len() == 1 {
                if let (Some(pred), Some(col)) = (
                    extract_equality_predicate(&inner.args[0]),
                    extract_is_not_null_column(positive_arm),
                ) {
                    return Some(NotNullFact { when: pred, column: col });
                }
            }
        }
    }
    None
}

/// Recognize the NULL-keyed implication: `(a IS NULL) OR (b IS NOT NULL)`.
/// Equivalent to `a IS NOT NULL → b IS NOT NULL`.
///
/// Common in real schemas as "this optional column requires that one":
/// `weight_limit_unit_consistency`, `sync_src_requires_sku`,
/// `shipping_credit_used_requires_checkout`.
fn recognize_null_implication(node: &Node) -> Option<NotNullFact> {
    let args = or_two_args(node)?;
    // Either arm can be the IS NULL side; the other must be IS NOT NULL.
    for (i, j) in [(0usize, 1usize), (1, 0)] {
        let null_arm = &args[i];
        let not_null_arm = &args[j];
        if let (Some(antecedent_col), Some(target_col)) = (
            extract_is_null_column(null_arm),
            extract_is_not_null_column(not_null_arm),
        ) {
            // Avoid emitting trivially-equal facts: `(a IS NULL) OR (a IS NOT NULL)`
            // is a tautology that proves nothing useful.
            if antecedent_col == target_col {
                continue;
            }
            return Some(NotNullFact {
                when: Predicate::IsNotNull { column: antecedent_col },
                column: target_col,
            });
        }
    }
    None
}

/// Return the two arguments of a top-level OR if the node has exactly two.
/// Used by both implication-form recognizers.
fn or_two_args(node: &Node) -> Option<&[Node]> {
    let b = match &node.node {
        Some(NodeEnum::BoolExpr(b)) => b,
        _ => return None,
    };
    if b.boolop() != BoolExprType::OrExpr || b.args.len() != 2 {
        return None;
    }
    Some(&b.args)
}

/// Strip surrounding `(expr)` wrappers — pg_query exposes parens via implicit
/// nesting in some shapes but the planner output is paren-heavy and we want to
/// see through them.
fn unwrap_paren(node: &Node) -> &Node {
    // pg_query doesn't have an explicit Paren node — parens collapse during parse.
    // This helper exists in case future patterns introduce wrappers (e.g. TypeCast
    // around a boolean expression). For now it's a no-op pass-through.
    node
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

fn extract_column_name(node: &Node) -> Option<String> {
    // Peel TypeCast wrappers; `(col)::text = 'x'::text` shows up in pg_get_constraintdef.
    let inner = match &node.node {
        Some(NodeEnum::TypeCast(tc)) => tc.arg.as_deref()?,
        _ => node,
    };
    if let Some(NodeEnum::ColumnRef(col_ref)) = &inner.node {
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

    fn fact(when_col: &str, when_val: &str, target: &str) -> NotNullFact {
        NotNullFact {
            when: Predicate::Eq {
                column: when_col.to_string(),
                value: Literal::Text(when_val.to_string()),
            },
            column: target.to_string(),
        }
    }

    #[test]
    fn parses_basic_biconditional() {
        // Form as pg_get_constraintdef emits it (with ::text casts).
        let expr = "CHECK (((product_type = 'paint'::text) = (paint_color IS NOT NULL)))";
        let facts = parse_check_as_facts(expr);
        assert_eq!(facts.len(), 1);
        let f = &facts[0];
        match &f.when {
            Predicate::Eq { column, value } => {
                assert_eq!(column, "product_type");
                assert_eq!(value, &Literal::Text("paint".into()));
            }
            other => panic!("expected Eq antecedent, got {:?}", other),
        }
        assert_eq!(f.column, "paint_color");
    }

    #[test]
    fn parses_biconditional_with_enum_cast() {
        let expr =
            "CHECK (((product_type = 'paint'::product_type) = (paint_color IS NOT NULL)))";
        let facts = parse_check_as_facts(expr);
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].column, "paint_color");
    }

    #[test]
    fn parses_biconditional_arms_either_order() {
        let expr = "CHECK (((paint_color IS NOT NULL) = (product_type = 'paint'::text)))";
        let facts = parse_check_as_facts(expr);
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].column, "paint_color");
    }

    #[test]
    fn ignores_unrelated_checks() {
        // Range / value-validity checks shouldn't produce any facts.
        assert!(parse_check_as_facts("CHECK (paint_ml > 0)").is_empty());
        assert!(parse_check_as_facts("CHECK (length(name) > 0)").is_empty());
    }

    #[test]
    fn ignores_in_list_checks() {
        // These are the existing parse_check_enum domain; not our concern.
        assert!(parse_check_as_facts("CHECK (status IN ('a', 'b', 'c'))").is_empty());
    }

    #[test]
    fn empty_on_unparseable() {
        assert!(parse_check_as_facts("not a check").is_empty());
    }

    #[test]
    fn parses_null_biconditional_both_directions() {
        // miniswap: sync_src_requires_last_synced
        let expr = "CHECK (((sync_src IS NULL) = (last_synced_at IS NULL)))";
        let facts = parse_check_as_facts(expr);
        assert_eq!(facts.len(), 2, "biconditional should emit one fact per direction");
        let mut targets: Vec<&str> = facts.iter().map(|f| f.column.as_str()).collect();
        targets.sort();
        assert_eq!(targets, vec!["last_synced_at", "sync_src"]);
        // Each fact's antecedent must be IsNotNull on the *other* column.
        for f in &facts {
            match &f.when {
                Predicate::IsNotNull { column } => {
                    assert_ne!(column, &f.column, "antecedent column must differ from target");
                }
                other => panic!("expected IsNotNull antecedent, got {:?}", other),
            }
        }
    }

    #[test]
    fn parses_null_implication() {
        // miniswap: sync_src_requires_sku
        let expr = "CHECK (((sync_src IS NULL) OR (external_sku IS NOT NULL)))";
        let facts = parse_check_as_facts(expr);
        assert_eq!(facts.len(), 1);
        match &facts[0].when {
            Predicate::IsNotNull { column } => assert_eq!(column, "sync_src"),
            other => panic!("expected IsNotNull antecedent, got {:?}", other),
        }
        assert_eq!(facts[0].column, "external_sku");
    }

    #[test]
    fn parses_null_implication_with_args_reversed() {
        // OR is commutative, and pg_get_constraintdef may emit either order.
        let expr = "CHECK (((external_sku IS NOT NULL) OR (sync_src IS NULL)))";
        let facts = parse_check_as_facts(expr);
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].column, "external_sku");
    }

    #[test]
    fn ignores_xor_form_null_biconditional() {
        // (a IS NULL) = (b IS NOT NULL) means exactly-one-non-null, not
        // "both or neither". We intentionally don't infer NOT NULL from this.
        let expr = "CHECK (((a IS NULL) = (b IS NOT NULL)))";
        assert!(parse_check_as_facts(expr).is_empty());
    }

    #[test]
    fn null_tautology_emits_nothing() {
        // `(a IS NULL) OR (a IS NOT NULL)` is always true — no real fact.
        let expr = "CHECK (((a IS NULL) OR (a IS NOT NULL)))";
        assert!(parse_check_as_facts(expr).is_empty());
    }

    /// Helper: assert that a set of (antecedent_column, antecedent_value, target_column)
    /// triples appears in `facts`, regardless of order.
    fn assert_facts_include(facts: &[NotNullFact], expected: &[(&str, &str, &str)]) {
        let actual: Vec<(String, String, String)> = facts
            .iter()
            .filter_map(|f| match &f.when {
                Predicate::Eq { column, value: Literal::Text(v) } => {
                    Some((column.clone(), v.clone(), f.column.clone()))
                }
                _ => None,
            })
            .collect();
        for (ac, av, tc) in expected {
            assert!(
                actual.iter().any(|(c, v, t)| {
                    c == ac && v == av && t == tc
                }),
                "expected fact (when {} = {} then {} IS NOT NULL) not found in:\n{:#?}",
                ac,
                av,
                tc,
                facts
            );
        }
    }

    #[test]
    fn parses_simple_case_single_arm() {
        let expr = r#"
            CHECK (
                CASE status
                    WHEN 'authorized'::text THEN ((charge_id IS NOT NULL))
                END
            )
        "#;
        let facts = parse_check_as_facts(expr);
        assert_facts_include(&facts, &[("status", "authorized", "charge_id")]);
    }

    #[test]
    fn parses_simple_case_multi_arm_multi_conjunct() {
        // The shape miniswap's order_payment_fields_check uses (simplified).
        let expr = r#"
            CHECK (
                CASE payment_status
                    WHEN 'authorized'::text THEN (
                        (payment_intent_id IS NOT NULL)
                        AND (charge_id IS NOT NULL)
                        AND (authorization_expires_at IS NOT NULL)
                    )
                    WHEN 'canceled'::text THEN (cancellation_reason IS NOT NULL)
                    ELSE true
                END
            )
        "#;
        let facts = parse_check_as_facts(expr);
        assert_facts_include(
            &facts,
            &[
                ("payment_status", "authorized", "payment_intent_id"),
                ("payment_status", "authorized", "charge_id"),
                ("payment_status", "authorized", "authorization_expires_at"),
                ("payment_status", "canceled", "cancellation_reason"),
            ],
        );
        assert_eq!(facts.len(), 4, "no extra facts expected: {:#?}", facts);
    }

    #[test]
    fn searched_case_form_supported() {
        // CASE WHEN col = 'a' THEN … — discriminator inside each arm.
        let expr = r#"
            CHECK (
                CASE
                    WHEN payment_status = 'authorized'::text THEN (charge_id IS NOT NULL)
                    WHEN payment_status = 'canceled'::text  THEN (cancellation_reason IS NOT NULL)
                END
            )
        "#;
        let facts = parse_check_as_facts(expr);
        assert_facts_include(
            &facts,
            &[
                ("payment_status", "authorized", "charge_id"),
                ("payment_status", "canceled", "cancellation_reason"),
            ],
        );
        assert_eq!(facts.len(), 2);
    }

    #[test]
    fn case_or_mixed_arm_contributes_nothing_but_clean_arms_still_work() {
        // miniswap's 'captured' arm has an OR inside it — that arm should be
        // skipped, but the clean 'authorized' arm next to it must still emit.
        let expr = r#"
            CHECK (
                CASE payment_status
                    WHEN 'authorized'::text THEN (charge_id IS NOT NULL)
                    WHEN 'captured'::text THEN (
                        (payment_captured_at IS NOT NULL)
                        AND (((payment_intent_id IS NOT NULL) AND (charge_id IS NOT NULL))
                             OR (credit_transfer_id IS NOT NULL))
                    )
                END
            )
        "#;
        let facts = parse_check_as_facts(expr);
        assert_facts_include(
            &facts,
            &[("payment_status", "authorized", "charge_id")],
        );
        // The 'captured' arm's top-level AND has one clean conjunct
        // (payment_captured_at) and one OR-branch we must skip.
        assert_facts_include(
            &facts,
            &[("payment_status", "captured", "payment_captured_at")],
        );
        // The OR-branch columns must NOT appear under 'captured'.
        let bad: Vec<_> = facts
            .iter()
            .filter(|f| {
                matches!(&f.when, Predicate::Eq { value: Literal::Text(v), .. } if v == "captured")
                    && (f.column == "payment_intent_id"
                        || f.column == "charge_id"
                        || f.column == "credit_transfer_id")
            })
            .collect();
        assert!(
            bad.is_empty(),
            "OR-branch columns must not produce 'captured' facts: {:#?}",
            bad
        );
    }

    #[test]
    fn case_is_null_arm_contributes_nothing() {
        // The 'pending' arm of order_payment_fields_check is all IS NULL —
        // we don't emit "must-be-NULL" facts, so this arm yields nothing.
        let expr = r#"
            CHECK (
                CASE payment_status
                    WHEN 'pending'::text THEN (
                        (payment_intent_id IS NULL)
                        AND (charge_id IS NULL)
                    )
                END
            )
        "#;
        let facts = parse_check_as_facts(expr);
        assert!(
            facts.is_empty(),
            "IS NULL conjuncts must not produce facts: {:#?}",
            facts
        );
    }

    #[test]
    fn case_with_enum_cast_in_when_value() {
        let expr = r#"
            CHECK (
                CASE payment_status
                    WHEN 'authorized'::order_payment_status THEN (charge_id IS NOT NULL)
                END
            )
        "#;
        let facts = parse_check_as_facts(expr);
        assert_facts_include(&facts, &[("payment_status", "authorized", "charge_id")]);
    }

    #[test]
    fn miniswap_order_payment_fields_check_verbatim() {
        // The exact text pg_get_constraintdef emits for the miniswap order table.
        let expr = "CHECK (
            CASE payment_status
                WHEN 'authorized'::order_payment_status THEN ((payment_intent_id IS NOT NULL) AND (charge_id IS NOT NULL) AND (authorization_expires_at IS NOT NULL))
                WHEN 'captured'::order_payment_status THEN ((payment_captured_at IS NOT NULL) AND (((payment_intent_id IS NOT NULL) AND (charge_id IS NOT NULL)) OR (credit_transfer_id IS NOT NULL)))
                WHEN 'pending'::order_payment_status THEN ((payment_intent_id IS NULL) AND (charge_id IS NULL) AND (authorization_expires_at IS NULL) AND (payment_captured_at IS NULL))
                WHEN 'canceled'::order_payment_status THEN (cancellation_reason IS NOT NULL)
                ELSE true
            END)";
        let facts = parse_check_as_facts(expr);
        assert_facts_include(
            &facts,
            &[
                ("payment_status", "authorized", "payment_intent_id"),
                ("payment_status", "authorized", "charge_id"),
                ("payment_status", "authorized", "authorization_expires_at"),
                ("payment_status", "captured", "payment_captured_at"),
                ("payment_status", "canceled", "cancellation_reason"),
            ],
        );
        // 'pending' (all IS NULL) and the OR-branch of 'captured' must yield nothing.
        let pending_facts: Vec<_> = facts
            .iter()
            .filter(|f| matches!(&f.when, Predicate::Eq { value: Literal::Text(v), .. } if v == "pending"))
            .collect();
        assert!(
            pending_facts.is_empty(),
            "'pending' arm produces no facts: {:#?}",
            pending_facts
        );
    }

    #[test]
    fn matches_expected_fact_shape() {
        let expr = "CHECK (((product_type = 'paint'::text) = (paint_color IS NOT NULL)))";
        let facts = parse_check_as_facts(expr);
        let expected = fact("product_type", "paint", "paint_color");
        match (&facts[0].when, &expected.when) {
            (
                Predicate::Eq { column: c1, value: v1 },
                Predicate::Eq { column: c2, value: v2 },
            ) => {
                assert_eq!(c1, c2);
                assert_eq!(v1, v2);
            }
            (a, b) => panic!("expected matching Eq predicates, got {:?} vs {:?}", a, b),
        }
        assert_eq!(facts[0].column, expected.column);
    }
}
