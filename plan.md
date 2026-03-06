# Double-Underscore Column Grouping Feature

## Overview

Columns named `{prefix}__{field}` (double underscore) are automatically grouped into a nested struct named `{Prefix}` in generated Rust code. When the group's source columns come from a LEFT JOIN'd table, the group field is `Option<Struct>`, and inner fields use their base-table nullability.

## Example

```sql
-- product is LEFT JOIN'd, so all its columns appear nullable in the view
-- but product.name and product.slug are NOT NULL on the base table
left join product p using (product_id)
...
p.name as product__name,
p.slug as product__slug,
p.msrp as product__msrp,  -- msrp IS nullable on the base table
i.title,
i.created_at
```

Generates:
```rust
pub struct ItemSummary {
    pub title: String,
    pub created_at: OffsetDateTime,
    pub product: Option<ItemSummaryProduct>,
}

pub struct ItemSummaryProduct {
    pub name: String,            // NOT NULL on base table
    pub slug: String,            // NOT NULL on base table
    pub msrp: Option<Decimal>,   // nullable on base table
}
```

## Implementation Steps

### Step 1: Extend `ViewNullabilityCache` to carry two-level nullability

**File: `src/view_nullability.rs`**

- Add `ColumnNullability` struct with `nullable_due_to_join: bool` and `nullable_on_base: bool`
- Change `ViewNullabilityCache` from `HashMap<Key, IndexMap<String, bool>>` to `HashMap<Key, IndexMap<String, ColumnNullability>>`
- Modify `analyze_view` to return `IndexMap<String, ColumnNullability>`
- Modify `is_column_ref_nullable` to return both pieces of info instead of early-returning `true` when `is_nullable`
- Add `is_not_null()` convenience method that returns `!(nullable_due_to_join || nullable_on_base)` for backward compat

### Step 2: Update all consumers of `ViewNullabilityCache`

**Files: `src/ty_index.rs`, `src/lib.rs`, `src/query_introspector.rs`, `src/fn_index.rs`**

- Update `apply_view_nullability_from_cache` in `ty_index.rs` to use `ColumnNullability`
- For non-grouped fields, behavior stays the same: `field.nullable = !cn.is_not_null()`
- Store the `ColumnNullability` on `PgField` (add `nullable_due_to_join: bool` field)
- Update `QueryColumn` to also carry `nullable_due_to_join`
- Update SQL function nullability application

### Step 3: Create `src/column_grouping.rs` — core grouping logic

Pure data-transformation module:

```rust
pub enum GroupedField<T> {
    Plain(T),
    Group {
        group_name: String,
        fields: Vec<T>,
        /// True if ANY field has nullable_due_to_join, making the group Option<Struct>
        optional: bool,
    },
}

pub fn group_by_double_underscore<T>(
    items: Vec<T>,
    get_name: impl Fn(&T) -> &str,
    is_join_nullable: impl Fn(&T) -> bool,
    set_name: impl Fn(&mut T, String),
) -> Vec<GroupedField<T>>
```

- Split on first `__` only
- Groups appear at position of first member
- Group is `optional` if any field has `nullable_due_to_join`
- When group is optional, strip join-nullability from inner fields (use base nullability only)

### Step 4: Modify `generate_row_struct` in `codegen.rs` (SQL query row structs)

- Call `group_by_double_underscore` on columns
- For each `Group`, generate nested struct `{ParentName}{GroupPascal}`
- Parent struct gets `pub group_name: NestedStruct` or `pub group_name: Option<NestedStruct>`
- `TryFrom<Row>` extracts by original column index, constructs nested struct
- When optional: check if all grouped columns are NULL → return None, else construct

### Step 5: Modify `PgType::Composite` codegen in `pg_type.rs` (view composite types)

- Only apply grouping when `relkind` is `'v'` or `'m'` (views/matviews)
- Skip `FromSql`/`ToSql` postgres derives when grouping is active (views use `TryFrom<Row>`)
- Generate nested structs same as Step 4

### Step 6: Modify function return struct codegen in `pg_fn.rs` (RETURN TABLE / OUT params)

- Apply same grouping to `self.out_args` for RETURN TABLE and multi-OUT functions

### Step 7: Unit tests in `column_grouping.rs`

- Basic grouping, single-column groups, no `__` passthrough, ordering, first-`__`-only split
- Optional group detection from join nullability

## Files to modify

| File | Change |
|---|---|
| `src/view_nullability.rs` | Add `ColumnNullability`, change cache type, return two-level info |
| `src/column_grouping.rs` | **New** — core grouping logic + unit tests |
| `src/lib.rs` | Add `mod column_grouping;` |
| `src/pg_type.rs` | Add `nullable_due_to_join` to `PgField`, modify Composite codegen |
| `src/ty_index.rs` | Update `apply_view_nullability_from_cache` for new cache type |
| `src/query_introspector.rs` | Add `nullable_due_to_join` to `QueryColumn` |
| `src/fn_index.rs` | Update SQL function nullability if applicable |
| `src/codegen.rs` | Modify `generate_row_struct` for grouping |
| `src/pg_fn.rs` | Modify RETURN TABLE / OUT param struct gen for grouping |