# pgrpc Nullability Audit — Miniswap `crates/db`

Cross-referenced all 45 query files in `sql/queries/` against the live schema (via psql)
and the generated bindings in `src/pgrpc/`. Methodology: for each output column, derived
its true nullability from the schema/SQL and compared it to the generated Rust type,
looking for two error classes — **false NOT NULL** (`T` where NULL is possible → runtime
deserialization panic) and **false nullable** (`Option<T>` where NULL is impossible →
ergonomic noise).

## Headline result

pgrpc's analyzer is **broadly correct and conservative**. Where it traces query columns
directly, it handles the hard cases right — LEFT-JOIN composite casts
(`da.*::api.delivery_address` → `Option`), `least(notnull, null)` → NOT NULL,
`coalesce(x, notnull)` → NOT NULL, scalar subqueries → `Option`, `count()` → NOT NULL vs
`sum()` → `Option`. The handful of genuine **false-NULLABLE** misses are minor.

**The real failure mode is not the analyzer's inference — it's the `@pgrpc_not_null`
view-column escape hatch.** When a view column is hand-annotated `@pgrpc_not_null`, pgrpc
trusts it blindly. Several annotations are **factually wrong**, producing unsound
non-`Option` types that are only saved today by coincidental data invariants.

## CRITICAL — false NOT NULL (unsound, panic risk)

These all stem from incorrect `@pgrpc_not_null` annotations, not the inference engine.
The analyzer "breaks down" by deferring to a human annotation it can't verify.

| View.column | Annotated | Actual SQL | Why it's nullable | Reached by binding? |
|---|---|---|---|---|
| **`api.item_summary.categories`** | `@pgrpc_not_null` → `Vec<CategoryBrief>` | bare scalar-subquery `array_agg(...)` **without** `COALESCE` | A scalar subquery's `array_agg` over zero rows returns **NULL**. No constraint/trigger forces an item to have ≥1 category. | **Yes** — `ItemSummary` (cart, search, etc.) |
| **`api.product_summary.categories`** | same | same pattern | same; product-category trigger is `DEFERRABLE` and fires only on `product_category` writes, so a category-less product slips through | **Yes** — product summary/search |
| **`api.product_details.categories`** | same | same pattern | same | **Yes** — `find_product_by_slug`, etc. |
| **`api.checkout_parcels.shippo_account_id`** | `@pgrpc_not_null` → `ShippoAccountId` | maps to `seller.shippo_account_id` | base column is **nullable** (`attnotnull=f`); a seller before Shippo onboarding is NULL | `CheckoutParcels` struct is generated |
| **`core.product_listing_counts.nib_listing_count` / `paint_listing_count`** | `@pgrpc_not_null` | `sum(...) FILTER (WHERE ...)::int` | a **filtered SUM returns NULL** when no rows match the filter, even on a non-empty group | Not directly (it's `core`); every consumer wraps in `COALESCE(...,0)`. Latent. |

The `categories` case was proven empirically: in a rolled-back transaction, an item with
zero category rows makes the view expression evaluate to `NULL`, which would panic when
deserialized into `Vec<CategoryBrief>`. These haven't blown up only because every row in
the current (tiny) dataset happens to satisfy the implicit invariant — none are enforced
by a `CHECK`/`NOT NULL`/airtight trigger.

Contrast: `api.item_details.categories`, `.units`, `.products` use
`COALESCE(<subquery>, ARRAY[]::...)` and are correctly safe. The bug is specifically the
`_summary`/`_details` `categories` columns that **omit the COALESCE** yet carry the
annotation.

## MINOR — false nullable (just `Option` noise)

- `get_seller_by_nanoid` / `api.seller_details`: **all correct** (initially suspected
  `away_until`/`is_away`, but verified `seller.away_until` is NOT NULL, so both are right).
- `api.order_details.parcels` (`array_agg` with `GROUP BY` → never NULL) and `line_items`
  → marked `Option<Vec<…>>`, never actually NULL.
- `get_item_for_update.images` (`array(...)` constructor → `{}` never NULL) →
  `Option<Vec<ItemImage>>`.
- `get_item_for_google_merchant.is_eligible` (short-circuit boolean AND) → `Option<bool>`.
- `search_products_by_image_embedding.distance` (`min()` post-WHERE-filter) → `Option<f64>`.
- `search_accounts`: `account_id`/`account_nanoid`/`display_name`/`country_code` from the
  **preserved** side of a LEFT JOIN → all marked `Option` (left-side columns keep NOT NULL).
- `search_articles.highlight` (`ts_headline` strict over NOT NULL input) → `Option<String>`.
- `update_seller_holiday` / `update_account_inventory_for_sale_only`:
  `UPDATE … RETURNING` of NOT NULL columns → `Option`.

The `search_accounts` case points at a real inference weakness: pgrpc appears to mark
**all** columns of a LEFT-JOIN query nullable rather than distinguishing preserved vs.
nullable side. Harmless (conservative) but the one place the *engine itself* (not an
annotation) is imprecise.

## Recommendation

The pattern to fix is the `@pgrpc_not_null` annotations that aren't backed by a real
invariant. Two options per case:
1. **Make the SQL honest**: wrap the bare `array_agg` subqueries in
   `COALESCE(..., ARRAY[]::api.category_brief[])` (matching what `item_details` already
   does) — then the annotation becomes true and the type is genuinely sound.
2. **Drop the annotation** so pgrpc emits `Option<...>`.

For pgrpc itself, the takeaway is that `@pgrpc_not_null` is the analyzer's blind spot:
it's an unchecked assertion. A useful enhancement would be to *verify* an annotation
against the column's traced expression where possible (e.g. warn when a `@pgrpc_not_null`
column derives from a non-coalesced aggregate, a nullable base column, or the nullable
side of an outer join) rather than trusting it unconditionally.
