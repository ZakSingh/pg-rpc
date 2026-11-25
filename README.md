# PgRPC

PgRPC generates type-safe Rust bindings for PostgreSQL functions, allowing you to call database functions as if they were native Rust code.

## Installation

### As a CLI tool
```bash
cargo install --path .
```

### As a library
Add to your `Cargo.toml`:
```toml
[dependencies]
pgrpc = { path = "path/to/pgrpc" }

# Or for build script usage:
[build-dependencies]
pgrpc = { path = "path/to/pgrpc" }
```

## Usage

### CLI Usage

Create a `pgrpc.toml` configuration file:
```toml
connection_string = "postgres://postgres:postgres@localhost:5432/mydb"
output_path = "src/generated"  # Optional - directory for generated files
schemas = ["public", "api"]

[types]
ltree = "String"

[exceptions]
P0001 = "Custom application error"

[task_queue]
schema = "tasks"
task_name_column = "task_name"
payload_column = "payload"
```

Then run:
```bash
# Uses output_path from config file
pgrpc_cli -c pgrpc.toml

# Or override the output path
pgrpc_cli -c pgrpc.toml -o src/custom_path/
```

This will create a directory structure with separate files for each schema:
```
src/generated/
├── mod.rs
├── public.rs
├── api.rs
└── ...
```

### Library Usage

#### In build.rs
```rust
use pgrpc::PgrpcBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR")?;
    
    PgrpcBuilder::from_config_file("pgrpc.toml")?
        .output_path(format!("{}/pgrpc", out_dir))
        .build()?;
    
    println!("cargo:rerun-if-changed=pgrpc.toml");
    Ok(())
}
```

#### Programmatic Usage
```rust
use pgrpc::PgrpcBuilder;

// Load from config file (uses output_path from config if specified)
PgrpcBuilder::from_config_file("pgrpc.toml")?
    .build()?;

// Or configure programmatically
PgrpcBuilder::new()
    .connection_string("postgres://postgres:postgres@localhost:5432/mydb")
    .schema("public")
    .schema("api")
    .type_mapping("ltree", "String")
    .exception("P0001", "Custom error")
    .output_path("src/generated")
    .build()?;
```

### Using Generated Code

```rust
// Include the generated module
include!(concat!(env!("OUT_DIR"), "/pgrpc/mod.rs"));

// Use the functions
let result = api::my_function(&client, arg1, arg2).await?;
```

### Performance Benefits

pgrpc generates separate files for each schema, which provides:
- **Faster incremental compilation**: Only modified schemas need recompilation
- **Better IDE performance**: Smaller files for language servers to analyze  
- **Improved build parallelism**: Rust can compile multiple files concurrently
- **Reduced memory usage**: Compiler handles smaller compilation units

This is especially beneficial for large databases with many schemas and functions.

## Configuration reference

All keys of `pgrpc.toml` (each has a matching `PgrpcBuilder` method). Unknown keys are
rejected, so a typo fails loudly instead of being ignored.

| Key | Default | Meaning |
|---|---|---|
| `connection_string` | `DATABASE_URL` env var | Database to introspect |
| `output_path` | required (or `-o`) | Directory for the generated files |
| `schemas` | required | Schemas to generate bindings for. Every user schema is still *analyzed*, so calls into helpers elsewhere are followed |
| `infer_view_nullability` | `true` | Infer NOT NULL for view columns, query results and SQL-function `OUT` columns |
| `disable_deserialize` | `[]` | `"schema.type"` names that should not derive `serde::Deserialize` |
| `strict_domains` | `[]` | `"schema.domain"` names generated as validated newtypes (see [Strict domain types](#strict-domain-types)) |
| `builders` | `[]` | `"schema.type"` names of composite types and views that also derive `bon::Builder` |
| `[types]` | | Map a Postgres type name to a Rust type path, e.g. `ltree = "String"` |
| `[exceptions]` | | Name a SQLSTATE: `P0001 = "Custom application error"` |
| `[queries] paths` | | Glob patterns of SQL query files (see [SQL Query Files](#sql-query-files)) |
| `[task_queue]` | | `schema`, `task_name_column`, `payload_column`, optional `table_schema`/`table_name` (see [Task Queue Integration](#task-queue-integration)) |
| `[errors]` | | `schema` and optional `raise_function` (see [Typed errors](#typed-errors)) |
| `[tracing] enabled` | `false` | Wrap generated functions in `tracing` spans named `schema.function` / `queries.name`. Arguments are never recorded. Requires `tracing = "0.1"` |

A schema whose module name collides with a module pgrpc generates itself (`errors`,
`nullable`, `composite`, `schema_cache`, `tsvector`, `datetime_serde`, and `tasks` /
`queries` / `custom_errors` when those features are on) is rejected, as are two schemas
that map to the same module name. The generated modules refer to each other as
`super::<module>`, so keep them together in one directory.

Unqualified table, view and function names in analyzed SQL are resolved along the
connection's `search_path`, as Postgres does. A function's own `SET search_path` clause is
not taken into account.

### Annotations

Annotations go in `COMMENT ON ...` text (or in `--` comment lines above a query in a
query file):

| Annotation | Where | Effect |
|---|---|---|
| `@pgrpc_not_null` | view column, composite attribute, function returning a single value | This column / the return value is never NULL |
| `@pgrpc_not_null(a, b)` | view, composite type, function (`OUT` / `TABLE` params), query | The named columns are never NULL |
| `@pgrpc_not_null(*)` | standalone composite type | Every attribute is never NULL |
| `@pgrpc_nullable` | composite attribute | Exception to the type's `@pgrpc_not_null(*)` |
| `@pgrpc_nullable(a, b)` | query; composite type | Query: make the named parameters `Option<T>`. Type: exceptions to its `@pgrpc_not_null(*)` |
| `@pgrpc_message`, `@pgrpc_field(...)` | constraint, error payload type | User-facing message (see [User-facing constraint messages](#user-facing-constraint-messages)) |

The rule for the two forms: bare means "this object", a list names its children.

Annotations state facts about the database that pgrpc cannot see for itself.
Preferences about the generated Rust (`strict_domains`, `builders`,
`disable_deserialize`, `[types]`) live in the config instead.

Annotations are checked. Generation fails, listing every problem and where it
is, on a tag that is unknown (`@pgrpc_not_nul`), malformed, on an object where it
means nothing (bare `@pgrpc_not_null` on a set-returning function), or that names
something that does not exist (a column the type does not have, a parameter the
query does not take). This covers functions in every analyzed schema, not only
the ones bindings are generated for.

`@pgrpc_not_null` is trusted, not verified: a wrong annotation surfaces as a decode
error at runtime. pgrpc warns at generation time in the one case it can prove wrong, a
column that comes from the nullable side of an outer join.

It also warns when an annotation is redundant, that is, when the analysis already
proves the view column, SQL-function `OUT` column or query column NOT NULL. A
redundant annotation changes nothing today and would hide a later schema change
that makes it wrong, so delete it. The run prints the number of redundant
annotations; `RUST_LOG=pgrpc=warn` lists each one.

### Dependencies of the generated code

The generated modules expect these crates in the consuming crate:

```toml
tokio-postgres = { version = "0.7", features = ["with-serde_json-1", "with-time-0_3"] }
postgres-types = { version = "0.2", features = ["derive"] }
deadpool-postgres = "0.14"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
time = { version = "0.3", features = ["serde", "macros", "formatting", "parsing"] }
rust_decimal = "1"
uuid = { version = "1", features = ["serde"] }
thiserror = "2"
bon = "3"
derive_more = { version = "2", features = ["full"] }
# only if used by your schema:
# postgis-butmaintained = "0.12"   # geometry / geography
# pgvector = { version = "0.4", features = ["postgres"] }
# tracing = "0.1"                  # with [tracing] enabled = true
```

### Type support

`timestamptz` maps to `time::OffsetDateTime`, `timestamp` to `time::PrimitiveDateTime`,
`date` to `time::Date`, `numeric` to `rust_decimal::Decimal`, `oid` to `u32`, `"char"` to
`i8`. Types with no binary codec in the dependencies above (ranges and multiranges,
`interval`, `money`, `cidr`, `macaddr`, `bit`, `xml`, `timetz`) are not supported:
functions and queries that use them are skipped, and columns of those types are dropped
from composites and views, each with a warning naming the object.

`[types]` replaces the Rust type for any Postgres type, by bare name or `schema.name`,
including the unsupported ones above:

```toml
[types]
macaddr = "crate::net::Mac"
```

The Rust type must implement `FromSql`/`ToSql` for that Postgres type, plus `Clone`,
`Debug` and serde's traits; the path is used verbatim inside the generated modules.

Functions with unnamed arguments get `arg1`, `arg2`, …; `VARIADIC` arguments are passed
as a slice.

## Task Queue Integration

PgRPC can generate type-safe Rust enums for PostgreSQL-based message queue systems. This feature allows you to define task types as PostgreSQL composite types and automatically generate corresponding Rust enums with serde support.

### Setup

1. **Create a dedicated schema for task types:**
```sql
CREATE SCHEMA tasks;
```

2. **Define task types as composite types:**
```sql
CREATE TYPE tasks.send_welcome_email AS (
    user_id INTEGER,
    email TEXT,
    template_name TEXT,
    send_at TIMESTAMPTZ
);

CREATE TYPE tasks.process_payment AS (
    payment_id UUID,
    amount DECIMAL(10,2),
    currency TEXT,
    retry_count INTEGER
);

CREATE TYPE tasks.resize_image AS (
    image_id BIGINT,
    target_width INTEGER,
    target_height INTEGER,
    quality INTEGER,
    format TEXT
);
```

3. **Configure task queue in `pgrpc.toml`:**
```toml
[task_queue]
schema = "tasks"                # Schema containing task composite types
table_schema = "mq"             # Schema containing the task queue table (optional, defaults to "mq")
table_name = "task"             # Name of the task queue table (optional, defaults to "task") 
task_name_column = "task_name"  # Column name for task type identifier
payload_column = "payload"      # Column name for task payload data
```

4. **Create your message queue table:**
```sql
CREATE TABLE mq.task (
    task_id SERIAL PRIMARY KEY,
    task_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
```

### Generated Code

PgRPC will generate a `TaskPayload` enum in the `tasks.rs` module:

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "task_name", content = "payload")]
pub enum TaskPayload {
    #[serde(rename = "send_welcome_email")]
    SendWelcomeEmail {
        pub user_id: i32,
        pub email: String,
        pub template_name: String,
        pub send_at: chrono::DateTime<chrono::Utc>,
    },
    
    #[serde(rename = "process_payment")]
    ProcessPayment {
        pub payment_id: uuid::Uuid,
        pub amount: rust_decimal::Decimal,
        pub currency: String,
        pub retry_count: i32,
    },
    
    #[serde(rename = "resize_image")]
    ResizeImage {
        pub image_id: i64,
        pub target_width: i32,
        pub target_height: i32,
        pub quality: i32,
        pub format: String,
    },
}

impl TaskPayload {
    pub fn task_name(&self) -> &'static str { /* ... */ }
    pub fn from_database_row(task_name: &str, payload: serde_json::Value) -> Result<Self, serde_json::Error> { /* ... */ }
    
    // Configuration helper methods
    pub const fn table_name() -> &'static str { /* "mq.task" */ }
    pub const fn task_name_column() -> &'static str { /* "task_name" */ }
    pub const fn payload_column() -> &'static str { /* "payload" */ }
}
```

### Usage Example

```rust
use pgrpc::tasks::TaskPayload;

// Create a task
let task = TaskPayload::SendWelcomeEmail {
    user_id: 123,
    email: "user@example.com".to_string(),
    template_name: "welcome".to_string(),
    send_at: chrono::Utc::now(),
};

// Serialize for database storage
let task_name = task.task_name();
let payload = serde_json::to_value(&task)?;

// Insert into queue (using generated helper methods)
let insert_sql = format!(
    "INSERT INTO {} ({}, {}) VALUES ($1, $2)",
    TaskPayload::table_name(),
    TaskPayload::task_name_column(),
    TaskPayload::payload_column()
);
client.execute(&insert_sql, &[&task_name, &payload]).await?;

// Process from queue
let select_sql = format!(
    "SELECT {}, {} FROM {} WHERE claimed_at IS NULL LIMIT 1",
    TaskPayload::task_name_column(),
    TaskPayload::payload_column(),
    TaskPayload::table_name()
);
let row = client.query_one(&select_sql, &[]).await?;

let task_name: String = row.get(TaskPayload::task_name_column());
let payload: serde_json::Value = row.get(TaskPayload::payload_column());
let task = TaskPayload::from_database_row(&task_name, payload)?;

match task {
    TaskPayload::SendWelcomeEmail { user_id, email, .. } => {
        // Handle welcome email task
    },
    TaskPayload::ProcessPayment { payment_id, amount, .. } => {
        // Handle payment processing task
    },
    TaskPayload::ResizeImage { image_id, target_width, .. } => {
        // Handle image resizing task
    },
}
```

### Alternative Table Configuration

You can customize the table location and column names as needed:

```toml
[task_queue]
schema = "task_types"           # Schema for composite types
table_schema = "queue"          # Schema for the task table  
table_name = "jobs"             # Custom table name
task_name_column = "job_type"   # Custom column name for task type
payload_column = "data"         # Custom column name for payload
```

This configuration works with:
```sql
CREATE TABLE queue.jobs (
    id SERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

The generated code will automatically use these configured names:
```rust
// Generated helper methods return your custom configuration
TaskPayload::table_name()          // Returns "queue.jobs"
TaskPayload::task_name_column()    // Returns "job_type"  
TaskPayload::payload_column()      // Returns "data"
```

### Benefits

- **Type Safety**: All task payloads are strongly typed with compile-time validation
- **Automatic Serialization**: Built-in serde support for JSON serialization/deserialization
- **PostgreSQL Integration**: Leverages PostgreSQL's type system for schema validation
- **Easy Evolution**: Add new task types by creating new composite types
- **Tagged Unions**: Serde's tagged union format for efficient JSON representation
- **Flexible Configuration**: Customize table and column names to match existing systems

## SQL Query Files

PgRPC can generate type-safe Rust functions directly from SQL query files, similar to [sqlc](https://sqlc.dev/) for Go. This allows you to write SQL queries in `.sql` files and automatically generate Rust code with proper nullability analysis.

### Setup

1. **Create SQL query files with annotations:**

```sql
-- queries/authors.sql

-- name: GetAuthor :one
SELECT * FROM authors WHERE id = :author_id LIMIT 1;

-- name: ListAuthors :many
SELECT * FROM authors ORDER BY name;

-- name: CreateAuthor :one
INSERT INTO authors (name, bio)
VALUES (:name, :bio)
RETURNING *;

-- name: UpdateAuthor :exec
UPDATE authors
SET name = :name, bio = :bio
WHERE id = :author_id;

-- name: DeleteAuthor :execrows
DELETE FROM authors WHERE id = :author_id;
```

2. **Configure in `pgrpc.toml`:**

```toml
[queries]
paths = ["queries/**/*.sql"]  # Glob patterns for SQL files
```

### Query Annotations

**Format:** `-- name: FunctionName :type`

**Query Types:**
- `:one` - Returns `Result<T, Error>`; exactly one row is expected. No row is an error
  (`PgRpcError::NoRows`, reachable through the query error enum's `Other` variant), not a panic.
- `:opt` - Returns `Result<Option<T>, Error>` for zero-or-one row queries
- `:many` - Returns `Result<Vec<T>, Error>` for multiple row queries
- `:exec` - Returns `Result<(), Error>` for commands with no return value
- `:execrows` - Returns `Result<u64, Error>` for commands returning affected row count

The `:type` is optional. Without it the cardinality is inferred from the query: a filter
that pins a primary key or unique constraint gives `:opt`, an aggregate without `GROUP BY`
gives `:one`, a statement without a result gives `:exec`, and anything unproven is `:many`.
The inference never claims fewer rows than the query can return (partial unique indexes,
`OR` between different key values, window functions and set-returning functions all fall
back to `:many`).

A query name may be defined only once across all query files; a duplicate is a generation
error listing every definition.

**Other annotations** (comment lines above the query):
- `-- @pgrpc_not_null(col1, col2)` - assert result columns NOT NULL where the analysis
  cannot prove it. Naming a column that is not in the result is an error. A warning is
  logged if the column provably comes from the nullable side of an outer join, or if
  the analysis already proves it NOT NULL (the annotation is redundant).
- `-- @pgrpc_nullable(param1, param2)` - make parameters `Option<T>`. Naming something
  that is not a `:parameter` of the query is an error.

### Parameter Syntax

**Named Parameters (Recommended):** Use `:param_name` for explicit parameter names:

```sql
-- name: GetUserByEmail :one
SELECT id, name, email FROM users WHERE email = :user_email;
```

Generates:
```rust
pub async fn get_user_by_email(
    client: &impl deadpool_postgres::GenericClient,
    user_email: &str,
) -> Result<GetUserByEmailRow, GetUserByEmailError>
```

**Parameter types.** A parameter compared with a column (`col = :p`, `col IN (:p, ...)`)
takes the column's declared type, so a parameter compared with a domain column is the
domain's Rust type rather than its base type. An explicit cast on the parameter wins over
that: `email = :p::text` makes it a plain string, and `:p::email` makes it the domain.

**Positional Parameters:** Use `$1, $2, ...` for PostgreSQL standard syntax:

```sql
-- name: UpdateUser :exec
UPDATE users SET name = $1 WHERE id = $2;
```

Parameter names will be inferred from context or use `param_1`, `param_2` as fallbacks.

Parameters are found by tokenizing the query, so a `:word` inside a string literal
(`to_char(ts, 'HH24:MI:SS')`), a comment, or an array slice (`arr[1:n]`) is left alone. To
use a parameter as a slice bound, parenthesize it: `arr[1:(:n)]`. Mixing named and
positional parameters so that they land on the same `$N` is an error.

### Nullability Analysis

**Key Feature:** PgRPC applies its powerful nullability analysis to query results, including:

- **Base Table Constraints:** NOT NULL constraints from table definitions
- **JOIN Analysis:** LEFT/RIGHT/FULL JOINs make columns nullable
- **View Analysis:** Queries referencing views inherit their nullability
- **Expression Analysis:** COALESCE, CASE, aggregate functions, etc.
- **Composite Attribute Projections:** `(p.col).attr` is non-null when `col` is NOT NULL
  and the composite's attribute is proven non-null (see
  [Nullability and CHECK constraints](#nullability-and-check-constraints))

**Example:**

Given this view:
```sql
CREATE VIEW user_posts AS
SELECT
    u.id,
    u.name,      -- NOT NULL in users table
    p.title,     -- Can be NULL (LEFT JOIN + no constraint)
    p.created_at
FROM users u
LEFT JOIN posts p ON u.id = p.user_id;
```

Query:
```sql
-- name: GetUserPosts :many
SELECT id, name, title, created_at FROM user_posts;
```

Generated code with **accurate nullability**:
```rust
#[derive(Debug, Clone)]
pub struct GetUserPostsRow {
    pub id: i32,
    pub name: String,              // NOT NULL (analyzed from base table)
    pub title: Option<String>,     // Nullable (LEFT JOIN + no constraint)
    pub created_at: Option<time::OffsetDateTime>, // Nullable
}

pub async fn get_user_posts(
    client: &impl deadpool_postgres::GenericClient,
) -> Result<Vec<GetUserPostsRow>, GetUserPostsError> {
    // Generated implementation
}
```

### Generated Code

For each query, PgRPC generates:

1. **Row Struct** (for `:one`, `:opt` and `:many` queries returning more than one column):
   - Named after the query (e.g., `GetAuthorRow`)
   - Fields with proper nullability from analysis
   - `TryFrom<tokio_postgres::Row>` implementation

2. **Async Function**:
   - Named after the query in snake_case
   - Accepts `&impl deadpool_postgres::GenericClient`
   - Type-safe parameters
   - Appropriate return type based on query type
   - SQL included in doc comments

**Example Generated Code** (abridged) for `-- name: GetAuthor :opt`:

```rust
/// Query: GetAuthor
pub async fn get_author(
    client: &impl deadpool_postgres::GenericClient,
    author_id: i32,
) -> Result<Option<GetAuthorRow>, GetAuthorError> {
    // prepares (cached) and runs the statement, then decodes the row by column name
}
```

Each query gets its own error enum (`GetAuthorError`) carrying the constraint and
SQLSTATE variants the statement can raise, plus `Other(PgRpcError)`.

### Column groups

Result columns named `{prefix}__{field}` (double underscore) are grouped into a nested
struct. This works for query rows, views, and `OUT`/`RETURNS TABLE` columns of SQL
functions:

```sql
select i.title,
       p.name as product__name,
       p.msrp as product__msrp
from item i
left join product p using (product_id)
```

```rust
pub struct ItemRow {
    pub title: String,
    pub product: Option<ItemRowProduct>,  // None when the LEFT JOIN finds no row
}

pub struct ItemRowProduct {
    pub name: String,            // NOT NULL on product
    pub msrp: Option<Decimal>,   // nullable on product
}
```

The group is `Option<Struct>` when its columns come from the nullable side of an outer
join; inside the struct, fields keep the nullability of their base table.

### Usage

```rust
use crate::generated::queries;

// :opt
if let Some(author) = queries::get_author(&client, 123).await? {
    println!("Author: {}", author.name);
}

// :many
for author in queries::list_authors(&client).await? {
    println!("{}: {}", author.id, author.name);
}

// :execrows
let deleted = queries::delete_author(&client, 123).await?;
println!("Deleted {} rows", deleted);
```

### Benefits

- **Type Safety:** Compile-time verification of query parameters and results
- **Nullability Analysis:** Superior to most SQL generators - analyzes JOINs, views, expressions
- **Named Parameters:** Clear, self-documenting parameter names with `:param`
- **View Composition:** Queries on views get correct nullability transitively
- **SQL First:** Write idiomatic SQL, not ORM abstractions
- **Documentation:** SQL queries included in generated doc comments
- **Incremental:** Works alongside function-based approach

## Decode errors

When a row column cannot be read into its generated Rust type, the error names
the column rather than just its position:

```
GetUserRow: failed to decode column `created_at` (index 7): error deserializing column 7: …
```

It is `PgRpcError::Decode(DecodeError)` with `item`, `column`, `index` and a
boxed `source`, reachable through every generated error enum's `Other`
variant. The source is a `tokio_postgres::Error` when a value failed to
convert, and a plain message when a query row's column could not be located
by name (it is gone, or the result now has two columns of that name). The
usual cause is a database that differs from the one the code was generated
against.

## Schema changes under a running client

Code generated against one schema keeps working after the schema gains things,
so a migration that only adds can be applied while older processes are still
serving:

| Added to the schema | An older client |
|---|---|
| Enum label | Reads and writes the labels it knows. A value carrying the new label fails to decode, as a `DecodeError`. |
| Composite attribute | Reads ignore it. Writes send it as NULL. |
| Column appended to a view or table that a function returns rows of | Never receives it: generated calls select the columns known at generation time, by name. |
| Column added to a table or view behind a `*` in a query file | Ignored wherever it lands, once the statement is re-prepared (see the limits below). |
| Column added to a `RETURNS TABLE` result | Same. |
| Parameter with a default | Unaffected. |

Removing, renaming or retyping any of these still fails, loudly.

Two limits:

- **Composite writes on a connection opened before the change fail until the
  connection forgets the old type.** A connection caches each type's attribute
  list, so it encodes the composite the old way and Postgres rejects it
  (`42804`, "wrong number of columns"). Reads on the same connection are fine.
  Generated functions only see a `GenericClient`, which cannot drop that
  cache, so the generated `schema_cache` module does it from the pool:

  ```rust
  // On every pool:
  let pool = config
      .builder(tls)?
      .runtime(deadpool_postgres::Runtime::Tokio1)
      .pre_recycle(schema_cache::hook())
      .build()?;

  // When the application learns a migration committed:
  schema_cache::invalidate();
  ```

  Each connection then drops its type and statement caches at its next
  checkout. A connection that is checked out while the migration commits can
  still fail one write before it is returned.
- **Query files with a `*` fail on a connection that prepared them before the
  change**, with "cached plan must not change result type", until that
  connection's statement cache is dropped (`schema_cache` above does this).
  Once re-prepared they decode correctly wherever the new column landed: row
  structs find their columns by name. For the same reason a query whose
  result has two columns with one name is rejected at generation time. Naming
  the columns instead of using `*` avoids the failure altogether.

Sending NULL for an unknown attribute means an older client that reads a
composite, changes it and writes it back clears the new attribute.

## Error analysis scope

Every PL/pgSQL and SQL function in every user schema is introspected, but Rust
bindings are only written for the schemas listed in `schemas`. The rest exist so
that error analysis can see through calls:

```sql
create function core.reserve_stock(p_listing_id int, p_qty int) returns void
language plpgsql as $$
begin
  update listing set stock = stock - p_qty where listing_id = p_listing_id;
  if not found then
    raise exception 'no such listing' using errcode = 'M0404';
  end if;
end $$;

create function api.checkout(p_listing_id int, p_qty int) returns void
language plpgsql as $$
begin
  perform core.reserve_stock(p_listing_id, p_qty);
end $$;
```

With `schemas = ["api"]`, `CheckoutError` carries both a `ListingConstraint`
variant (for the `update` inside the helper) and the `M0404` variant, exactly as
if the statements were written in `api.checkout` itself. Calls are followed
transitively and through SQL-language helpers; recursion is fine.

A call is followed when it is a static, direct call: `perform s.f(...)`,
`v := s.f(...)`, `select s.f(...) into ...`, `select * from s.f(...)`,
`call s.p(...)`, or a call nested inside one of those expressions. The callee is
resolved by name: a schema-qualified call directly, an unqualified call first in
the caller's schema and then along the connection's `search_path`. Dynamic SQL
(`execute`), overload selection by argument type, and functions from extensions
are not followed. When a function is overloaded, one overload is bound (fewest
arguments first) and a warning names the ones skipped.

The same applies to [SQL query files](#sql-query-files): a query such as
`select core.reserve_stock(:listing_id, :qty)` gets the helper's variants on its
error enum. An unqualified call in a query file resolves along the `search_path`.

Triggers are followed the same way: the triggers that fire for a function's
DML contribute their raises, the raises of the functions they call, and the
constraints of the tables they write. Those writes fire triggers of their own,
which are followed too, to any depth. A `delete` contributes the `RESTRICT` and
`NO ACTION` foreign keys of the tables that reference the deleted one, under
those tables' enums, since that is where Postgres reports the violation. An
`update` that sets a referenced key column does the same for the foreign keys
whose `ON UPDATE` action is `RESTRICT` or `NO ACTION`.

The statement-level rules:

- `insert` contributes every constraint of the target table (omitted columns get
  defaults, which are checked too). `on conflict (cols)` removes the unique key
  it names; `do update` adds the constraints on the columns it sets.
- `update` and `delete` contribute the constraints on the columns written, and
  `merge` contributes whichever of the three its `when` clauses perform.
- `ON DELETE`/`ON UPDATE` `CASCADE`, `SET NULL` and `SET DEFAULT` are followed
  as writes to the referencing table, with that table's triggers and foreign
  keys, to any depth.
- Partial and expression unique indexes are included. A write to a partitioned
  table also counts as a write to its partitions, and a write through a
  single-table view reaches the base table.
- `select ... into strict` and `execute ... into strict` add
  `no_data_found`/`too_many_rows`; `assert` adds `P0004`.

Exception handlers are not modelled: an error a function catches still appears
on its enum.

Because the analysis over-approximates, a helper used by many functions makes
its callers' error enums larger. Errors raised from dynamic SQL (a `raise`
behind `execute`) are not seen; they arrive through the enum's `Other`
variant.

## User-facing constraint messages

A constraint violation is typed (`CreateSubmissionError::SubmissionConstraint(SubmissionConstraint::TitleLength, db)`)
but the Postgres message is not something to show a user. Put the message in
the constraint's comment:

```sql
comment on constraint title_length on submission is $$
  Printed on the certificate, where there is room for this much.

  @pgrpc_message Title must be 60 characters or fewer
$$;

-- A multi-column CHECK: form-level rather than tied to a field.
comment on constraint token_follows_grading on submission is $$
  @pgrpc_message A token can only be assigned once the piece is graded
  @pgrpc_field()
$$;

-- The form field is not named after the column.
comment on constraint promotion_code_key on promotion is $$
  @pgrpc_message That code isn't valid
  @pgrpc_field(promo_code)
$$;

-- Domain CHECKs work the same way.
comment on constraint postcode_format on domain postcode is '@pgrpc_message Not a valid postcode';
```

`@pgrpc_message` runs from the tag to the end of the line. The field defaults
to the constraint's single column; `@pgrpc_field(name)` overrides it and
`@pgrpc_field()` means none. The rest of the comment stays documentation, and
a constraint without the tag is untouched.

Every per-table constraint enum gets `name()` (the Postgres constraint name,
also what `Display` prints), `message()`, `field()` and `user_message()`. For
violations that reached you without a typed variant, every generated error
enum, and a bare `DbError` destructured out of one, has `user_message()`. It
looks the message up by the schema, table (or domain) and constraint names on
the `DbError`, or for a typed error by the `@pgrpc_message` on its payload
type (see below):

```rust
use db::errors::{PgRpcErrorExt, ConstraintMessage};

fn validation_from(err: &impl PgRpcErrorExt) -> Option<ValidationErrors> {
    let ConstraintMessage { message, field } = err.user_message()?;
    Some(match field {
        Some(f) => ValidationErrors::new().add_field_error(f, message),
        None => ValidationErrors::new().add_form_error(message),
    })
}
```

The underlying lookups, `constraint_message(schema, table, constraint)` and
`domain_constraint_message(schema, domain, constraint)`, are public in the
`errors` module; `schema` is an `Option` and `None` takes the first match. A
domain CHECK has no column to default to, so only an explicit `@pgrpc_field`
attaches a field there. Not-null violations have no comment target in
Postgres; they already report `column()`.

## Typed errors

A plain `RAISE ... USING ERRCODE = 'M0422'` surfaces as a variant carrying the
`DbError`, named after the `[exceptions]` entry for the code if there is one,
otherwise the standard condition name (`UniqueViolation`, `NoDataFound`), and
otherwise the raw code (`M0422`). `select ... into strict` contributes
`NoDataFound` and `TooManyRows`. For errors that need structured data, put
composite types in an errors schema and enable it:

```rust
PgrpcBuilder::new()
    .enable_errors("errors")   // schema holding the payload types;
                               // raise helper defaults to core.raise_error
```

```toml
[errors]
schema = "errors"
raise_function = "core.raise_error"
```

```sql
create type errors.insufficient_inventory as (item_id bigint, requested int, available int);

create procedure core.raise_error(p_error anyelement, p_sqlstate text default 'M0422')
language plpgsql as $$
begin
  raise exception using
    errcode = p_sqlstate,
    hint    = 'application/json',
    message = (jsonb_build_object('type', substring(pg_typeof(p_error)::text from '([^.]+)$'))
               || to_jsonb(p_error))::text;
end $$;

-- anywhere, including helpers and triggers:
call core.raise_error(row(p_item_id, p_qty, v_available)::errors.insufficient_inventory);
```

Every composite type in the schema becomes a payload struct in
`custom_errors.rs`. A function, trigger or query that raises one (directly or
through any function it calls) gets a variant holding both the deserialized
payload and the `DbError`:

```rust
match api::reserve(&client, item_id, qty).await {
    Err(ReserveError::InsufficientInventory(payload, db)) => {
        // payload.available, db.column(), …
    }
    …
}
```

The variant is selected by the `application/json` hint and the `type` field
in the message, before any SQLSTATE matching, so a typed error raised with a
mapped code such as `M0422` is still the typed variant. A hand-written `RAISE`
that sets the hint and builds its message with a literal `'type', 'name'` pair
is recognised too.

Because a typed error's `message()` is JSON, a generic "show the message"
path should refuse them: `is_typed_error()` and `typed_error_name()` are on
`PgRpcErrorExt`. Better, tag the payload type and let `user_message()` cover
it with no Rust arm:

```sql
comment on type errors.insufficient_inventory is $$
  @pgrpc_message Not enough stock for that quantity
  @pgrpc_field(quantity)
$$;
```

## Justification

Currently it is common to write a 'repository layer' for applications that handles persistence with the database.

Traditionally the repository layer provides an abstraction over persistence so that the underlying store can be swapped
out in the future.

This often means that you're reduced to using the 'lowest common denominator' feature set of your database. Usually this
means CRUD and little else.

The motivation behind this library is to experiment with what a PL/PgSql 'repository layer' could look like. Anything
that interacts with the database can only do so through postgres functions. This has numerous benefits, and also some
notable downsides. I think the trade-offs are highly worth it if you're a small team, and you have the ability and
tooling in place to perform database migrations easily and at will.

## Why not JSON?

One solution here is to return JSON. This has the benefit of not requiring you to write composite types and domains for
API responses. However, it comes with numerous downsides:

1. Lack of type safety. Composite and domain types add verbosity, but they also ensure you never return an invalid
   reponse back to the client. They also permit us to generate type definitions in Rust for function responses.
2. Lower performance. Encoding JSON is more expensive than aggregating arrays. Additionally, sending JSON over the wire
   is slower, as it means re-sending all of the column names, and can't take advantage of the binary protocol, so we
   have to parse the JSON on the receiver end.
3. Lossy. JSON only has one `number` type, so we lose information.

## Nullability and CHECK constraints

Columns marked NOT NULL in tables are non-null in the generated Rust code.

Composite types aren't so simple: Postgres doesn't allow NOT NULL on the attributes of
a composite type. An attribute is treated as non-null when either

- its comment carries `@pgrpc_not_null`, or the type's comment carries
  `@pgrpc_not_null(a, b, ...)` or `@pgrpc_not_null(*)`, or
- a domain over the composite has a CHECK constraint proving `(value).attr is not null`
  for every non-null value.

For the CHECK route, a conjunction proves each of its parts and a disjunction proves only
what *every* branch proves. A `value is null` branch is ignored, since it only covers a
NULL composite, which the column's own nullability already handles:

| CHECK constraint                                                   | Non-null attributes |
|--------------------------------------------------------------------|---------------------|
| `(value).a is not null and (value).b is not null`                  | `a`, `b`            |
| `value is null or (value).a is not null`                           | `a`                 |
| `value is null or ((value).a is not null and (value).b is not null)` | `a`, `b`          |
| `(value).a is not null or (value).b is not null`                   | none                |

The same evidence applies when a view or query projects a single attribute out of a
composite-typed column. Given `length distance not null`, where `distance` is a domain
over a composite whose `amount` is annotated or CHECK-proven, `(p.length).amount as
length_amount` generates `length_amount: Decimal` rather than `Option<Decimal>`. The
projection stays `Option` when the base column is nullable or on the outer side of a
join, when there is no evidence for the attribute, or when it is subscripted
(`(p.x).tags[1]`).

## Strict domain types

Domains normally generate transparent newtypes:

```rust
#[derive(Debug, Clone, /* ... */, serde::Deserialize)]
pub struct Zip(pub String);
```

Nothing stops Rust code from constructing or deserializing a value that violates
the domain's CHECK constraint — `Zip("not a zip".into())` compiles fine. Mark a
domain "strict" in `pgrpc.toml` (or via `PgrpcBuilder::strict_domain`) to opt into
validation:

```toml
strict_domains = ["public.zip"]
```

This changes the generated type to:

```rust
#[serde(try_from = "String")]
pub struct Zip(String);          // field is private

impl Zip {
    pub fn new_unchecked(value: String) -> Self;  // bypasses validation
    pub fn into_inner(self) -> String;
    pub fn as_inner(&self) -> &String;            // read access (also via Deref)
}
```

You provide the validation by implementing `TryFrom` — the generated code is
`include!`-d into your crate, so coherence allows this:

```rust
impl TryFrom<String> for db::public::Zip {
    type Error = ZipError;  // must implement Display

    fn try_from(s: String) -> Result<Self, Self::Error> {
        if s.len() == 9 && s.bytes().all(|b| b.is_ascii_digit()) {
            Ok(Self::new_unchecked(s))
        } else {
            Err(ZipError)
        }
    }
}
```

Notes:

- All serde deserialization (JSON APIs, task-queue payloads) now runs your
  validator, and `Zip::try_from(s)` / `s.try_into()` is the validated constructor.
- Values read from Postgres skip validation — the database already enforces the
  CHECK constraint.
- Forgetting the impl fails compilation with
  ``the trait bound `Zip: TryFrom<String>` is not satisfied``.
- Strict mode is not supported for domains over composite types (a warning is
  logged and the annotation is ignored).
- If the domain is also listed in `disable_deserialize`, no `Deserialize` impl is
  generated at all; the field still becomes private and `new_unchecked` /
  `into_inner` are still generated.