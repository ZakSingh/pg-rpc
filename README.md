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
SELECT * FROM authors WHERE id = @author_id LIMIT 1;

-- name: ListAuthors :many
SELECT * FROM authors ORDER BY name;

-- name: CreateAuthor :one
INSERT INTO authors (name, bio)
VALUES (@name, @bio)
RETURNING *;

-- name: UpdateAuthor :exec
UPDATE authors
SET name = @name, bio = @bio
WHERE id = @author_id;

-- name: DeleteAuthor :execrows
DELETE FROM authors WHERE id = @author_id;
```

2. **Configure in `pgrpc.toml`:**

```toml
[queries]
paths = ["queries/**/*.sql"]  # Glob patterns for SQL files
```

### Query Annotations

**Format:** `-- name: FunctionName :type`

**Query Types:**
- `:one` - Returns `Result<Option<T>, Error>` for single row queries (uses `query_opt`)
- `:many` - Returns `Result<Vec<T>, Error>` for multiple row queries (uses `query`)
- `:exec` - Returns `Result<(), Error>` for commands with no return value
- `:execrows` - Returns `Result<u64, Error>` for commands returning affected row count

### Parameter Syntax

**Named Parameters (Recommended):** Use `@param_name` for explicit parameter names:

```sql
-- name: GetUserByEmail :one
SELECT id, name, email FROM users WHERE email = @user_email;
```

Generates:
```rust
pub async fn get_user_by_email(
    client: &impl deadpool_postgres::GenericClient,
    user_email: &str,
) -> Result<Option<GetUserByEmailRow>, tokio_postgres::Error>
```

**Positional Parameters:** Use `$1, $2, ...` for PostgreSQL standard syntax:

```sql
-- name: UpdateUser :exec
UPDATE users SET name = $1 WHERE id = $2;
```

Parameter names will be inferred from context or use `param_1`, `param_2` as fallbacks.

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
) -> Result<Vec<GetUserPostsRow>, tokio_postgres::Error> {
    // Generated implementation
}
```

### Generated Code

For each query, PgRPC generates:

1. **Row Struct** (for `:one` and `:many` queries):
   - Named after the query (e.g., `GetAuthorRow`)
   - Fields with proper nullability from analysis
   - `TryFrom<tokio_postgres::Row>` implementation

2. **Async Function**:
   - Named after the query in snake_case
   - Accepts `&impl deadpool_postgres::GenericClient`
   - Type-safe parameters
   - Appropriate return type based on query type
   - SQL included in doc comments

**Example Generated Code:**

```rust
/// Query: GetAuthor
///
/// SQL:
/// ```sql
/// SELECT * FROM authors WHERE id = $1 LIMIT 1
/// ```
pub async fn get_author(
    client: &impl deadpool_postgres::GenericClient,
    author_id: i32,
) -> Result<Option<GetAuthorRow>, tokio_postgres::Error> {
    let query = "SELECT id, name, bio FROM authors WHERE id = $1 LIMIT 1";
    let params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![&author_id];

    let row = client.query_opt(query, &params).await?;
    match row {
        Some(row) => Ok(Some(row.try_into()?)),
        None => Ok(None),
    }
}
```

### Usage

```rust
use crate::generated::queries;

// Query single row
let author = queries::get_author(&client, 123).await?;
if let Some(author) = author {
    println!("Author: {}", author.name);
}

// Query multiple rows
let authors = queries::list_authors(&client).await?;
for author in authors {
    println!("{}: {}", author.id, author.name);
}

// Execute command
queries::delete_author(&client, 123).await?;

// Execute with row count
let deleted = queries::delete_author(&client, 123).await?;
println!("Deleted {} rows", deleted);
```

### Benefits

- **Type Safety:** Compile-time verification of query parameters and results
- **Nullability Analysis:** Superior to most SQL generators - analyzes JOINs, views, expressions
- **Named Parameters:** Clear, self-documenting parameter names with `@param`
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

It is `PgRpcError::Decode(DecodeError)` with `item`, `column`, `index` and the
`tokio_postgres::Error` source, reachable through every generated error enum's
`Other` variant. The usual cause is a database that differs from the one the
code was generated against, for example a `select *` over a table whose column
order changed.

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
the caller's schema and then in `public`. Dynamic SQL (`execute`), overload
selection by argument type, and functions from extensions are not followed.

The same applies to [SQL query files](#sql-query-files): a query such as
`select core.reserve_stock(:listing_id, :qty)` gets the helper's variants on its
error enum. An unqualified call in a query file resolves in `public`.

Triggers are followed the same way: the triggers that fire for a function's
DML contribute their raises, the raises of the functions they call, and the
constraints of the tables they write. A `delete` contributes the `RESTRICT` and
`NO ACTION` foreign keys of the tables that reference the deleted one, under
those tables' enums, since that is where Postgres reports the violation.

Because the analysis over-approximates, a helper used by many functions makes
its callers' error enums larger. If a specific function needs an extra variant
that the analysis cannot see (a `raise` behind `execute`, for example), put
`@pgrpc_throws XXXXX` in the function's comment.

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

- its comment carries `@pgrpc_not_null` (or the type's comment carries
  `@pgrpc_not_null(a, b, ...)`), or
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
domain "strict" to opt into validation:

```sql
COMMENT ON DOMAIN zip IS '@pgrpc_strict';
```

or equivalently in `pgrpc.toml` (or via `PgrpcBuilder::strict_domain`):

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