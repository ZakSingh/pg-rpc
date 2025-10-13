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

Columns marked non-null in tables are non-null in the generated Rust code.
Domains aren't so simple; we have to parse their check constraints to determine nullability.

`(value).a is not null or (value).b is not null`
Means that `a` and `b` will be `Option`.

If the `is not null` has an ancestor `or`, the column is `Option`. Otherwise it is non-null.