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