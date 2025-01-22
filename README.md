# Justification

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

If I have a query that does array aggs,

## Nullability and CHECK constraints

Columns marked non-null in tables are non-null in the generated Rust code.
Domains aren't so simple; we have to parse their check constraints to determine nullability.

`(value).a is not null or (value).b is not null`
Means that `a` and `b` will be `Option`.

If the `is not null` has an ancestor `or`, the column is `Option`. Otherwise it is non-null.

If the inner type of a domain is a composite type, it should just create a new composite type with the name of the
domain,
rather than needing a separate domain type.

Do domains have any purpose for the end user? ALL they can do is add constraints to their inner type. So may as well
pass it through.