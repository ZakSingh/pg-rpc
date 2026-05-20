//! Benchmark for the per-field-clone cost in generated `ToSql` impls.
//!
//! Two patterns are compared, both modeling what pgrpc emits for a composite:
//!
//! - **Owned** (current codegen): the impl builds `InnerXxx { field: self.field.clone(), .. }`
//!   then calls `inner.to_sql(..)`. Every field is cloned.
//! - **Borrowed** (proposed codegen): the impl builds `InnerXxxRef { field: &self.field, .. }`
//!   then calls `inner.to_sql(..)`. No clones.
//!
//! Both inner structs derive `postgres_types::ToSql`. The derive's composite body
//! only ever reads `&self.field`, which works equally for `T` and `&T` because
//! `impl<T: ToSql> ToSql for &T` exists in postgres-types.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use postgres_types::private::BytesMut;
use postgres_types::{Field, Kind, ToSql, Type};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Owned (current codegen) pattern
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct OwnedRs {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub tag: String,
    pub count: i64,
    pub note: Option<String>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, postgres_types::ToSql)]
#[postgres(name = "bench_composite")]
struct InnerOwned {
    #[postgres(name = "id")]
    id: Uuid,
    #[postgres(name = "name")]
    name: String,
    #[postgres(name = "description")]
    description: String,
    #[postgres(name = "tag")]
    tag: String,
    #[postgres(name = "count")]
    count: i64,
    #[postgres(name = "note")]
    note: Option<String>,
    #[postgres(name = "payload")]
    payload: Vec<u8>,
}

impl ToSql for OwnedRs {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let inner = InnerOwned {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            tag: self.tag.clone(),
            count: self.count.clone(),
            note: self.note.clone(),
            payload: self.payload.clone(),
        };
        inner.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "bench_composite"
    }

    postgres_types::to_sql_checked!();
}

// ---------------------------------------------------------------------------
// Borrowed (proposed codegen) pattern
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct BorrowedRs {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub tag: String,
    pub count: i64,
    pub note: Option<String>,
    pub payload: Vec<u8>,
}

#[derive(Debug, postgres_types::ToSql)]
#[postgres(name = "bench_composite")]
struct InnerBorrowed<'a> {
    #[postgres(name = "id")]
    id: &'a Uuid,
    #[postgres(name = "name")]
    name: &'a String,
    #[postgres(name = "description")]
    description: &'a String,
    #[postgres(name = "tag")]
    tag: &'a String,
    #[postgres(name = "count")]
    count: &'a i64,
    // Modeling the "Option<&T>" plumbing the planned codegen uses for
    // optional-group fields (see plan: spot 1).
    #[postgres(name = "note")]
    note: Option<&'a String>,
    #[postgres(name = "payload")]
    payload: &'a Vec<u8>,
}

impl ToSql for BorrowedRs {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let inner = InnerBorrowed {
            id: &self.id,
            name: &self.name,
            description: &self.description,
            tag: &self.tag,
            count: &self.count,
            note: self.note.as_ref(),
            payload: &self.payload,
        };
        inner.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "bench_composite"
    }

    postgres_types::to_sql_checked!();
}

// ---------------------------------------------------------------------------
// Bench scaffolding
// ---------------------------------------------------------------------------

fn make_composite_type() -> Type {
    // OIDs are arbitrary — to_sql only reads names and kinds from the Type.
    let fields = vec![
        Field::new("id".into(), Type::UUID),
        Field::new("name".into(), Type::TEXT),
        Field::new("description".into(), Type::TEXT),
        Field::new("tag".into(), Type::TEXT),
        Field::new("count".into(), Type::INT8),
        Field::new("note".into(), Type::TEXT),
        Field::new("payload".into(), Type::BYTEA),
    ];
    Type::new(
        "bench_composite".into(),
        100_000,
        Kind::Composite(fields),
        "public".into(),
    )
}

fn sample_owned() -> OwnedRs {
    OwnedRs {
        id: Uuid::from_u128(0xdead_beef_0000_0000_1111_2222_3333_4444),
        name: "the quick brown fox jumps over the lazy dog".to_string(),
        description:
            "a moderately long description that costs real bytes to clone on every to_sql call"
                .to_string(),
        tag: "benchmark-tag-value".to_string(),
        count: 42,
        note: Some("optional note with some content".to_string()),
        payload: vec![0xABu8; 256],
    }
}

fn sample_borrowed() -> BorrowedRs {
    let o = sample_owned();
    BorrowedRs {
        id: o.id,
        name: o.name,
        description: o.description,
        tag: o.tag,
        count: o.count,
        note: o.note,
        payload: o.payload,
    }
}

/// Assert that the two patterns emit byte-identical wire output. Runs once
/// before the benches so a regression in the borrowed path fails loudly
/// instead of quietly producing wrong bytes faster.
fn assert_wire_equivalence() {
    let ty = make_composite_type();
    let owned = sample_owned();
    let borrowed = sample_borrowed();

    let mut buf_owned = BytesMut::with_capacity(4096);
    let mut buf_borrowed = BytesMut::with_capacity(4096);

    owned.to_sql(&ty, &mut buf_owned).unwrap();
    borrowed.to_sql(&ty, &mut buf_borrowed).unwrap();

    assert_eq!(
        &buf_owned[..],
        &buf_borrowed[..],
        "owned and borrowed ToSql produced different wire bytes"
    );
}

fn bench_owned(c: &mut Criterion) {
    assert_wire_equivalence();

    let ty = make_composite_type();
    let value = sample_owned();
    let mut buf = BytesMut::with_capacity(4096);

    c.bench_function("owned_to_sql", |b| {
        b.iter(|| {
            buf.clear();
            let r = black_box(&value).to_sql(black_box(&ty), &mut buf).unwrap();
            black_box(r);
            black_box(&buf);
        })
    });
}

fn bench_borrowed(c: &mut Criterion) {
    let ty = make_composite_type();
    let value = sample_borrowed();
    let mut buf = BytesMut::with_capacity(4096);

    c.bench_function("borrowed_to_sql", |b| {
        b.iter(|| {
            buf.clear();
            let r = black_box(&value).to_sql(black_box(&ty), &mut buf).unwrap();
            black_box(r);
            black_box(&buf);
        })
    });
}

criterion_group!(benches, bench_owned, bench_borrowed);
criterion_main!(benches);
