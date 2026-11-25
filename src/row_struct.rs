//! The one emitter for "a struct with a field per result column, plus the
//! code that reads it from a row".
//!
//! Query rows, function OUT/TABLE rows, composites and views all describe
//! their struct as a [`RowShape`] and get the struct definitions from
//! [`RowShape::emit_structs`] and the `TryFrom<Row>` impl from
//! [`RowShape::emit_decoder`]. Columns named `prefix__field` can be folded
//! into nested structs (see [`crate::column_grouping`]); a group whose columns
//! all come from the nullable side of an outer join is `Option<Struct>`.
//!
//! What genuinely differs between the callers is data, not code: the
//! attributes on each struct and field, the name a decode error reports, and
//! how a column is located in the row ([`Decode`]).

use crate::column_grouping::{group_by_double_underscore, GroupedField};
use crate::ident::{dedupe_idents, ident_tokens, sql_to_rs_string, CaseType};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/// Where each result column goes: straight onto the row struct, or into a
/// group. Indices are positions in the column list the layout was built from.
#[derive(Debug)]
pub enum Slot {
    Plain(usize),
    Group {
        /// The shared prefix (`owner` of `owner__name`).
        name: String,
        optional: bool,
        /// Each member's index and its name without the prefix.
        members: Vec<(usize, String)>,
    },
}

/// One slot per column, no grouping.
pub fn flat_layout(column_count: usize) -> Vec<Slot> {
    (0..column_count).map(Slot::Plain).collect()
}

/// Folds `prefix__field` columns into groups. Each column is given as the
/// name to group on and whether it is nullable only because of an outer join.
pub fn grouped_layout<'a>(columns: impl IntoIterator<Item = (&'a str, bool)>) -> Vec<Slot> {
    let items: Vec<(usize, String, bool)> = columns
        .into_iter()
        .enumerate()
        .map(|(i, (name, join_nullable))| (i, name.to_string(), join_nullable))
        .collect();
    group_by_double_underscore(
        items,
        |(_, name, _)| name,
        |(_, _, join_nullable)| *join_nullable,
        |(_, name, _), stripped| *name = stripped,
    )
    .into_iter()
    .map(|slot| match slot {
        GroupedField::Plain((index, _, _)) => Slot::Plain(index),
        GroupedField::Group {
            group_name,
            fields,
            optional,
        } => Slot::Group {
            name: group_name,
            optional,
            members: fields.into_iter().map(|(index, name, _)| (index, name)).collect(),
        },
    })
    .collect()
}

/// A column as the callbacks of [`RowShape::build`] see it.
#[derive(Debug, Clone, Copy)]
pub struct Place<'a> {
    /// Position in the column list.
    pub index: usize,
    /// For a column inside a group, its name without the group prefix.
    pub member: Option<&'a str>,
}

/// What a caller says about one field once its Rust name is settled.
pub struct FieldParts {
    /// The column's name in the row, for decoders that look it up by name.
    pub column: String,
    /// The column name a decode error reports.
    pub label: String,
    /// The field's full Rust type, `Option` included.
    pub ty: TokenStream,
    /// Doc and serde attributes.
    pub attrs: TokenStream,
}

#[derive(Debug)]
pub struct RowField {
    /// Position of the column in the result, as of generation time.
    pub index: usize,
    pub column: String,
    pub label: String,
    /// Field name, unique within its struct.
    pub ident: TokenStream,
    pub ty: TokenStream,
    pub attrs: TokenStream,
}

#[derive(Debug)]
pub struct RowGroup {
    /// Field name on the row struct.
    pub ident: TokenStream,
    /// Name of the nested struct.
    pub struct_name: proc_macro2::Ident,
    pub optional: bool,
    pub fields: Vec<RowField>,
}

#[derive(Debug)]
pub enum RowMember {
    Field(RowField),
    Group(RowGroup),
}

/// How the generated decoder finds a column in a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decode {
    /// At its generation-time position: the statement names its columns, so
    /// the positions are fixed (function calls).
    Position,
    /// By name, resolved to positions once per result set: emits `COLUMNS`,
    /// `decode_all` and `decode(row, at)` next to `TryFrom<Row>` (queries).
    ResolvedName,
    /// By name on every read (composites and views selected as rows).
    /// `locals` binds each column to a local before the struct is built.
    Name { locals: bool },
}

/// A row struct: its name and, in order, its fields and groups.
#[derive(Debug)]
pub struct RowShape {
    pub name: TokenStream,
    /// `name` as decode errors report it.
    item: String,
    pub members: Vec<RowMember>,
}

impl RowShape {
    /// Builds the shape for `layout`.
    ///
    /// `candidate` proposes a Rust name for a column; names are then made
    /// unique per struct (the row struct's fields and groups together, and
    /// the fields of each group), and `finish` supplies the rest of the field
    /// given the name it ended up with. Nested structs are named
    /// `{nested_prefix}{Group}`.
    pub fn build(
        name: TokenStream,
        nested_prefix: &str,
        layout: Vec<Slot>,
        candidate: impl Fn(Place) -> String,
        finish: impl Fn(Place, &TokenStream) -> FieldParts,
    ) -> Self {
        let field = |place: Place, ident: &str| {
            let ident = ident_tokens(ident);
            let parts = finish(place, &ident);
            RowField {
                index: place.index,
                column: parts.column,
                label: parts.label,
                ident,
                ty: parts.ty,
                attrs: parts.attrs,
            }
        };

        let top_idents = dedupe_idents(
            layout
                .iter()
                .map(|slot| match slot {
                    Slot::Plain(index) => candidate(Place { index: *index, member: None }),
                    Slot::Group { name, .. } => sql_to_rs_string(name, CaseType::Snake),
                })
                .collect(),
        );
        let mut struct_names = dedupe_idents(
            layout
                .iter()
                .filter_map(|slot| match slot {
                    Slot::Group { name, .. } => Some(format!(
                        "{}{}",
                        nested_prefix,
                        sql_to_rs_string(name, CaseType::Pascal)
                    )),
                    Slot::Plain(_) => None,
                })
                .collect(),
        )
        .into_iter();

        let members = layout
            .iter()
            .zip(&top_idents)
            .map(|(slot, top_ident)| match slot {
                Slot::Plain(index) => {
                    RowMember::Field(field(Place { index: *index, member: None }, top_ident))
                }
                Slot::Group { optional, members, .. } => {
                    let places: Vec<Place> = members
                        .iter()
                        .map(|(index, name)| Place { index: *index, member: Some(name) })
                        .collect();
                    let idents = dedupe_idents(places.iter().map(|p| candidate(*p)).collect());
                    RowMember::Group(RowGroup {
                        ident: ident_tokens(top_ident),
                        struct_name: format_ident!(
                            "{}",
                            struct_names.next().expect("one name per group")
                        ),
                        optional: *optional,
                        fields: places
                            .iter()
                            .zip(&idents)
                            .map(|(place, ident)| field(*place, ident))
                            .collect(),
                    })
                }
            })
            .collect();

        RowShape {
            item: name.to_string(),
            name,
            members,
        }
    }

    /// Every column's field, in column order.
    pub fn columns(&self) -> Vec<&RowField> {
        let mut fields: Vec<&RowField> = self
            .members
            .iter()
            .flat_map(|member| match member {
                RowMember::Field(field) => std::slice::from_ref(field),
                RowMember::Group(group) => group.fields.as_slice(),
            })
            .collect();
        fields.sort_by_key(|f| f.index);
        fields
    }

    /// The nested group structs followed by the row struct. `attrs` go on the
    /// row struct and `group_attrs` on each nested one (docs, derives).
    pub fn emit_structs(&self, attrs: &TokenStream, group_attrs: &TokenStream) -> TokenStream {
        let field_def = |f: &RowField| {
            let RowField { ident, ty, attrs, .. } = f;
            quote! {
                #attrs
                pub #ident: #ty
            }
        };

        let nested = self.members.iter().filter_map(|member| match member {
            RowMember::Group(group) => {
                let struct_name = &group.struct_name;
                let fields = group.fields.iter().map(field_def);
                Some(quote! {
                    #group_attrs
                    pub struct #struct_name {
                        #(#fields),*
                    }
                })
            }
            RowMember::Field(_) => None,
        });

        let fields = self.members.iter().map(|member| match member {
            RowMember::Field(field) => field_def(field),
            RowMember::Group(RowGroup { ident, struct_name, optional: true, .. }) => {
                quote! { pub #ident: Option<#struct_name> }
            }
            RowMember::Group(RowGroup { ident, struct_name, .. }) => {
                quote! { pub #ident: #struct_name }
            }
        });

        let name = &self.name;
        quote! {
            #(#nested)*

            #attrs
            pub struct #name {
                #(#fields),*
            }
        }
    }

    /// `impl TryFrom<tokio_postgres::Row>` for the row struct, plus whatever
    /// else `decode` calls for.
    pub fn emit_decoder(&self, decode: Decode) -> TokenStream {
        let locals = decode == Decode::Name { locals: true };
        let value = |f: &RowField| {
            if locals {
                let local = local_ident(f);
                quote! { #local }
            } else {
                self.read(f, decode)
            }
        };
        let bind = |f: &RowField| {
            let local = local_ident(f);
            let read = self.read(f, decode);
            quote! { let #local = #read; }
        };

        // Locals read before the struct is built (`locals` only).
        let mut bindings: Vec<TokenStream> = Vec::new();
        let mut assignments: Vec<TokenStream> = Vec::new();
        for member in &self.members {
            match member {
                RowMember::Field(f) => {
                    let ident = &f.ident;
                    let value = value(f);
                    if locals {
                        bindings.push(bind(f));
                    }
                    assignments.push(quote! { #ident: #value });
                }
                RowMember::Group(group) => {
                    let RowGroup { ident, struct_name, .. } = group;
                    let fields = group.fields.iter().map(|f| {
                        let ident = &f.ident;
                        let value = value(f);
                        quote! { #ident: #value }
                    });
                    let construct = quote! {
                        #struct_name {
                            #(#fields),*
                        }
                    };
                    let group_bindings: Vec<TokenStream> = if locals {
                        group.fields.iter().map(bind).collect()
                    } else {
                        Vec::new()
                    };
                    if group.optional {
                        // Every member comes from the same outer-joined
                        // relation, so they are NULL together: probe the
                        // first and only read the rest when it is present.
                        // `NullProbe` accepts any Postgres type, where a
                        // typed sentinel would fail the type check for every
                        // column that is not of that type.
                        let is_null = self.probe(&group.fields[0], decode);
                        assignments.push(quote! {
                            #ident: if #is_null {
                                None
                            } else {
                                #(#group_bindings)*
                                Some(#construct)
                            }
                        });
                    } else {
                        bindings.extend(group_bindings);
                        assignments.push(quote! { #ident: #construct });
                    }
                }
            }
        }

        let name = &self.name;
        let item = &self.item;
        if decode != Decode::ResolvedName {
            return quote! {
                impl TryFrom<tokio_postgres::Row> for #name {
                    type Error = super::errors::DecodeError;

                    fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
                        #(#bindings)*

                        Ok(Self {
                            #(#assignments),*
                        })
                    }
                }
            };
        }

        // `decode(row, at)` reads column `i` of the generated list from
        // position `at[i]`.
        let columns = self.columns();
        let column_count = columns.len();
        let names = columns.iter().map(|f| f.column.as_str());
        quote! {
            impl #name {
                /// Result columns as they were at generation time.
                const COLUMNS: [&'static str; #column_count] = [#(#names),*];

                /// Decode a whole result set, locating the columns once.
                pub fn decode_all(
                    rows: Vec<tokio_postgres::Row>,
                ) -> Result<Vec<Self>, super::errors::DecodeError> {
                    let Some(first) = rows.first() else {
                        return Ok(Vec::new());
                    };
                    let at = super::errors::column_positions(#item, &Self::COLUMNS, first)?;
                    rows.iter().map(|row| Self::decode(row, &at)).collect()
                }

                fn decode(
                    row: &tokio_postgres::Row,
                    at: &[usize; #column_count],
                ) -> Result<Self, super::errors::DecodeError> {
                    Ok(Self {
                        #(#assignments),*
                    })
                }
            }

            impl TryFrom<tokio_postgres::Row> for #name {
                type Error = super::errors::DecodeError;

                fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
                    let at = super::errors::column_positions(#item, &Self::COLUMNS, &row)?;
                    Self::decode(&row, &at)
                }
            }
        }
    }

    /// `row.try_get(<column>)?`, a failure attributed to this struct and the
    /// column as a `DecodeError`.
    fn read(&self, f: &RowField, decode: Decode) -> TokenStream {
        let (at, err) = self.locate(f, decode);
        quote! { row.try_get(#at).map_err(#err)? }
    }

    /// Whether the column is NULL, read without committing to its type.
    fn probe(&self, f: &RowField, decode: Decode) -> TokenStream {
        let (at, err) = self.locate(f, decode);
        let key = match decode {
            Decode::Name { .. } => quote! { &str },
            Decode::Position | Decode::ResolvedName => quote! { usize },
        };
        quote! {
            row.try_get::<#key, super::nullable::NullProbe>(#at)
                .map_err(#err)?
                .is_null
        }
    }

    /// The `try_get` argument for a column and the `map_err` closure for it.
    fn locate(&self, f: &RowField, decode: Decode) -> (TokenStream, TokenStream) {
        let RowField { index, column, label, .. } = f;
        let item = &self.item;
        let (at, position) = match decode {
            Decode::Position => (quote! { #index }, quote! { #index }),
            Decode::ResolvedName => (quote! { at[#index] }, quote! { at[#index] }),
            Decode::Name { .. } => (quote! { #column }, quote! { #index }),
        };
        (
            at,
            quote! { super::errors::DecodeError::at(#item, #label, #position) },
        )
    }
}

/// The local a column is bound to, named by position so that no column name
/// can yield an invalid or clashing identifier.
fn local_ident(f: &RowField) -> proc_macro2::Ident {
    format_ident!("__c{}", f.index)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(columns: &[(&str, bool)]) -> RowShape {
        RowShape::build(
            quote! { Row },
            "Row",
            grouped_layout(columns.iter().copied()),
            |place| sql_to_rs_string(place.member.unwrap_or(columns[place.index].0), CaseType::Snake),
            |place, _| FieldParts {
                column: columns[place.index].0.to_string(),
                label: columns[place.index].0.to_string(),
                ty: quote! { i32 },
                attrs: quote! {},
            },
        )
    }

    fn compact(tokens: TokenStream) -> String {
        tokens.to_string().replace(' ', "")
    }

    #[test]
    fn clashing_names_are_made_unique_at_every_level() {
        let shape = shape(&[
            ("owner", false),
            ("owner__id", false),
            ("owner__Id", false),
            ("Owner__x", false),
        ]);
        let structs = compact(shape.emit_structs(&quote! {}, &quote! {}));
        assert!(structs.contains("pubstructRowOwner{pubid:i32,pubid2:i32}"), "{structs}");
        assert!(structs.contains("pubstructRowOwner2{pubx:i32}"), "{structs}");
        assert!(
            structs.contains("pubstructRow{pubowner:i32,pubowner2:RowOwner,pubowner3:RowOwner2}"),
            "{structs}"
        );
    }

    #[test]
    fn optional_group_is_probed_before_it_is_read() {
        let shape = shape(&[("id", false), ("o__a", true), ("o__b", true)]);
        assert!(compact(shape.emit_structs(&quote! {}, &quote! {})).contains("pubo:Option<RowO>"));

        let by_position = compact(shape.emit_decoder(Decode::Position));
        assert!(by_position.contains("o:ifrow.try_get::<usize,super::nullable::NullProbe>(1usize)"));
        assert!(by_position.contains("a:row.try_get(1usize)"));

        let resolved = compact(shape.emit_decoder(Decode::ResolvedName));
        assert!(resolved.contains(r#"constCOLUMNS:[&'staticstr;3usize]=["id","o__a","o__b"];"#));
        assert!(resolved.contains("b:row.try_get(at[2usize])"));

        let by_name = compact(shape.emit_decoder(Decode::Name { locals: true }));
        assert!(by_name.contains(r#"let__c0=row.try_get("id")"#));
        assert!(by_name.contains(r#"else{let__c1=row.try_get("o__a")"#), "{by_name}");
        assert!(by_name.contains("Some(RowO{a:__c1,b:__c2})"));
    }
}
