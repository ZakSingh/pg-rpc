use crate::codegen::OID;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
pub enum ConstraintKind {
    #[serde(rename = "c")]
    Check,
    #[serde(rename = "f")]
    ForeignKey,
    #[serde(rename = "p")]
    PrimaryKey,
    #[serde(rename = "u")]
    Unique,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct Constraint {
    kind: ConstraintKind,
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PgRel {
    oid: OID,
    constraints: HashSet<Constraint>,
}

#[derive(Debug, Default)]
pub struct RelIndex(HashMap<OID, PgRel>);

impl Deref for RelIndex {
    type Target = HashMap<OID, PgRel>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RelIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
