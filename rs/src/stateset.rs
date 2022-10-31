use std::collections::BTreeMap;
use diamond_types::{CausalGraph, Time};
use smallvec::SmallVec;

type DocName = Time;

type Pair<T> = (Time, T);

#[derive(Debug, Clone)]
pub(crate) struct StateSet<T: Clone> {
    pub(crate) values: BTreeMap<DocName, SmallVec<[Pair<T>; 1]>>,

    // Internal from version -> value at that version
    pub(crate) index: BTreeMap<Time, DocName>,
    pub(crate) cg: CausalGraph,
}

impl<T: Clone> StateSet<T> {
    pub fn new() -> Self {
        Self {
            values: Default::default(),
            index: Default::default(),
            cg: Default::default()
        }
    }


}