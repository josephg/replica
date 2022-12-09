use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::Debug;
use diamond_types::{AgentId, CausalGraph, DTRange, LV};
use diamond_types::causalgraph::agent_assignment::remote_ids::RemoteVersion;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use smallvec::{SmallVec, smallvec};
use crate::cg_hacks::{merge_partial_versions, PartialCGEntry, serialize_cg_from_version};

pub type DocName = LV;

type Pair<T> = (LV, T);
// type RawPair<T> = (RemoteId, T);
type RawPair<'a, T> = (RemoteVersion<'a>, T);

// type RawVersionRef<'a> = (&'a str, usize);
// type RawVersion = (SmartString, usize);
// type RawVersion = RemoteId;

// fn borrow_rv(rv: &RawVersion) -> RawVersionRef<'_> {
//     (rv.agent.as_str(), rv.seq)
// }

#[derive(Debug, Clone)]
pub(crate) struct StateSet<T: Clone> {
    pub(crate) values: BTreeMap<DocName, SmallVec<[Pair<T>; 1]>>,

    // Internal from version -> value at that version
    pub(crate) index: BTreeMap<LV, DocName>,

    // pub(crate) version: Frontier,
    pub(crate) cg: CausalGraph,
}

impl<T: Clone> StateSet<T> {
    pub fn new() -> Self {
        Self {
            values: Default::default(),
            index: Default::default(),
            // version: Default::default(),
            cg: Default::default()
        }
    }

    pub fn print_values(&self) where T: Debug {
        for (key, pairs) in &self.values {
            println!("{key}: {:?}", pairs);
        }
    }

    pub fn local_set(&mut self, agent_id: AgentId, key: Option<LV>, value: T) -> LV {
        let v = self.cg.assign_local_op(agent_id, 1).start;

        let key = key.unwrap_or(v);
        let old_pairs = self.values.insert(key, smallvec![(v, value)]);

        if let Some(old_pairs) = old_pairs {
            for (v2, _) in old_pairs {
                self.index.remove(&v2);
            }
        }

        self.index.insert(v, key);

        v
    }

    pub fn local_insert(&mut self, agent_id: AgentId, value: T) -> LV {
        self.local_set(agent_id, None, value)
    }

    pub fn modified_keys_since_v(&self, since_v: LV) -> impl Iterator<Item=DocName> + '_ {
        self.index.range(since_v..).map(|(_v, &key)| {
            key
        })
    }

    pub fn modified_keys_since_frontier(&self, since: &[LV]) -> impl Iterator<Item=DocName> + '_ {
        let diff = self.cg.graph.diff(since, self.cg.version.as_ref()).1;
        diff.into_iter().flat_map(|range| {
            self.index.range(range).map(|(_v, &key)| {
                key
            })
        })
    }

    // Could take &self here but we need to separate cg for the borrowck.
    fn raw_to_v(cg: &CausalGraph, rv: RemoteVersion) -> LV {
        cg.agent_assignment.remote_to_local_version(rv)
    }

    // fn add_index(&mut self, v: Time, key: DocName) {
    //     todo!();
    // }
    // fn remove_index(&mut self, v: Time) {
    //     todo!();
    // }

    /// The causal graph must be updated before this is called.
    fn merge_set(&mut self, key_raw: RemoteVersion<'_>, mut given_raw_pairs: SmallVec<[RawPair<T>; 2]>)
        // where T: Clone
    {
        let key = Self::raw_to_v(&self.cg, key_raw);

        match self.values.entry(key) {
            Entry::Vacant(e) => {
                // Just insert the received value.
                e.insert(given_raw_pairs.into_iter().map(|(rv, val)| {
                    let lv = self.cg.agent_assignment.remote_to_local_version(rv);
                    self.index.insert(lv, key);
                    (lv, val)
                }).collect());
            }
            Entry::Occupied(mut e) => {
                // Merge the new entry with our existing entry. Usually this will be a 1-1 swap,
                // but we need to handle cases of concurrent writes too.
                let val = e.get_mut();
                if val.len() == 1 && given_raw_pairs.len() == 1 {
                    let old_lv = val[0].0;
                    let new_lv = self.cg.agent_assignment.remote_to_local_version(given_raw_pairs[0].0);

                    if let Some(ord) = self.cg.graph.version_cmp(new_lv, old_lv) {
                        if ord == Ordering::Greater {
                            // Replace it.
                            let pair = given_raw_pairs.remove(0); // This is weird.
                            drop(given_raw_pairs);
                            val[0] = (new_lv, pair.1);
                            // val[0] = given_raw_pairs[0].1.clone();
                            self.index.remove(&old_lv);
                            self.index.insert(new_lv, key);
                        } // Else the new item is old. Ignore it!
                        return;
                    } // else they're concurrent. Fall through below.
                }

                // Slow mode. Find all the versions at play, figure out which ones to keep and
                // build the new pairs list from that.

                // TODO: Using an arena allocator for all this junk would be better.
                let old_versions: SmallVec<[LV; 2]> = val.iter().map(|(v, _)| *v).collect();
                let new_versions: SmallVec<[LV; 2]> = given_raw_pairs.iter().map(|(rv, _)| (
                    self.cg.agent_assignment.remote_to_local_version(*rv)
                )).collect();

                // TODO: Might also be better to just clone() the items in here instead of copying
                // the memory all over the place.
                let mut new_values: SmallVec<[Option<T>; 2]> = given_raw_pairs.into_iter()
                    .map(|(_, val)| Some(val))
                    .collect();

                let mut idx_changes: SmallVec<[(LV, bool); 2]> = smallvec![];

                // dbg!(old_versions.iter().copied().chain(new_versions.iter().copied()).collect::<Vec<_>>());
                self.cg.graph.find_dominators_full(
                    old_versions.iter().copied().chain(new_versions.iter().copied()),
                    |v, dominates| {
                        // There's 3 cases here:
                        // - Its in the old set (val)
                        // - Its in the new set (new_versions)
                        // - Or its in both.
                        if dominates && !old_versions.contains(&v) {
                            // Its in new only and we need to add it.
                            // self.add_index(v, key);
                            idx_changes.push((v, true));

                            // let val = new_versions.
                            let idx = new_versions.iter().position(|v2| *v2 == v)
                                .unwrap();
                            val.push((v, new_values[idx].take().unwrap()));
                        } else if !dominates && !new_versions.contains(&v) {
                            // Its in old only, and its been superseded. Remove it!
                            idx_changes.push((v, false));
                            let idx = val.iter().position(|(v2, _)| *v2 == v)
                                .unwrap();
                            val.swap_remove(idx);
                        }
                    }
                );

                if val.len() >= 2 {
                    val.sort_unstable_by_key(|(v, _)| *v);
                }

                for (v, is_add) in idx_changes {
                    if is_add {
                        self.index.insert(v, key);
                    } else {
                        self.index.remove(&v);
                    }
                }
            }
        }
    }

    #[allow(unused)]
    pub fn dbg_check(&self) {
        let mut expected_idx_size = 0;

        for (key, pairs) in self.values.iter() {
            if pairs.len() >= 2 {
                let version: SmallVec<[LV; 2]> = pairs.iter().map(|(v, _)| *v).collect();

                let dominators = self.cg.graph.find_dominators(&version);
                assert_eq!(version.as_slice(), dominators.as_ref());
            }

            expected_idx_size += pairs.len();

            // Each entry should show up in the index.
            for (v, _) in pairs.iter() {
                assert_eq!(self.index.get(v), Some(key));
            }
        }

        self.cg.dbg_check(false);

        assert_eq!(expected_idx_size, self.index.len());
    }

    pub(crate) fn resolve_pairs<'a>(&'a self, pairs: &'a [Pair<T>]) -> &Pair<T> {
        let len = pairs.len();

        let mut iter = pairs.iter();
        let first = iter.next().expect("Internal consistency violation - pairs list empty");

        if len > 1 {
            let av = self.cg.agent_assignment.local_to_agent_version(first.0);

            let (_, result) = iter.fold((av, first), |(av, pair1), pair2| {
                let av2 = self.cg.agent_assignment.local_to_agent_version(pair2.0);
                if self.cg.agent_assignment.tie_break_agent_versions(av, av2) == Ordering::Greater {
                    (av, pair1)
                } else {
                    (av2, pair2)
                }
            });
            result
        } else {
            first
        }
    }

    fn get_values_ref(&self, key: DocName) -> Option<impl Iterator<Item = &T>> {
        self.values.get(&key)
            .map(|pairs| pairs.iter().map(|(_, val)| val))
    }
}

type CGDelta<'a> = SmallVec<[PartialCGEntry<'a>; 4]>;
type SSDelta<'a, T> = SmallVec<[(RemoteVersion<'a>, SmallVec<[RawPair<'a, T>; 2]>); 4]>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteStateDelta<'a, T> {
    #[serde(borrow)]
    pub(crate) cg: CGDelta<'a>,
    #[serde(borrow)]
    pub ops: SSDelta<'a, T>
}

impl<T: Clone + Serialize + DeserializeOwned> StateSet<T> {
    pub fn merge_delta(&mut self, cg_delta: &CGDelta, ops: SSDelta<T>) -> DTRange {
        let updated = merge_partial_versions(&mut self.cg, cg_delta);

        for (key, pairs) in ops {
            self.merge_set(key, pairs);
        }

        updated
    }

    pub fn delta_since(&self, v: &[LV]) -> RemoteStateDelta<T> {
        let cg_delta = serialize_cg_from_version(&self.cg, v, self.cg.version.as_ref());

        // dbg!(&self.version);
        let ranges = self.cg.graph.diff(v, self.cg.version.as_ref());
        assert!(ranges.0.is_empty());
        let ranges = ranges.1;

        // dbg!(&ranges);

        let mut docs: BTreeMap<DocName, SmallVec<[Pair<T>; 2]>> = Default::default();
        // let mut ops = smallvec![];
        for r in ranges {
            for (v, key) in self.index.range(r) {
                let pair = self.values.get(key)
                    .unwrap()
                    .iter()
                    .find(|(v2, _)| *v2 == *v)
                    .unwrap();

                docs.entry(*key).or_default().push(pair.clone());
            }
        }

        RemoteStateDelta {
            cg: cg_delta,
            ops: docs
                .into_iter()
                .map(|(name, pairs)| (
                    self.cg.agent_assignment.local_to_remote_version(name),
                    pairs.into_iter().map(|(v, value)| (self.cg.agent_assignment.local_to_remote_version(v), value))
                        .collect()
                ))
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use diamond_types::causalgraph::agent_assignment::remote_ids::RemoteVersion;
    use diamond_types::Frontier;
    use smallvec::smallvec;
    use crate::stateset::StateSet;

    #[test]
    fn local_insert() {
        let mut ss = StateSet::new();
        let agent = ss.cg.get_or_create_agent_id("seph");
        ss.local_set(agent, None, 123);

        ss.dbg_check();
        // dbg!(ss);
    }

    #[test]
    fn remote_set() {
        let mut ss: StateSet<String> = StateSet::new();
        let seph = ss.cg.get_or_create_agent_id("seph");
        let mike = ss.cg.get_or_create_agent_id("mike");
        ss.dbg_check();

        ss.cg.assign_local_op_with_parents(&[], seph, 1).last();
        ss.cg.version = Frontier::from_sorted(&[0]);
        ss.merge_set(RemoteVersion("seph", 0), smallvec![(("seph", 0).into(), "hi".into())]);
        ss.dbg_check();
        assert!(ss.get_values_ref(0).unwrap().eq((&["hi"]).iter()));

        // Replacing it with the same value should do nothing.
        ss.merge_set(RemoteVersion("seph", 0), smallvec![(("seph", 0).into(), "hi".into())]);
        ss.dbg_check();
        assert!(ss.get_values_ref(0).unwrap().eq((&["hi"]).iter()));

        // Now we'll supercede it
        let a = ss.cg.assign_local_op_with_parents(&[0], seph, 1).last();
        ss.cg.version = Frontier::from_sorted(&[a]);
        ss.merge_set(RemoteVersion("seph", 0), smallvec![(("seph", 1).into(), "yo".into())]);
        ss.dbg_check();
        assert!(ss.get_values_ref(0).unwrap().eq((&["yo"]).iter()));

        // And insert something concurrent...
        let b = ss.cg.assign_local_op_with_parents(&[], mike, 1).last();
        ss.cg.version = Frontier::from_sorted(&[a, b]);
        ss.merge_set(RemoteVersion("seph", 0), smallvec![(("mike", 0).into(), "xxx".into())]);
        ss.dbg_check();
        assert!(ss.get_values_ref(0).unwrap().eq((&["yo", "xxx"]).iter()));

        // dbg!(&ss);
        // And collapse the concurrent editing state
        // println!("\n\n------");
        let c = ss.cg.assign_local_op_with_parents(&[a, b], seph, 1).last();
        ss.cg.version = Frontier::from_sorted(&[c]);
        ss.merge_set(RemoteVersion("seph", 0), smallvec![(("seph", 2).into(), "m".into())]);
        ss.dbg_check();
        // dbg!(ss.get_values_ref(0).unwrap().collect::<Vec<_>>());
        assert!(ss.get_values_ref(0).unwrap().eq((&["m"]).iter()));


        // dbg!(ss.delta_since(&[c]));
    }
}
