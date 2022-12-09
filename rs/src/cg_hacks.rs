use diamond_types::{CausalGraph, DTRange, Frontier, HasLength, LV};
use diamond_types::causalgraph::agent_assignment::remote_ids::RemoteVersion;
use diamond_types::causalgraph::agent_span::AgentSpan;
use serde::{Deserialize, Serialize};
use smallvec::{SmallVec, smallvec};
use smartstring::alias::String as SmartString;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PartialCGEntry<'a> {
    agent: SmartString,
    seq: usize,
    len: usize,
    #[serde(borrow)]
    parents: SmallVec<[RemoteVersion<'a>; 2]>,
}

pub(crate) fn serialize_cg_from_version<'a>(cg: &'a CausalGraph, v: &[LV], cur_version: &[LV]) -> SmallVec<[PartialCGEntry<'a>; 4]> {
    let ranges = cg.graph.diff(v, cur_version);
    assert!(ranges.0.is_empty());

    let mut entries = smallvec![];
    for r in ranges.1 {
        // dbg!(r, &cg);
        for entry in cg.iter_range(r) {
            entries.push(PartialCGEntry {
                agent: cg.agent_assignment.get_agent_name(entry.span.agent).into(),
                seq: entry.span.seq_range.start,
                len: entry.len(),
                parents: entry.parents.iter().map(|p| cg.agent_assignment.local_to_remote_version(*p)).collect()
            })
        }
    }
    entries
}

pub(crate) fn merge_partial_versions(cg: &mut CausalGraph, pe: &[PartialCGEntry]) -> DTRange {
    let start = cg.len();

    for e in pe {
        let parents = e.parents
            .iter()
            .map(|rv| cg.agent_assignment.try_remote_to_local_version(*rv).unwrap())
            .collect::<Frontier>();

        let agent = cg.get_or_create_agent_id(&e.agent);
        let _v_span = cg.merge_and_assign(parents.as_ref(), AgentSpan {
            agent,
            seq_range: (e.seq .. e.seq + e.len).into()
        });
    }

    (start .. cg.len()).into()
}

pub(crate) fn advance_frontier_from_serialized(frontier: &mut Frontier, pe: &[PartialCGEntry], cg: &CausalGraph) {
    for e in pe {
        if let Ok(last) = cg.agent_assignment.try_remote_to_local_version(RemoteVersion(&e.agent, e.seq + e.len - 1)) {
            frontier.0.push(last);
        }
    }

    *frontier = cg.graph.find_dominators(frontier.as_ref());
}

#[cfg(test)]
mod tests {
    use diamond_types::CausalGraph;
    use crate::cg_hacks::serialize_cg_from_version;

    #[test]
    fn foo() {
        let mut cg = CausalGraph::new();
        cg.get_or_create_agent_id("seph");
        cg.assign_local_op_with_parents(&[], 0, 10);
        let s = serialize_cg_from_version(&cg, &[5], &[9]);
        dbg!(serde_json::to_string(&s).unwrap());
    }
}