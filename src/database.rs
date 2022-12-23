use std::collections::BTreeMap;
use smartstring::alias::String as SmartString;
use rand::distributions::Alphanumeric;
use diamond_types::{AgentId, CRDTKind, CreateValue, Frontier, LV, ROOT_CRDT_ID};
use diamond_types::causalgraph::agent_assignment::remote_ids::RemoteVersionOwned;
use diamond_types::{Branch, OpLog, SerializedOps};
use diamond_types::list::operation::TextOperation;
use rand::Rng;
use smallvec::SmallVec;
use crate::stateset::{LVKey, StateSet};

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct InboxEntry {
    version: SmallVec<[RemoteVersionOwned; 2]>,
    kind: SmartString,
}

impl InboxEntry {

}

#[derive(Debug)]
pub struct Database {
    pub(crate) inbox: StateSet<InboxEntry>,
    pub(crate) docs: BTreeMap<LVKey, OpLog>,
    pub(crate) index_agent: AgentId,
}

impl Database {
    pub fn new() -> Self {
        let agent: SmartString = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();

        let mut ss = StateSet::new();
        let agent = ss.cg.get_or_create_agent_id(&agent);

        Self {
            inbox: ss,
            docs: Default::default(),
            index_agent: agent
        }
    }

    pub fn insert_new_item(&mut self, kind: &str, doc: OpLog) -> LVKey {
        let id = self.inbox.local_insert(self.index_agent, InboxEntry {
            version: doc.cg.remote_frontier_owned(),
            kind: kind.into(),
        });
        self.docs.insert(id, doc);
        id
    }

    pub fn create_post(&mut self) -> LVKey {
        // To avoid sync problems, we'll initialize the post entirely using a special SCHEMA user.
        let mut doc = OpLog::new();
        let agent = doc.cg.agent_assignment.get_or_create_agent_id("SCHEMA");
        let title = doc.local_map_set(agent, ROOT_CRDT_ID, "title", CreateValue::NewCRDT(CRDTKind::Text));
        doc.local_text_op(agent, title, TextOperation::new_insert(0, "Untitled"));
        let content = doc.local_map_set(agent, ROOT_CRDT_ID, "content", CreateValue::NewCRDT(CRDTKind::Text));
        doc.local_text_op(agent, content, TextOperation::new_insert(0, "yo check out this sick post"));
        // dbg!(&doc.cg.version);

        self.insert_new_item("post", doc)
    }

    pub fn doc_updated(&mut self, item: LVKey) {
        let Some(existing_value) = self.inbox.get_value(item) else { return; };

        let Some(doc) = self.docs.get(&item) else { return; };

        // TODO: Change to a borrowed frontier.
        let doc_rv = doc.cg.agent_assignment.local_to_remote_frontier_owned(doc.cg.version.as_ref());
        if existing_value.version != doc_rv {
            let mut new_value = existing_value.clone();
            new_value.version = doc_rv;
            self.inbox.local_set(self.index_agent, item, new_value);
        }
        // pair.
        // let old_val = self.inbox.resolve_pairs()
        // self.inbox.local_set(self.index_agent, Some(item))
    }

    pub fn posts(&self) -> impl Iterator<Item=LVKey> + '_ {
        self.inbox.values.iter().filter_map(|(doc_name, pairs)| {
            let val = &self.inbox.resolve_pairs(pairs.as_slice()).1;
            if val.kind == "post" {
                Some(*doc_name)
            } else { None }
        })
    }

    pub fn agent_name(&self) -> &str {
        self.inbox.cg.agent_assignment.get_agent_name(self.index_agent)
    }

    pub fn get_doc_mut(&mut self, key: LVKey) -> Option<(&mut OpLog, AgentId)> {
        self.docs.get_mut(&key)
            .map(|doc| {
                let agent_name = self.inbox.cg.agent_assignment.get_agent_name(self.index_agent);
                let agent = doc.cg.get_or_create_agent_id(agent_name);
                (doc, agent)
            })
    }

    pub fn changes_to_doc_since(&self, doc: LVKey, v: &[LV]) -> Option<SerializedOps> {
        let doc = self.docs.get(&doc)?;
        Some(doc.ops_since(v))
    }

    pub fn changes_to_post_content_since(&self, doc: LVKey, v: &[LV]) -> Option<(Vec<TextOperation>, Frontier)> {
        let doc = self.docs.get(&doc)?;

        let content = doc.text_at_path(&["content"]);
        Some((
            doc.text_changes_since(content, v)
                .into_iter()
                .filter_map(|(_, op)| op)
                .collect(),
            doc.cg.version.clone()
        ))
    }

    pub fn post_content(&self, doc: LVKey) -> Option<String> {
        let doc = self.docs.get(&doc)?;
        let content = doc.text_at_path(&["content"]);
        Some(doc.checkout_text(content).to_string())
    }

    pub fn checkout(&self, doc: LVKey) -> Option<Branch> {
        self.docs.get(&doc)
            .map(|oplog| oplog.checkout_tip())
    }

    /// returns if there were updates
    pub fn update_branch(&self, doc: LVKey, branch: &mut Branch) -> bool {
        let oplog = self.docs.get(&doc).unwrap();
        let merged_versions = branch.merge_changes_to_tip(oplog);
        !merged_versions.is_empty()
    }

    pub fn dbg_print_docs(&self) {
        for (local_name, doc) in self.docs.iter() {
            println!("doc {} -> version {:?}, data: {:?}", local_name, doc.cg.version, doc.checkout());
        }
    }
}
