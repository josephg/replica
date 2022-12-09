use std::collections::BTreeMap;
use smartstring::alias::String as SmartString;
use rand::distributions::Alphanumeric;
use diamond_types::{AgentId, CRDTKind, CreateValue, Frontier, LV, ROOT_CRDT_ID};
use diamond_types::causalgraph::agent_assignment::remote_ids::RemoteVersionOwned;
use diamond_types::experiments::{ExperimentalOpLog, SerializedOps};
use diamond_types::list::operation::TextOperation;
use rand::Rng;
use smallvec::SmallVec;
use crate::stateset::{DocName, StateSet};

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
    pub(crate) docs: BTreeMap<DocName, ExperimentalOpLog>,
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

    pub fn insert_new_item(&mut self, kind: &str, doc: ExperimentalOpLog) -> DocName {
        let id = self.inbox.local_insert(self.index_agent, InboxEntry {
            version: doc.cg.remote_frontier_owned(),
            kind: kind.into(),
        });
        self.docs.insert(id, doc);
        id
    }

    pub fn create_post(&mut self) -> DocName {
        // To avoid sync problems, we'll initialize the post entirely using a special SCHEMA user.
        let mut doc = ExperimentalOpLog::new();
        let agent = doc.cg.agent_assignment.get_or_create_agent_id("SCHEMA");
        let title = doc.local_map_set(agent, ROOT_CRDT_ID, "title", CreateValue::NewCRDT(CRDTKind::Text));
        doc.local_text_op(agent, title, TextOperation::new_insert(0, "Untitled"));
        doc.local_map_set(agent, ROOT_CRDT_ID, "content", CreateValue::NewCRDT(CRDTKind::Text));

        self.insert_new_item("post", doc)
    }

    pub fn posts(&self) -> impl Iterator<Item=DocName> + '_ {
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

    pub fn get_doc_mut(&mut self, key: DocName) -> Option<(&mut ExperimentalOpLog, AgentId)> {
        self.docs.get_mut(&key)
            .map(|doc| {
                let agent_name = self.inbox.cg.agent_assignment.get_agent_name(self.index_agent);
                let agent = doc.cg.get_or_create_agent_id(agent_name);
                (doc, agent)
            })
    }

    pub fn changes_to_doc_since(&self, doc: DocName, v: &[LV]) -> Option<SerializedOps> {
        let doc = self.docs.get(&doc)?;
        Some(doc.ops_since(v))
    }

    pub fn changes_to_post_content_since(&self, doc: DocName, v: &[LV]) -> Option<(Vec<TextOperation>, Frontier)> {
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

    pub fn post_content(&self, doc: DocName) -> Option<String> {
        let doc = self.docs.get(&doc)?;
        let content = doc.text_at_path(&["content"]);
        Some(doc.checkout_text(content).to_string())
    }

}
