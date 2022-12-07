use std::collections::BTreeMap;
use smartstring::alias::String as SmartString;
use rand::distributions::Alphanumeric;
use diamond_types::{AgentId, LV};
use diamond_types::causalgraph::agent_assignment::remote_ids::{RemoteFrontier, RemoteVersionOwned};
use diamond_types::experiments::ExperimentalOpLog;
use rand::Rng;
use smallvec::SmallVec;
use crate::stateset::StateSet;

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct InboxEntry {
    version: SmallVec<[RemoteVersionOwned; 2]>
}

impl InboxEntry {

}

#[derive(Debug)]
pub struct Database {
    pub(crate) inbox: StateSet<InboxEntry>,
    pub(crate) docs: BTreeMap<LV, ExperimentalOpLog>,
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

    pub fn create_item(&mut self) -> LV {
        let id = self.inbox.local_insert(self.index_agent, Default::default());
        self.docs.insert(id, ExperimentalOpLog::new());
        id
    }

    pub fn agent_name(&self) -> &str {
        self.inbox.cg.agent_assignment.get_agent_name(self.index_agent)
    }

    pub fn get_doc_mut(&mut self, key: LV) -> Option<(&mut ExperimentalOpLog, AgentId)> {
        self.docs.get_mut(&key)
            .map(|doc| {
                let agent_name = self.inbox.cg.agent_assignment.get_agent_name(self.index_agent);
                let agent = doc.cg.get_or_create_agent_id(agent_name);
                (doc, agent)
            })
    }
}
