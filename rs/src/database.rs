use smartstring::alias::String as SmartString;
use rand::distributions::Alphanumeric;
use diamond_types::AgentId;
use rand::Rng;
use crate::stateset::StateSet;

#[derive(Debug)]
pub struct Database {
    pub(crate) inbox: StateSet<usize>,
    agent: AgentId,
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
            agent
        }
    }

    pub fn insert_item(&mut self, value: usize) {
        self.inbox.local_insert(self.agent, value);
    }
}
