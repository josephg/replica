use std::collections::{BTreeMap, BTreeSet};
use diamond_types::Frontier;
use diamond_types::causalgraph::summary::{VersionSummary, VersionSummaryFlat};
use tokio::net::TcpStream;
use std::io;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use std::ops::{Deref, DerefMut};
use std::string::ParseError;
use diamond_types::causalgraph::agent_assignment::remote_ids::RemoteVersion;
use diamond_types::experiments::SerializedOps;
use tokio::net::tcp::WriteHalf;
use tokio::select;
use crate::cg_hacks::advance_frontier_from_serialized;
use crate::database::{Database, InboxEntry};
use crate::stateset::{LVKey, RemoteStateDelta};
use serde::{Serialize, Deserialize};
use smallvec::{SmallVec, smallvec};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum NetMessage<'a> {
    KnownIdx { vs: VersionSummaryFlat },
    IdxDelta {
        #[serde(borrow)]
        idx_delta: Box<RemoteStateDelta<'a, InboxEntry>>,
        #[serde(borrow)]
        sub_deltas: SmallVec<[(RemoteVersion<'a>, SerializedOps<'a>); 2]>,
    },
    SubscribeDocs {
        // Not sure if we should fetch, or fetch and subscribe or what here.
        #[serde(borrow)]
        docs: SmallVec<[(RemoteVersion<'a>, VersionSummary); 2]>,
    },
    DocsDelta {
        #[serde(borrow)]
        deltas: SmallVec<[(RemoteVersion<'a>, SerializedOps<'a>); 2]>,
    }
}

type DatabaseHandle = Arc<RwLock<Database>>;

async fn send_message<'a, W: AsyncWrite + Unpin>(stream: &mut W, msg: NetMessage<'a>) -> Result<(), io::Error> {
    let mut msg = serde_json::to_vec(&msg).unwrap();
    msg.push(b'\n');
    // print!("WRITE {}", std::str::from_utf8(&msg).unwrap());
    stream.write_all(&msg).await
}

#[derive(Debug)]
struct ConnectionState {
    remote_frontier: Frontier,
    unknown_versions: Option<VersionSummaryFlat>,

    my_subscriptions: BTreeSet<LVKey>,
    remote_subscriptions: BTreeMap<LVKey, Frontier>, // Storing the last known version on the remote peer.
}

type IndexChannel = (broadcast::Sender<usize>, broadcast::Receiver<usize>);

/// Protocol stores & represents the state for a connection to another peer.
pub struct Protocol<'a> {
    // I could store the network socket here too, but it makes it much harder with the borrowck.
    database: &'a mut DatabaseHandle,
    notify: IndexChannel,
    state: Option<ConnectionState>,

    // stdout: StandardStream,
    // color: Color,
}

impl<'a> Protocol<'a> {
    // fn new(mut socket: &'a TcpStream, database: &'a mut DatabaseHandle, mut notify: IndexChannel) -> Self {
    fn new(database: &'a mut DatabaseHandle, notify: IndexChannel) -> Self {
        Self {
            database,
            notify,
            state: None,
            // stdout,
            // color,
        }
    }

    async fn send_delta<D: Deref<Target=Database>>(since_frontier: &mut Frontier, db: D, subs: Option<&mut BTreeMap<LVKey, Frontier>>, writer: &mut WriteHalf<'_>) -> Result<(), io::Error> {
        let idx_delta = db.inbox.delta_since(since_frontier.as_ref());

        let mut sub_deltas = smallvec![];
        if let Some(subs) = subs {
            for key in db.inbox.modified_keys_since_frontier(since_frontier.as_ref()) {
                if let Some(known_frontier) = subs.get_mut(&key) {
                    println!("Getting doc {} ops since {:?}", key, known_frontier.as_ref());
                    // We might not have any new operations for the doc yet, or even know about it.
                    let Some(doc) = db.docs.get(&key) else { continue; };

                    if known_frontier.as_ref() != doc.cg.version.as_ref() {
                        let remote_name = db.inbox.cg.agent_assignment.local_to_remote_version(key);
                        let ops = doc.ops_since(known_frontier.as_ref());
                        sub_deltas.push((remote_name, ops));
                        subs.insert(key, doc.cg.version.clone());
                    }
                }
            }
        }

        send_message(writer, NetMessage::IdxDelta { idx_delta: Box::new(idx_delta), sub_deltas }).await?;
        since_frontier.replace(db.inbox.cg.version.as_ref());
        Ok(())
    }

    fn apply_doc_updates<'b, D: DerefMut<Target=Database>, I: Iterator<Item = (RemoteVersion<'b>, SerializedOps<'b>)>>(db: &mut D, deltas_iter: I) -> Result<(), ParseError> {
        for (remote_name, changes) in deltas_iter {
            let local_name = db.inbox.cg.agent_assignment.remote_to_local_version(remote_name);

            let doc = db.docs.entry(local_name).or_default();
            doc.merge_ops(changes).unwrap(); // TODO: Pass error correctly.

            // println!("doc {} -> version {:?}, data: {:?}", local_name, doc.cg.version, doc.checkout());
            println!("updated doc {} ({:?}) to version {:?}", local_name, remote_name, doc.cg.version.as_ref());
        }

        for (local_name, doc) in db.docs.iter() {
            println!("doc {} -> version {:?}, data: {:?}", local_name, doc.cg.version, doc.checkout());
        }

        Ok(())
    }

    // fn get_doc_updates_since<D: Deref<Target=Database>>(db: D, v: &[LV]) -> SmallVec<[(RemoteVersion<'a>, SerializedOps<'a>); 2]> {
    //
    // }

    async fn handle_message(&mut self, msg: NetMessage<'_>, writer: &mut WriteHalf<'_>) -> Result<(), io::Error> {
        match msg {
            NetMessage::KnownIdx { vs } => {
                // dbg!(&vs);

                let db = self.database.read().await;
                let (mut remote_frontier, remainder) = db.inbox.cg.intersect_with_flat_summary(&vs, &[]);
                println!("remote frontier {:?}", remote_frontier);

                // dbg!(&remote_frontier, &remainder);

                if remote_frontier != db.inbox.cg.version {
                    println!("Sending delta to {:?}", db.inbox.cg.version);
                    Self::send_delta(&mut remote_frontier, db, None, writer).await?;
                }

                if let Some(r) = remainder.as_ref() {
                    println!("Remainder {:?}", r);
                }

                self.state = Some(ConnectionState {
                    remote_frontier,
                    unknown_versions: remainder,
                    my_subscriptions: Default::default(),
                    remote_subscriptions: Default::default(),
                });
            }
            NetMessage::IdxDelta { idx_delta: delta, sub_deltas } => {
                // dbg!(&delta);
                let mut db = self.database.write().await;

                let ops = delta.ops;
                let cg_delta = delta.cg;
                let diff = db.inbox.merge_delta(&cg_delta, ops);

                let state = self.state.as_mut().unwrap();

                println!("remote frontier is {:?}", state.remote_frontier);
                advance_frontier_from_serialized(&mut state.remote_frontier, &cg_delta, &db.inbox.cg);
                println!("->mote frontier is {:?}", state.remote_frontier);
                state.unknown_versions = None;

                if !diff.is_empty() {
                    let mut sub = smallvec![];
                    for key in db.inbox.modified_keys_since_v(diff.start) {
                        if !state.my_subscriptions.contains(&key) {
                            // Usually just one key.
                            let remote_name = db.inbox.cg.agent_assignment.local_to_remote_version(key);
                            // I'm going to take for granted that the version has changed, and we care about it.
                            let local_vs = db.docs.get(&key)
                                .map(|doc| {
                                    doc.cg.agent_assignment.summarize_versions()
                                })
                                .unwrap_or_default();

                            println!("Subscribing to {} ({:?})", key, remote_name);
                            sub.push((remote_name, local_vs));

                            // We should probably only do this after the sub message has been sent below?
                            // Though if an error happens we'll bail anyway, so its fine for now ...
                            state.my_subscriptions.insert(key);
                        }
                    }

                    if !sub.is_empty() {
                        send_message(writer, NetMessage::SubscribeDocs { docs: sub }).await?;
                    } else {
                        // I don't know why I need to do this, but its needed to make the borrowck happy.
                        drop(sub);
                    }

                    Self::apply_doc_updates(&mut db, sub_deltas.into_iter()).unwrap();

                    // dbg!(diff);
                    db.inbox.print_values();
                    drop(db); // TODO: Check performance of doing this. Should make read handles cheaper?
                    self.notify.0.send(diff.end).unwrap();
                } else {
                    assert!(sub_deltas.is_empty());
                }
            }

            NetMessage::SubscribeDocs { docs } => {
                let db = self.database.read().await;
                let state = self.state.as_mut().unwrap();

                let mut deltas = smallvec![];
                for (remote_name, vs) in docs {
                    // We should always have this document at this point.
                    // TODO: if we don't, error rather than panic.
                    let local_name = db.inbox.cg.agent_assignment.remote_to_local_version(remote_name);

                    println!("Remote peer subscribed to {} ({:?})", local_name, remote_name);

                    // I'll ignore the known version summary for now. We could store that to avoid
                    // re-sending operations later.
                    let since_version = if let Some(oplog) = db.docs.get(&local_name) {
                        let since_version = oplog.cg.intersect_with_summary(&vs, &[]).0;

                        // I'll still include an empty set of changes even if the remote peer is up to date,
                        // so they know the subscription is ready.
                        let ops = oplog.ops_since(since_version.as_ref());
                        deltas.push((remote_name, ops));
                        since_version
                    } else { Frontier::root() };

                    state.remote_subscriptions.insert(local_name, since_version);
                }

                send_message(writer, NetMessage::DocsDelta { deltas }).await?;
            }

            NetMessage::DocsDelta { deltas } => {
                let mut db = self.database.write().await;
                Self::apply_doc_updates(&mut db, deltas.into_iter()).unwrap();
            }
        }

        Ok(())
    }

    async fn on_database_updated<'b>(&mut self, writer: &mut WriteHalf<'b>, db_len: usize) -> Result<(), io::Error> {
        if let Some(state) = self.state.as_mut() {
            if db_len > 0 && state.remote_frontier.0.last() == Some(&(db_len - 1)) {
                // This is a bit ghastly. I want to filter out changes that the remote peer already
                // has before we acquire a (maybe expensive?) database read handle.
                // println!("Skipping update because peer is already up to date!");
                return Ok(());
            }

            println!("Got broadcast message!");
            let db = self.database.read().await;
            if let Some(uv) = state.unknown_versions.as_mut() {
                // If the remote peer has some versions we don't know about, first check to see if
                // it already knows about some of the versions we've updated locally. This prevents
                // a bug where the peer oversends data.
                let (f, v) = db.inbox.cg.intersect_with_flat_summary(uv, state.remote_frontier.as_ref());
                // I could just handle this via destructuring, but it causes an intellij error.
                state.remote_frontier = f;
                state.unknown_versions = v;

                println!("Trimmed unknown versions to {:?}", state.unknown_versions);
            }

            if state.remote_frontier != db.inbox.cg.version {
                // TODO: Optimize this by calculating & serializing the delta once instead of
                // once per peer.
                let state = self.state.as_mut().unwrap();
                println!("Send delta! {:?} -> {:?}", state.remote_frontier, db.inbox.cg.version);
                Self::send_delta(&mut state.remote_frontier, db, Some(&mut state.remote_subscriptions), writer).await?;
            }
        }

        Ok(())
    }

    pub async fn run(&mut self, mut socket: TcpStream) -> Result<(), io::Error> {
        let (reader, mut writer) = socket.split();

        let reader = BufReader::new(reader);
        let mut line_reader = reader.lines();

        send_message(&mut writer, NetMessage::KnownIdx {
            vs: self.database.read().await.inbox.cg.agent_assignment.summarize_versions_flat()
        }).await?;

        loop {
            select! {
                line = line_reader.next_line() => {
                    if let Some(line) = line? {
                        // println!("READ {line}");

                        let msg: NetMessage = serde_json::from_str(&line)?;
                        self.handle_message(msg, &mut writer).await?;
                    } else {
                        // End of network stream.
                        println!("End of network stream");
                        break;
                    }
                }

                recv_result = self.notify.1.recv() => {
                    match recv_result {
                        Err(e) => {
                            // This can happen if the peer isn't keeping up.
                            println!("Message error {:?}", e);
                            break;
                        }
                        Ok(db_len) => {
                            self.on_database_updated(&mut writer, db_len).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn start(socket: TcpStream, database: &mut DatabaseHandle, notify: IndexChannel) -> Result<(), io::Error> {
        Protocol::new(database, notify).run(socket).await
    }
}
