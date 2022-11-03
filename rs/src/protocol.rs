use diamond_types::Frontier;
use diamond_types::causalgraph::summary::VersionSummaryFlat;
use tokio::net::TcpStream;
use std::io;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use std::ops::Deref;
use tokio::net::tcp::WriteHalf;
use tokio::select;
use crate::cg_hacks::advance_frontier_from_serialized;
use crate::database::Database;
use crate::stateset::RemoteStateDelta;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum NetMessage<'a> {
    KnownIdx { vs: VersionSummaryFlat },
    IdxDelta {
        #[serde(borrow)]
        delta: Box<RemoteStateDelta<'a, usize>>
    },
}

type DatabaseHandle = Arc<RwLock<Database>>;

async fn send_message<'a, W: AsyncWrite + Unpin>(stream: &mut W, msg: NetMessage<'a>) -> Result<(), io::Error> {
    let mut msg = serde_json::to_vec(&msg).unwrap();
    msg.push(b'\n');
    print!("WRITE {}", std::str::from_utf8(&msg).unwrap());
    // stdout().write_all(&msg).unwrap();
    stream.write_all(&msg).await
}

#[derive(Debug)]
struct ConnectionState {
    remote_frontier: Frontier,
    unknown_versions: Option<VersionSummaryFlat>
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
    fn new(database: &'a mut DatabaseHandle, mut notify: IndexChannel) -> Self {

        Self {
            database,
            notify,
            state: None,
            // stdout,
            // color,
        }
    }

    async fn send_delta<D: Deref<Target=Database>>(frontier: &mut Frontier, db: D, writer: &mut WriteHalf<'_>) -> Result<(), io::Error> {
        let delta = db.inbox.delta_since(frontier.as_ref());
        send_message(writer, NetMessage::IdxDelta { delta: Box::new(delta) }).await?;
        frontier.replace(db.inbox.version.as_ref());
        Ok(())
    }

    async fn handle_message(&mut self, msg: NetMessage<'_>, writer: &mut WriteHalf<'_>) -> Result<(), io::Error> {
        match msg {
            NetMessage::KnownIdx { vs } => {
                // dbg!(&vs);

                let db = self.database.read().await;
                let (mut remote_frontier, remainder) = db.inbox.cg.intersect_with_flat_summary(&vs, &[]);
                println!("remote frontier {:?}", remote_frontier);

                // dbg!(&remote_frontier, &remainder);

                if remote_frontier != db.inbox.version {
                    println!("Sending delta to {:?}", db.inbox.version);
                    Self::send_delta(&mut remote_frontier, db, writer).await?;
                }

                if let Some(r) = remainder.as_ref() {
                    println!("Remainder {:?}", r);
                }

                self.state = Some(ConnectionState {
                    remote_frontier,
                    unknown_versions: remainder
                });
            }
            NetMessage::IdxDelta { delta } => {
                // dbg!(&delta);
                let mut db = self.database.write().await;

                let ops = delta.ops;
                let cg_delta = delta.cg;
                let diff = db.inbox.merge_delta(&cg_delta, ops);

                let ConnectionState {unknown_versions, remote_frontier} = self.state.as_mut().unwrap();
                println!("remote frontier is {:?}", remote_frontier);
                advance_frontier_from_serialized(&db.inbox.cg, &cg_delta, remote_frontier);
                println!("->mote frontier is {:?}", remote_frontier);
                *unknown_versions = None;

                if !diff.is_empty() {
                    // dbg!(diff);
                    db.inbox.print_values();
                    drop(db); // TODO: Check performance of doing this. Should make read handles cheaper?
                    self.notify.0.send(diff.end).unwrap();
                }
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

            if state.remote_frontier != db.inbox.version {
                println!("Send delta! {:?} -> {:?}", state.remote_frontier, db.inbox.version);
                Self::send_delta(&mut state.remote_frontier, db, writer).await?;
            }
        }

        Ok(())
    }

    pub async fn run(&mut self, mut socket: TcpStream) -> Result<(), io::Error> {
        let (reader, mut writer) = socket.split();

        let reader = BufReader::new(reader);
        let mut line_reader = reader.lines();

        send_message(&mut writer, NetMessage::KnownIdx {
            vs: self.database.read().await.inbox.cg.summarize_versions_flat()
        }).await?;

        loop {
            select! {
                line = line_reader.next_line() => {
                    if let Some(line) = line? {
                        println!("READ {line}");

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

    pub async fn start(mut socket: TcpStream, database: &mut DatabaseHandle, mut notify: IndexChannel) -> Result<(), io::Error> {
        Protocol::new(database, notify).run(socket).await
    }
}
