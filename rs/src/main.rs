#![allow(unused)]

extern crate core;

mod stateset;
mod cg_hacks;

use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use std::vec;
use bpaf::{Bpaf, Parser, short};
use diamond_types::causalgraph::summary::{VersionSummary, VersionSummaryFlat};
use diamond_types::{AgentId, Frontier};
use rand::distributions::Alphanumeric;
use rand::{Rng, RngCore};
use tokio::{io, select, signal};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use serde::{Serialize, Deserialize};
use smartstring::alias::String as SmartString;
use tokio::sync::{broadcast, RwLock};
use crate::cg_hacks::advance_frontier_from_serialized;
use crate::stateset::{RemoteStateDelta, StateSet};


#[derive(Debug)]
struct Database {
    inbox: StateSet<usize>,
    agent: AgentId,
}

impl Database {
    fn new() -> Self {
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

    fn insert_item(&mut self, value: usize) {
        self.inbox.local_insert(self.agent, value);
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum NetMessage<'a> {
    KnownIdx { vs: VersionSummaryFlat },
    IdxDelta {
        #[serde(borrow)]
        delta: RemoteStateDelta<'a, usize>
    },
}


type DatabaseHandle = Arc<RwLock<Database>>;

async fn send_message<'a, W: AsyncWrite + Unpin>(stream: &mut W, msg: NetMessage<'a>) -> Result<(), io::Error> {
    let mut msg = serde_json::to_vec(&msg).unwrap();
    msg.push(b'\n');
    stream.write_all(&msg).await
}

#[derive(Debug)]
enum ProtocolState {
    Waiting,
    Established {
        remote_frontier: Frontier,
        unknown_versions: Option<VersionSummaryFlat>
    }
}

impl ProtocolState {
    fn get_established_mut(&mut self) -> (&mut Frontier, &mut Option<VersionSummaryFlat>) {
        match self {
            ProtocolState::Established { remote_frontier, unknown_versions } => {
                (remote_frontier, unknown_versions)
            }
            _ => panic!("Unexpected state")
        }
    }
}

type IndexChannel = (broadcast::Sender<()>, broadcast::Receiver<()>);

async fn run_protocol(mut socket: TcpStream, database: &mut DatabaseHandle, mut notify: IndexChannel) -> Result<(), io::Error> {
    let (reader, mut writer) = socket.split();

    // As we connect, both peers say hello by sending each other their version summaries.
    // writer.write_all(b"hi there\n").await?;
    send_message(&mut writer, NetMessage::KnownIdx {
        vs: database.read().await.inbox.cg.summarize_versions_flat()
    }).await?;

    let mut state = ProtocolState::Waiting;

    let reader = BufReader::new(reader);
    let mut line_reader = reader.lines();

    loop {
        select! {
            line = line_reader.next_line() => {
                if let Some(line) = line? {
                    println!("Line {line}");

                    let msg: NetMessage = serde_json::from_str(&line)?;
                    match msg {
                        NetMessage::KnownIdx { vs } => {
                            // dbg!(&vs);

                            let db = database.read().await;
                            let (remote_frontier, remainder) = db.inbox.cg.intersect_with_flat_summary(&vs, &[]);
                            // dbg!(&remote_frontier, &remainder);

                            if remote_frontier != db.inbox.version {
                                println!("Sending delta...");
                                let delta = db.inbox.delta_since(remote_frontier.as_ref());
                                send_message(&mut writer, NetMessage::IdxDelta { delta }).await?;
                            }

                            state = ProtocolState::Established {
                                remote_frontier,
                                unknown_versions: remainder
                            };
                        }
                        NetMessage::IdxDelta { delta } => {
                            // dbg!(&delta);
                            let mut db = database.write().await;

                            let (remote_frontier, unknown_versions) = state.get_established_mut();
                            advance_frontier_from_serialized(&db.inbox.cg, &delta.cg, remote_frontier);
                            *unknown_versions = None;

                            println!("remote frontier is {:?}", remote_frontier);

                            let diff = db.inbox.merge_delta(delta);

                            if !diff.is_empty() {
                                // dbg!(diff);
                                db.inbox.print_values();
                                notify.0.send(()).unwrap();
                            }

                            // db.inbox.cg.

                            // db.inbox.ge
                        }
                    }
                } else {
                    // End of network stream.
                    println!("End of network stream");
                    break;
                }
            }

            recv_result = notify.1.recv() => {
                if let Err(e) = recv_result {
                    eprintln!("Message error {:?}", e);
                    break;
                }

                println!("Got broadcast message!");
                if let ProtocolState::Established { remote_frontier, unknown_versions } = &mut state {
                    let db = database.read().await;
                    if let Some(uv) = unknown_versions.as_mut() {
                        (*remote_frontier, *unknown_versions) = db.inbox.cg.intersect_with_flat_summary(uv, remote_frontier.as_ref());
                        println!("Trimmed unknown versions to {:?}", unknown_versions);
                        // dbg!((&remote_frontier, &unknown_versions));
                    }

                    if remote_frontier != &db.inbox.version {
                        println!("Send delta!");
                        let delta = db.inbox.delta_since(remote_frontier.as_ref());
                        send_message(&mut writer, NetMessage::IdxDelta { delta }).await?;
                    }
                }
            }
        }
    }


    Ok(())
}

#[tokio::main]
async fn main() {
    let mut db = Database::new();
    db.insert_item(rand::thread_rng().next_u32() as usize);
    let database = Arc::new(RwLock::new(db));

    // let m = NetMessage::KnownIdx { x: 123 };
    // dbg!(serde_json::to_string(&m).unwrap());

    let opts: CmdOpts = cmd_opts().run();
    dbg!(&opts);

    let (tx, rx1) = tokio::sync::broadcast::channel(16);
    drop(rx1);

    for port in opts.listen_ports.iter().copied() {
        let handle = database.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let listener = TcpListener::bind(
                (Ipv4Addr::new(0,0,0,0), port)
            ).await?;

            loop {
                let (socket, addr) = listener.accept().await?;
                println!("{} connected", addr);
                let mut handle = handle.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    let (tx2, rx2) = (tx.clone(), tx.subscribe());
                    run_protocol(socket, &mut handle, (tx2, rx2)).await?;
                    println!("{} disconnected", addr);
                    Ok::<(), io::Error>(())
                });
            }

            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        });
    }

    for addr in opts.connect.iter().cloned() {
        let mut handle = database.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            // let handle = database.clone();
            // TODO: Add reconnection support.
            let addr: Vec<_> = addr.collect();

            loop {
                println!("Trying to connect to {:?} ...", addr);
                // Walk through the socket addresses trying to connect
                let mut socket = None;
                for a in &addr {
                    dbg!(a);
                    let s = TcpStream::connect(a).await;
                    match s {
                        Ok(s) => {
                            socket = Some(s);
                            break;
                        }
                        Err(err) => {
                            eprintln!("Could not connect to {}: {}", a, err);
                        }
                    }

                };
                println!("ok so far...");

                if let Some(socket) = socket {
                    let (tx2, rx2) = (tx.clone(), tx.subscribe());
                    run_protocol(socket, &mut handle, (tx2, rx2)).await?;
                    println!("Disconnected! :(");
                } else {
                    eprintln!("Could not connect to requested peer");
                }

                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            Ok::<(), io::Error>(())
        });
    }

    if opts.listen_ports.is_empty() && opts.connect.is_empty() {
        eprintln!("Nothing to do!");
        return;
    }

    if let Err(err) = signal::ctrl_c().await {
        eprintln!("Unable to listen to shutdown signal {}", err);
    }
}

fn parse_connect() -> impl Parser<Vec<vec::IntoIter<SocketAddr>>> {
    short('c')
        .long("connect")
        .argument("CONNECT")
        .map(|s: String| s.to_socket_addrs().unwrap())
        .many()
}

#[derive(Debug, Clone, Bpaf)]
#[bpaf(options, version)]
struct CmdOpts {
    #[bpaf(short, long)]
    listen_ports: Vec<u16>,

    #[bpaf(external(parse_connect))]
    connect: Vec<vec::IntoIter<SocketAddr>>,
}