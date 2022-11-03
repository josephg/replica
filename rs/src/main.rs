#![allow(unused)]

extern crate core;

mod stateset;
mod cg_hacks;
mod database;

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
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, Lines};
use tokio::net::{TcpListener, TcpStream};
use serde::{Deserialize, Serialize};
use smartstring::alias::String as SmartString;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::sync::{broadcast, RwLock};
use database::Database;
use crate::cg_hacks::advance_frontier_from_serialized;
use crate::stateset::{RemoteStateDelta, StateSet};
use std::io::{stdout, Write};


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

type IndexChannel = (broadcast::Sender<()>, broadcast::Receiver<()>);

/// Protocol stores & represents the state for a connection to another peer.
struct Protocol<'a> {
    // I could store the network socket here too, but it makes it much harder with the borrowck.
    database: &'a mut DatabaseHandle,
    notify: IndexChannel,
    state: Option<ConnectionState>,

    stdout: StandardStream,
    color: Color,
}

impl<'a> Protocol<'a> {
    // fn new(mut socket: &'a TcpStream, database: &'a mut DatabaseHandle, mut notify: IndexChannel) -> Self {
    fn new(database: &'a mut DatabaseHandle, mut notify: IndexChannel, color: Color) -> Self {
        let mut stdout = StandardStream::stdout(ColorChoice::Auto);

        Self {
            database,
            notify,
            state: None,
            stdout,
            color,
        }
    }

    async fn handle_message(&mut self, msg: NetMessage<'_>, writer: &mut WriteHalf<'_>) -> Result<(), io::Error> {
        match msg {
            NetMessage::KnownIdx { vs } => {
                // dbg!(&vs);

                let db = self.database.read().await;
                self.stdout.set_color(ColorSpec::new().set_fg(Some(self.color)));
                let (mut remote_frontier, remainder) = db.inbox.cg.intersect_with_flat_summary(&vs, &[]);
                writeln!(&mut self.stdout, "remote frontier {:?}", remote_frontier);

                // dbg!(&remote_frontier, &remainder);

                if remote_frontier != db.inbox.version {
                    writeln!(&mut self.stdout, "Sending delta to {:?}", db.inbox.version);
                    let delta = db.inbox.delta_since(remote_frontier.as_ref());
                    send_message(writer, NetMessage::IdxDelta { delta: Box::new(delta) }).await?;
                    remote_frontier = db.inbox.version.clone();
                }

                if let Some(r) = remainder.as_ref() {
                    writeln!(&mut self.stdout, "Remainder {:?}", r);
                }

                self.state = Some(ConnectionState {
                    remote_frontier,
                    unknown_versions: remainder
                });
            }
            NetMessage::IdxDelta { delta } => {
                // dbg!(&delta);
                let mut db = self.database.write().await;
                self.stdout.set_color(ColorSpec::new().set_fg(Some(self.color)));

                let ops = delta.ops;
                let cg_delta = delta.cg;
                let diff = db.inbox.merge_delta(&cg_delta, ops);

                let ConnectionState {unknown_versions, remote_frontier} = self.state.as_mut().unwrap();
                writeln!(&mut self.stdout, "remote frontier is {:?}", remote_frontier);
                advance_frontier_from_serialized(&db.inbox.cg, &cg_delta, remote_frontier);
                writeln!(&mut self.stdout, "->mote frontier is {:?}", remote_frontier);
                *unknown_versions = None;

                if !diff.is_empty() {
                    // dbg!(diff);
                    db.inbox.print_values();
                    self.notify.0.send(()).unwrap();
                }
            }
        }

        Ok(())
    }

    async fn on_database_updated<'b>(&mut self, writer: &mut WriteHalf<'b>) -> Result<(), io::Error> {
        writeln!(&mut self.stdout, "Got broadcast message!");
        if let Some(state) = self.state.as_mut() {
            let db = self.database.read().await;
            self.stdout.set_color(ColorSpec::new().set_fg(Some(self.color)));
            if let Some(uv) = state.unknown_versions.as_mut() {
                // If the remote peer has some versions we don't know about, first check to see if
                // it already knows about some of the versions we've updated locally. This prevents
                // a bug where the peer oversends data.
                let (f, v) = db.inbox.cg.intersect_with_flat_summary(uv, state.remote_frontier.as_ref());
                // I could just handle this via destructuring, but it causes an intellij error.
                state.remote_frontier = f;
                state.unknown_versions = v;

                writeln!(&mut self.stdout, "Trimmed unknown versions to {:?}", state.unknown_versions);
            }

            if state.remote_frontier != db.inbox.version {
                writeln!(&mut self.stdout, "Send delta! {:?} -> {:?}", state.remote_frontier, db.inbox.version);
                let delta = db.inbox.delta_since(state.remote_frontier.as_ref());
                send_message(writer, NetMessage::IdxDelta { delta: Box::new(delta) }).await?;
            }
        }

        Ok(())
    }

    pub async fn run(&mut self, mut socket: TcpStream) -> Result<(), io::Error> {
        let (reader, mut writer) = socket.split();

        let reader = BufReader::new(reader);
        let mut line_reader = reader.lines();

        self.stdout.set_color(ColorSpec::new().set_fg(Some(self.color)));
        send_message(&mut writer, NetMessage::KnownIdx {
            vs: self.database.read().await.inbox.cg.summarize_versions_flat()
        }).await?;

        loop {
            select! {
                line = line_reader.next_line() => {
                    if let Some(line) = line? {
                        self.stdout.set_color(ColorSpec::new().set_fg(Some(self.color)));
                        writeln!(&mut self.stdout, "READ {line}");

                        let msg: NetMessage = serde_json::from_str(&line)?;
                        self.handle_message(msg, &mut writer).await?;
                    } else {
                        // End of network stream.
                        self.stdout.set_color(ColorSpec::new().set_fg(Some(self.color)));
                        writeln!(&mut self.stdout, "End of network stream");
                        break;
                    }
                }

                recv_result = self.notify.1.recv() => {
                    if let Err(e) = recv_result {
                        self.stdout.set_color(ColorSpec::new().set_fg(Some(self.color)));
                        writeln!(&mut self.stdout, "Message error {:?}", e);
                        break;
                    }

                    self.on_database_updated(&mut writer).await?;
                }
            }
        }


        Ok(())
    }
}

async fn run_protocol(mut socket: TcpStream, database: &mut DatabaseHandle, mut notify: IndexChannel, color: Color) -> Result<(), io::Error> {
    Protocol::new(database, notify, color).run(socket).await
}

#[tokio::main(flavor= "current_thread")]
async fn main() {
    let mut db = Database::new();
    db.insert_item(rand::thread_rng().next_u32() as usize);
    let database = Arc::new(RwLock::new(db));

    let opts: CmdOpts = cmd_opts().run();
    dbg!(&opts);

    let colors: Vec<Color> = vec![
        Color::Green,
        Color::Cyan,
        Color::Yellow,
        Color::Magenta,
    ];
    let mut i = 0;
    let mut next_color = move || {
        i += 1;
        println!("COLOR {i}");
        colors[i % colors.len()]
    };

    let (tx, rx1) = tokio::sync::broadcast::channel(16);
    drop(rx1);

    for port in opts.listen_ports.iter().copied() {
        let handle = database.clone();
        let tx = tx.clone();
        let color = next_color();
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
                    run_protocol(socket, &mut handle, (tx2, rx2), color).await?;
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
        let color = next_color();
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
                    run_protocol(socket, &mut handle, (tx2, rx2), color).await?;
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