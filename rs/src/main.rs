mod stateset;

use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::vec;
use bpaf::{Bpaf, Parser, short};
use diamond_types::causalgraph::summary::VersionSummary;
use tokio::{io, signal};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use serde::{Serialize, Deserialize};
use smallvec::SmallVec;
use tokio::sync::RwLock;
use crate::stateset::StateSet;


#[derive(Debug)]
struct Database {
    inbox: StateSet<usize>
}

impl Database {
    fn new() -> Self {
        Self {
            inbox: StateSet::new()
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum NetMessage {
    KnownIdx {vs: VersionSummary},
}


type DatabaseHandle = Arc<RwLock<Database>>;

async fn send_message<W: AsyncWrite + Unpin>(stream: &mut W, msg: NetMessage) -> Result<(), io::Error> {
    let mut msg = serde_json::to_vec(&msg).unwrap();
    msg.push(b'\n');
    stream.write_all(&msg).await
}

async fn run_protocol(mut socket: TcpStream, database: DatabaseHandle) -> Result<(), io::Error> {
    let (reader, mut writer) = socket.split();

    // As we connect, both peers say hello by sending each other their version summaries.
    // writer.write_all(b"hi there\n").await?;
    send_message(&mut writer, NetMessage::KnownIdx {
        vs: database.read().await.inbox.cg.summarize()
    }).await?;

    let reader = BufReader::new(reader);
    let mut line_reader = reader.lines();
    while let Some(line) = line_reader.next_line().await? {
        println!("Line {line}");

        let msg: NetMessage = serde_json::from_str(&line)?;
        match msg {
            NetMessage::KnownIdx { vs } => {
                dbg!(vs);
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let database = Arc::new(RwLock::new(Database::new()));

    // let m = NetMessage::KnownIdx { x: 123 };
    // dbg!(serde_json::to_string(&m).unwrap());

    let opts: CmdOpts = cmd_opts().run();
    dbg!(&opts);

    for port in opts.listen_ports.iter().copied() {
        let handle = database.clone();
        tokio::spawn(async move {
            let listener = TcpListener::bind(
                (Ipv4Addr::new(0,0,0,0), port)
            ).await?;

            loop {
                let (socket, addr) = listener.accept().await?;
                println!("{} connected", addr);
                let handle = handle.clone();
                tokio::spawn(async move {
                    run_protocol(socket, handle).await?;
                    println!("{} disconnected", addr);
                    Ok::<(), io::Error>(())
                });
            }

            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        });
    }

    for addr in opts.connect.iter().cloned() {
        let handle = database.clone();
        tokio::spawn(async move {
            // TODO: Add reconnection support.

            // Walk through the socket addresses trying to connect
            let mut socket = None;
            for a in addr {
                let s = TcpStream::connect(a).await?;
                socket = Some(s);
                break;
            };

            if let Some(socket) = socket {
                run_protocol(socket, handle).await
            } else {
                eprintln!("Could not connect to requested peer");
                Ok(())
            }
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