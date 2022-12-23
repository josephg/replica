#![allow(unused)]

use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use std::vec;
use bpaf::{Bpaf, Parser, short};
use diamond_types::causalgraph::summary::{VersionSummary, VersionSummaryFlat};
use diamond_types::{AgentId, CreateValue, Frontier, Primitive, ROOT_CRDT_ID};
use rand::distributions::Alphanumeric;
use rand::{Rng, RngCore};
use tokio::{io, select, signal};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, Lines};
use tokio::net::{TcpListener, TcpStream};
use serde::{Deserialize, Serialize};
use smartstring::alias::String as SmartString;
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::sync::{broadcast, RwLock};
use std::io::{stdout, Write};
use std::ops::Deref;
use replica::{connect, listen};
use replica::database::Database;


// #[tokio::main(flavor= "current_thread")]
#[tokio::main]
async fn main() {
    let mut db = Database::new();

    db.create_post();
    db.dbg_print_docs();

    // let name = db.create_item();
    // let (doc, agent) = db.get_doc_mut(name).unwrap();
    // doc.local_map_set(agent, ROOT_CRDT_ID, "yo", CreateValue::Primitive(Primitive::I64(rand::thread_rng().next_u32() as i64)));

    let database = Arc::new(RwLock::new(db));

    let opts: CmdOpts = cmd_opts().run();
    dbg!(&opts);

    // let colors: Vec<Color> = vec![
    //     Color::Green,
    //     Color::Cyan,
    //     Color::Yellow,
    //     Color::Magenta,
    // ];
    // let mut i = 0;
    // let mut next_color = move || {
    //     i += 1;
    //     println!("COLOR {i}");
    //     colors[i % colors.len()]
    // };

    let (tx, rx1) = tokio::sync::broadcast::channel(16);
    drop(rx1);

    for port in opts.listen_ports.iter().copied() {
        listen(port, database.clone(), tx.clone());
    }

    for addr in opts.connect.iter().cloned() {
        connect(addr.collect(), database.clone(), tx.clone());
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