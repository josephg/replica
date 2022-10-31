use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::vec;
use bpaf::{Bpaf, Parser, short};
use tokio::{io, signal};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum NetMessage {
    KnownIdx {x: usize},
}

async fn run_protocol(mut socket: TcpStream) -> Result<(), io::Error> {
    let (reader, mut writer) = socket.split();
    writer.write_all(b"hi there\n").await?;
    let reader = BufReader::new(reader);
    let mut line_reader = reader.lines();
    while let Some(line) = line_reader.next_line().await? {
        println!("Line {line}");
        // writer.write_all(line.as_bytes()).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    // let m = NetMessage::KnownIdx { x: 123 };
    // dbg!(serde_json::to_string(&m).unwrap());

    let opts: CmdOpts = cmd_opts().run();
    dbg!(&opts);

    for port in opts.listen_ports.iter().copied() {
        tokio::spawn(async move {
            let listener = TcpListener::bind(
                (Ipv4Addr::new(0,0,0,0), port)
            ).await?;

            loop {
                let (socket, addr) = listener.accept().await?;
                println!("{} connected", addr);
                tokio::spawn(async move {
                    run_protocol(socket).await?;
                    println!("{} disconnected", addr);
                    Ok::<(), io::Error>(())
                });
            }

            #[allow(unreachable_code)]
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