mod stateset;
mod cg_hacks;
pub mod database;
pub(crate) mod protocol;

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;
use crate::database::Database;
use crate::protocol::Protocol;

pub fn connect(addr: Vec<SocketAddr>, handle: Arc<RwLock<Database>>, tx: Sender<usize>) {
    tokio::spawn(
        connect_internal(addr, handle, tx)
    );
}

pub async fn connect_internal(addr: Vec<SocketAddr>, mut handle: Arc<RwLock<Database>>, tx: Sender<usize>) -> Result<(), io::Error> {
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
            if let Err(e) = Protocol::start(socket, &mut handle, (tx2, rx2)).await {
                println!("Could not connect: {:?}", e);
            }
            println!("Disconnected! :(");
        } else {
            eprintln!("Could not connect to requested peer");
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    // #[allow(unreachable_code)]
    // Ok::<(), io::Error>(())
}

pub fn listen(port: u16, handle: Arc<RwLock<Database>>, tx: Sender<usize>) {
    tokio::spawn(async move {
        let listener = TcpListener::bind(
            (Ipv4Addr::new(0, 0, 0, 0), port)
        ).await?;

        loop {
            let (socket, addr) = listener.accept().await?;
            println!("{} connected", addr);
            let mut handle = handle.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let (tx2, rx2) = (tx.clone(), tx.subscribe());
                Protocol::start(socket, &mut handle, (tx2, rx2)).await?;
                println!("{} disconnected", addr);
                Ok::<(), io::Error>(())
            });
        }

        #[allow(unreachable_code)]
        Ok::<(), io::Error>(())
    });
}
