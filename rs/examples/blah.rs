use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use replica::connect;
use replica::database::Database;

pub struct DatabaseHandle {
    tokio_handle: Option<Handle>,
    db_handle: Arc<RwLock<Database>>,
    running: AtomicBool,
    // s: Sender<usize>,
}

// impl Drop for DatabaseHandle {
//     fn drop(&mut self) {
//         if self.running.load(Ordering::Relaxed) {
//             panic!("Runtime not stopped");
//         }
//         // println!("DATABASE DROPPED");
//         // panic!();
//     }
// }


impl DatabaseHandle {
    fn new() -> Self {
        let mut database = Database::new();

        DatabaseHandle {
            tokio_handle: None,
            db_handle: Arc::new(RwLock::new(database)),
            running: AtomicBool::new(false),
        }
    }

    fn start<S: FnMut() + Send + 'static>(&mut self, mut signal: S) {
        let was_running = self.running.swap(true, Ordering::Relaxed);
        if was_running {
            panic!("Tokio runtime already started");
        }

        let rt = Runtime::new().unwrap();
        self.tokio_handle = Some(rt.handle().clone());

        let handle = self.db_handle.clone();

        std::thread::spawn(move || {
            rt.block_on(async {
                let (tx, mut rx) = tokio::sync::broadcast::channel(16);

                // let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4444);
                connect(vec![
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4444),
                    // SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 4444),
                ], handle.clone(), tx);

                // callback(t);
                loop {
                    let x = rx.recv().await;
                    x.unwrap();
                    // println!("recv: {x}");
                    signal()
                }
            });
        });
    }


    fn with_read_database<F: FnOnce(RwLockReadGuard<Database>) -> R, R>(&self, f: F) -> R {
        let data = self.db_handle.clone();
        self.tokio_handle.as_ref().unwrap().block_on(async move {
            let reader = data.read().await;
            f(reader)
        })
    }
    fn with_write_database<F: FnOnce(RwLockWriteGuard<Database>) -> R, R>(&mut self, f: F) -> R {
        let data = self.db_handle.clone();
        self.tokio_handle.as_ref().unwrap().block_on(async move {
            let writer = data.write().await;
            f(writer)
        })
    }

    fn num_posts(&self) -> usize {
        self.with_read_database(|db| db.posts().count())
    }

    fn get_post_content(&self, idx: usize) -> Option<String> {
        self.with_read_database(|db| {
            let mut posts = db.posts();
            let name = posts.nth(idx)?;

            Some(db.post_content(name)?)
        })
    }


    // fn borrow_data(&mut self) -> u32 {
    //     let data = self.data.clone();
    //     self.tokio_handle.as_ref().unwrap().block_on(async move {
    //         let r = data.read().await;
    //         // println!("{}", r);
    //         *r
    //     })
    //     // 100
    // }
}

fn main() {
    let mut db = DatabaseHandle::new();
    db.start(|| {
        println!("signal!");
    });
    // db.stop();
    std::thread::sleep(Duration::from_secs(1000));
}
