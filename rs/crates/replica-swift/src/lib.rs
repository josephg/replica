use std::ffi::c_void;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ptr::null;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use replica::connect;
use replica::database::Database;

// type DatabaseHandle<'a> = RwLockReadGuard<'a, Database>;

#[swift_bridge::bridge]
mod ffi {
    extern "Rust" {
        // type DatabaseHandle;
    //     #[swift_bridge::bridge(swift_repr = "struct")]
    //     struct TextOp {
    //
    //     }
    //
        fn foo() -> Vec<usize>;
    }
}

fn foo() -> Vec<usize> {
    vec![1,2,3]
}

type CCallback = extern "C" fn(*mut c_void) -> ();

#[no_mangle]
pub extern "C" fn database_new() -> *mut DatabaseConnection { Box::into_raw(Box::new(DatabaseConnection::new())) }

#[no_mangle]
pub extern "C" fn database_free(this: *mut DatabaseConnection) {
    let this = unsafe { Box::from_raw(this) };
    drop(this);
}

struct SendCPtr(*mut c_void);

unsafe impl Send for SendCPtr {}

#[no_mangle]
pub extern "C" fn database_start(this: *mut DatabaseConnection, signal_data: *mut c_void, signal_callback: CCallback) {
    let this = unsafe { &mut *this };

    let signal_data = SendCPtr(signal_data);
    this.start(move || {
        let s = &signal_data; // Needed so we move signal_data itself.
        signal_callback(s.0)
    });
}

// #[no_mangle]
// pub extern "C" fn with_db(this: *mut DatabaseConnection, signal_data: *mut c_void, cb: extern "C" fn(*mut c_void, *const DatabaseHandle) -> ()) {
//     let this = unsafe { &mut *this };
//     this.with_read_database(|db| {
//
//     });
// }


#[no_mangle]
pub extern "C" fn database_num_posts(this: *mut DatabaseConnection) -> u64 {
    let this = unsafe { &mut *this };
    this.with_read_database(|db| db.posts().count() as u64)
}

#[no_mangle]
pub extern "C" fn database_get_edits_since(this: *mut DatabaseConnection, doc_name: usize, signal_data: *mut c_void, cb: extern "C" fn(*mut c_void, data: usize) -> ()) {
    let this = unsafe { &mut *this };
    let ops = this.with_read_database(|db| {
        db.changes_to_post_content_since(doc_name, &[])
    });

    let num = ops.map(|ops| ops.0.len()).unwrap_or(0);
    cb(signal_data, num);
}

#[no_mangle]
pub extern "C" fn database_get_post_content(this: *mut DatabaseConnection, doc_name: usize, signal_data: *mut c_void, cb: extern "C" fn(*mut c_void, content: *const u8) -> ()) {
    let this = unsafe { &mut *this };
    let content = this.with_read_database(|db| {
        db.post_content(doc_name)
    });

    let ptr = content.map(|s| s.as_ptr()).unwrap_or(null());
    cb(signal_data, ptr);
}

#[no_mangle]
pub extern "C" fn database_get_posts(this: *mut DatabaseConnection, signal_data: *mut c_void, cb: extern "C" fn(*mut c_void, len: usize, bytes: *const usize) -> ()) {
    let this = unsafe { &mut *this };
    let posts: Vec<usize> = this.with_read_database(|db| {
        db.posts().collect()
    });

    cb(signal_data, posts.len(), posts.as_ptr())
    // cb(signal_data, Box::into_raw(Box::new(posts)));
}

pub struct DatabaseConnection {
    tokio_handle: Option<Handle>,
    db_handle: Arc<RwLock<Database>>,
    running: AtomicBool,
    // s: Sender<usize>,
}

impl Drop for DatabaseConnection {
    fn drop(&mut self) {
        if self.running.load(Ordering::Relaxed) {
            panic!("Runtime not stopped");
        }
        // println!("DATABASE DROPPED");
        // panic!();
    }
}


impl DatabaseConnection {
    fn new() -> Self {
        let mut database = Database::new();
        // database.create_post();

        DatabaseConnection {
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
                    // If rx.recv returns an error, its because there are no more tx (senders).
                    // At that point its impossible for more network messages to be received.
                    //
                    // This will happen when the socket connection throws an error.
                    // TODO: Do something better than panic here.
                    rx.recv().await.unwrap();
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

    // fn num_posts(&self) -> usize {
    //     self.with_read_database(|db| db.posts().count())
    // }

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