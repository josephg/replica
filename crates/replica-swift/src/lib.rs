use std::ffi::c_void;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use diamond_types::Branch;
use diamond_types::list::operation::TextOperation;
use diamond_types::LV;
use tokio::runtime::Handle;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::broadcast::Sender;
use replica::connect;
use replica::database::Database;

#[swift_bridge::bridge]
mod ffi {
    // #[swift_bridge::bridge(swift_repr = "struct")]
    // struct BridgeTextOperation {
    //     /// The range of items in the document being modified by this operation.
    //     pub start: usize,
    //     pub end: usize,
    //
    //     /// Is this operation an insert or a delete?
    //     pub is_insert: bool,
    //
    //     /// What content is being inserted or deleted. Empty string for deletes.
    //     pub content: String,
    // }


    extern "Rust" {
        // type DatabaseHandle;
        //     #[swift_bridge::bridge(swift_repr = "struct")]
        //     struct TextOp {
        //
        //     }
        //
        fn foo() -> Vec<usize>;
    }

    extern "Rust" {
        type LocalBranch;

        fn get_version(&self) -> Vec<usize>;

        fn get_post_content(&self) -> String;
        fn update(&mut self, db: *mut DatabaseConnection) -> bool;
        // fn xf_operations_since(&self, frontier: &[usize]) -> Vec<BridgeTextOperation>;

        fn replace_content_wchar(&mut self, db: *mut DatabaseConnection, replace_start: usize, replace_end: usize, ins_content: String);
    }

}

pub struct LocalBranch {
    doc_name: LV,
    content: Branch
}


impl LocalBranch {
    fn get_version(&self) -> Vec<usize> {
        self.content.frontier.iter().copied().collect()
    }

    fn get_post_content(&self) -> String {
        let content_crdt = self.content.text_at_path(&["content"]);
        self.content.texts.get(&content_crdt).unwrap().to_string()
    }

    fn update(&mut self, db: *mut DatabaseConnection) -> bool {
        let db = unsafe { &mut *db };

        // with_read_database blocks, so this should be threadsafe (so long as branch is is Send).
        db.with_read_database(|db| {
            db.update_branch(self.doc_name, &mut self.content)
        })
    }

    fn replace_content_wchar(&mut self, db: *mut DatabaseConnection, replace_start_wchars: usize, replace_end_wchars: usize, ins_content: String) {
        let db = unsafe { &mut *db };
        db.with_write_database(|mut db| {
            let (oplog, agent) = db.get_doc_mut(self.doc_name).unwrap();
            let content_crdt = oplog.text_at_path(&["content"]);

            let rope = self.content.texts.get(&content_crdt).unwrap().borrow();
            let start_chars = rope.wchars_to_chars(replace_start_wchars);
            if replace_start_wchars != replace_end_wchars {
                let end_chars = rope.wchars_to_chars(replace_end_wchars);
                oplog.local_text_op(agent, content_crdt, TextOperation::new_delete(start_chars..end_chars));
            }
            drop(rope);
            if !ins_content.is_empty() {
                oplog.local_text_op(agent, content_crdt, TextOperation::new_insert(start_chars, &ins_content));
            }

            // This could be massively optimized, since we know we can FF.
            self.content.merge_changes_to_tip(&oplog);

            db.doc_updated(self.doc_name);
        });
        db.sender.send(0).unwrap();
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
// pub extern "C" fn database_update_branch(this: *mut DatabaseConnection, doc_name: usize, branch: *mut c_void) -> bool {
//     let this = unsafe { &mut *this };
//     let branch = unsafe { &mut *(branch as *mut ExperimentalBranch) };
//
//     // with_read_database blocks, so this should be threadsafe (so long as branch is is Send).
//     this.with_read_database(|db| {
//         db.update_branch(doc_name, branch)
//     })
// }

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

// pub extern "C" fn database_checkout(this: *mut DatabaseConnection, doc_name: usize, signal_data: *mut c_void, cb: extern "C" fn(*mut c_void, content: *mut Branch) -> ()) {
#[no_mangle]
pub extern "C" fn database_checkout(this: *mut DatabaseConnection, doc_name: usize, signal_data: *mut c_void, cb: extern "C" fn(*mut c_void, content: *mut c_void) -> ()) {
    let this = unsafe { &mut *this };
    let post = this.with_read_database(|db| {
        db.checkout(doc_name)
    }).unwrap();

    let ptr = Box::into_raw(Box::new(LocalBranch {
        doc_name,
        content: post
    }));
    cb(signal_data, ptr as *mut c_void);
}
// #[no_mangle]
// pub extern "C" fn database_get_post_content(this: *mut DatabaseConnection, doc_name: usize, signal_data: *mut c_void, cb: extern "C" fn(*mut c_void, content: *const u8) -> ()) {
//     let this = unsafe { &mut *this };
//     let content = this.with_read_database(|db| {
//         db.post_content(doc_name)
//     });
//
//     let ptr = content.map(|s| s.as_ptr()).unwrap_or(null());
//     cb(signal_data, ptr);
// }

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
    sender: Sender<usize>,
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
        let database = Database::new();
        // database.create_post();

        let (sender, _) = tokio::sync::broadcast::channel(16);

        DatabaseConnection {
            tokio_handle: None,
            db_handle: Arc::new(RwLock::new(database)),
            running: AtomicBool::new(false),
            sender,
        }
    }

    fn start<S: FnMut() + Send + 'static>(&mut self, mut signal: S) {
        let was_running = self.running.swap(true, Ordering::Relaxed);
        if was_running {
            panic!("Tokio runtime already started");
        }

        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        self.tokio_handle = Some(rt.handle().clone());

        let handle = self.db_handle.clone();

        let (tx, mut rx) = (self.sender.clone(), self.sender.subscribe());
        std::thread::spawn(move || {
            rt.block_on(async {

                // let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4444);
                let socket_addrs = "test.replica.tech:4444".to_socket_addrs().unwrap();
                connect(socket_addrs.collect(), handle.clone(), tx);

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
    #[allow(unused)]
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

    // fn checkout_post(&self, doc: LV) -> Option<ExperimentalBranch> {
    //     self.with_read_database(|db| {
    //         db.checkout(doc)
    //     })
    // }

    // fn get_post_content(&self, idx: usize) -> Option<String> {
    //     self.with_read_database(|db| {
    //         let mut posts = db.posts();
    //         let name = posts.nth(idx)?;
    //
    //         Some(db.post_content(name)?)
    //     })
    // }


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