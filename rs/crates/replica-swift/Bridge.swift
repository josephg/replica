import Foundation

public func db_new() -> OpaquePointer {
    database_new()!
}

public func db_start(db: OpaquePointer, signal_data: UnsafeMutableRawPointer?, signal: CCallback) {
    database_start(db, signal_data, signal)
}
//public func db_start(db: OpaquePointer, init_data: UnsafeMutableRawPointer?, on_ready: CCallback,
//                     signal_data: UnsafeMutableRawPointer?, signal: CCallback) {
//    database_start(db, init_data, on_ready, signal_data, signal)
//}

//public func database_start(db: OpaquePointer, ready: () -> Void) {
//    func onReady() {
//        print("READY")
//        ready()
//    }
//
//    print("starting")
//    database_start(db, onReady)
//    print("xxxx")
//}

//public func db_num_posts(db: OpaquePointer) -> UInt64 {
//    database_num_posts(db)
//}

//void *signal_data, void (*cb)(void*, uintptr_t len, const uintptr_t *names));
public func db_get_posts(db: OpaquePointer, signal_data: UnsafeMutableRawPointer?, signal: (@convention(c) (UnsafeMutableRawPointer?, UInt, UnsafePointer<UInt>?) -> Void)) {
    database_get_posts(db, signal_data, signal)
}

//
//void hello_world(void);
//
//struct Database *database_new(void);
//
//void database_free(struct Database *this_);
//
//void database_start(struct Database *this_);
//
//void database_borrow(struct Database *this_);

