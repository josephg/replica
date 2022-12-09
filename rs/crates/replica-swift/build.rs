extern crate cbindgen;

use std::env;

fn main() {
    // let out_dir = PathBuf::from("./generated");
    //
    // let bridges = vec!["src/lib.rs"];
    // for path in bridges.iter() {
    //     println!("cargo:rerun-if-changed={}", path);
    // }
    //
    // swift_bridge_build::parse_bridges(bridges)
    //     .write_all_concatenated(out_dir, env!("CARGO_PKG_NAME"));

    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let mut config = cbindgen::Config::default();
    config.language = cbindgen::Language::C;
    config.pragma_once = true;

    cbindgen::generate_with_config(crate_dir, config)
        .expect("Unable to generate bindings")
        .write_to_file("bridge/x/bridge.h");
}