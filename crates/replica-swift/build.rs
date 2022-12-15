extern crate cbindgen;

use std::env;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from("./bridge");

    let bridges = vec!["src/lib.rs"];
    for path in bridges.iter() {
        println!("cargo:rerun-if-changed={}", path);
    }

    let swift_bridge_gen = swift_bridge_build::parse_bridges(bridges);
    swift_bridge_gen.write_all_concatenated(out_dir, "replica-swift");

    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let mut config = cbindgen::Config::default();
    config.language = cbindgen::Language::C;
    config.pragma_once = true;

    // There's a problem here: swift-bridge-cli does a great job at making
    // I'm using swift-bridge-cli, but it only supports one bridge .h / .swift file pair.
    // We'll concatenate the extra c methods.
    let headers_file = File::options().append(true).create_new(false)
        .open("bridge/replica-swift/replica-swift.h").unwrap();

    cbindgen::generate_with_config(crate_dir, config)
        .expect("Unable to generate bindings")
        .write(headers_file);
        // .write_to_file("./temp_bridge.h");

    // // So I'm going to concatenate the output from swift-bridge in place.
    // let mut headers_file = File::options().append(true).create_new(false)
    //     .open("bridge/c_extras/bridge.h").unwrap();
    // headers_file.write_all(swift_bridge_gen.concat_c().as_bytes()).unwrap();

    let mut swift_bridge = File::options().append(true).create_new(false)
        .open("bridge/replica-swift/replica-swift.swift").unwrap();
    let c_bridge_swift = std::fs::read("./Bridge.swift").unwrap();
    swift_bridge.write_all(&c_bridge_swift).unwrap();
    // swift_file.write_all(swift_bridge_gen.concat_swift().as_bytes()).unwrap();
}