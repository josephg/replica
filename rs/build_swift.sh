#!/bin/bash

set -e

THISDIR=$(dirname $0)
cd $THISDIR

#FLAGS=""
#MODE="debug"
FLAGS="--release"
MODE="release"

export SWIFT_BRIDGE_OUT_DIR="$(pwd)/crates/replica-swift/bridge"
export RUSTFLAGS=""
# Build the project for the desired platforms:
cargo build $FLAGS --target x86_64-apple-darwin -p replica-swift
cargo build $FLAGS --target aarch64-apple-darwin -p replica-swift
mkdir -p ./target/universal-macos/"$MODE"

lipo \
    ./target/aarch64-apple-darwin/"$MODE"/libreplica_swift.a \
    ./target/x86_64-apple-darwin/"$MODE"/libreplica_swift.a \
    -create -output ./target/universal-macos/"$MODE"/libreplica_swift.a

#cargo build $FLAGS --target aarch64-apple-ios -p replica-swift
#cargo build $FLAGS --target aarch64-apple-ios-sim -p replica-swift
#mkdir -p ./target/universal-ios/"$MODE"

#lipo \
#    ./target/aarch64-apple-ios-sim/"$MODE"/libreplica_swift.a \
#    -create -output ./target/universal-ios/"$MODE"/libreplica_swift.a
#    ./target/aarch64-apple-ios/"$MODE"/libreplica_swift.a \

swift-bridge-cli create-package \
  --bridges-dir "$SWIFT_BRIDGE_OUT_DIR" \
  --out-dir target/replica_swift \
  --macos ./target/universal-macos/"$MODE"/libreplica_swift.a \
  --name Replica
#  --ios ./target/aarch64-apple-ios/"$MODE"/libreplica_swift.a \
#  --simulator ./target/aarch64-apple-ios-sim/"$MODE"/libreplica_swift.a \


#--simulator target/universal-ios/"$MODE"/libreplica_swift.a \
