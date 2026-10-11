#!/usr/bin/env sh
# Builds release binaries into dist/: `linux` (static musl via cargo-zigbuild) or `macos` (native).
# Each archive holds the binary, LICENSE and THIRD_PARTY_NOTICES.
set -eu
mode=${1:?usage: build-dist.sh linux|macos}
out=dist
features=
mkdir -p "$out"
build() { # cargo-subcommand rust-target
  cargo "$1" --locked --release -p loadfleet-cli ${features:+--features "$features"} --target "$2"
}
pack() { # rust-target os arch
  mkdir -p "$out/$2-$3"
  cp "target/$1/release/loadfleet" "$out/$2-$3/loadfleet"
  cp LICENSE THIRD_PARTY_NOTICES "$out/$2-$3/"
  tar -C "$out/$2-$3" -czf "$out/loadfleet-$2-$3.tar.gz" loadfleet LICENSE THIRD_PARTY_NOTICES
  (cd "$out" && shasum -a 256 "loadfleet-$2-$3.tar.gz" > "loadfleet-$2-$3.tar.gz.sha256")
}
case $mode in
  linux)
    build zigbuild x86_64-unknown-linux-musl
    build zigbuild aarch64-unknown-linux-musl
    pack x86_64-unknown-linux-musl linux amd64
    pack aarch64-unknown-linux-musl linux arm64 ;;
  macos)
    build build aarch64-apple-darwin
    build build x86_64-apple-darwin
    pack aarch64-apple-darwin darwin arm64
    pack x86_64-apple-darwin darwin amd64 ;;
  *) echo "unknown mode $mode" >&2; exit 1 ;;
esac
ls -l "$out"
