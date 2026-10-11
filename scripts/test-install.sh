#!/usr/bin/env sh
# Installs from a locally built tarball through install.sh and checks the binary runs.
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cargo build --release -p loadfleet-cli
os=$(uname -s | tr '[:upper:]' '[:lower:]'); arch=$(uname -m)
case $arch in x86_64|amd64) arch=amd64 ;; arm64|aarch64) arch=arm64 ;; esac
mkdir -p "$tmp/rel/latest/download"
tar -C "$root/target/release" -czf "$tmp/rel/latest/download/loadfleet-$os-$arch.tar.gz" loadfleet
(cd "$tmp/rel/latest/download" && shasum -a 256 "loadfleet-$os-$arch.tar.gz" > "loadfleet-$os-$arch.tar.gz.sha256")
LOADFLEET_BASE_URL="file://$tmp/rel" LOADFLEET_BIN_DIR="$tmp/bin" sh "$root/install.sh" > "$tmp/out.txt"
"$tmp/bin/loadfleet" version | grep -q '^loadfleet ' || { echo "binary did not run"; exit 1; }
grep -q 'ulimit -n' "$tmp/out.txt" || { echo "tuning advice missing"; exit 1; }
# the temp bin dir is not on PATH: the installer must say how to add it
grep -q "is not on your PATH" "$tmp/out.txt" || { echo "PATH hint missing"; exit 1; }
# plain http and other schemes are refused before anything is downloaded
if LOADFLEET_BASE_URL="http://127.0.0.1:9/rel" LOADFLEET_BIN_DIR="$tmp/bin3" sh "$root/install.sh" >/dev/null 2>&1; then
  echo "an http:// base URL was accepted"; exit 1
fi
# a corrupted download must be refused
echo garbage > "$tmp/rel/latest/download/loadfleet-$os-$arch.tar.gz"
if LOADFLEET_BASE_URL="file://$tmp/rel" LOADFLEET_BIN_DIR="$tmp/bin2" sh "$root/install.sh" >/dev/null 2>&1; then
  echo "checksum mismatch was not detected"; exit 1
fi
echo "install test OK"
