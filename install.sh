#!/usr/bin/env sh
# loadfleet installer: downloads the static binary for this OS/arch, verifies its sha256, installs it,
# and prints the OS tuning a load agent needs.
# Usage: curl --proto '=https' --tlsv1.2 -fsSL <raw url>/install.sh | sh
set -eu
version=${LOADFLEET_VERSION:-latest}
base=${LOADFLEET_BASE_URL:-https://github.com/alicankuklaci/loadfleet/releases}
os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case $arch in x86_64|amd64) arch=amd64 ;; arm64|aarch64) arch=arm64 ;; *) echo "unsupported arch $arch" >&2; exit 1 ;; esac
case $os in linux|darwin) ;; *) echo "unsupported OS $os" >&2; exit 1 ;; esac
if [ "$version" = latest ]; then path="latest/download"; else path="download/$version"; fi
name="loadfleet-$os-$arch.tar.gz"
# downloads only over HTTPS (TLS >= 1.2), redirects included; file:// is for scripts/test-install.sh
case $base in
  https://*) proto='=https' ;;
  file://*) proto='=file' ;;
  *) echo "LOADFLEET_BASE_URL must be an https:// URL" >&2; exit 1 ;;
esac
fetch() { curl --proto "$proto" --proto-redir "$proto" --tlsv1.2 -fsSL "$1" -o "$2"; }
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fetch "$base/$path/$name" "$tmp/$name"
fetch "$base/$path/$name.sha256" "$tmp/$name.sha256"
(cd "$tmp" && if command -v sha256sum >/dev/null 2>&1; then sha256sum -c "$name.sha256"; else shasum -a 256 -c "$name.sha256"; fi) >/dev/null \
  || { echo "checksum mismatch for $name" >&2; exit 1; }
tar -C "$tmp" -xzf "$tmp/$name"
dir=${LOADFLEET_BIN_DIR:-/usr/local/bin}
if ! mkdir -p "$dir" 2>/dev/null || ! [ -w "$dir" ]; then dir="$HOME/.local/bin"; mkdir -p "$dir"; fi
install -m 0755 "$tmp/loadfleet" "$dir/loadfleet"
echo "installed $("$dir/loadfleet" version) to $dir/loadfleet"
case ":$PATH:" in
  *":$dir:"*) ;;
  *) echo "note: $dir is not on your PATH. Add it, e.g. in ~/.profile:"
     echo "  export PATH=\"$dir:\$PATH\"" ;;
esac
if [ "$os" = darwin ]; then
  # deploy/io.loadfleet.agent.plist writes its logs here
  mkdir -p "$HOME/Library/Logs/loadfleet"
fi
echo
echo "Tuning for a load agent (needed above ~10k connections):"
echo "  ulimit -n 1048576                       # per shell; services: LimitNOFILE / SoftResourceLimits"
if [ "$os" = linux ]; then
  echo "  sudo sysctl -w net.ipv4.ip_local_port_range=\"1024 65535\""
  echo "  sudo sysctl -w fs.file-max=2097152 net.core.somaxconn=65535 net.ipv4.tcp_tw_reuse=1"
  echo "  docker: docker run --ulimit nofile=1048576:1048576 --network host -e LOADFLEET_TOKEN ghcr.io/alicankuklaci/loadfleet agent ..."
else
  echo "  sudo sysctl -w kern.maxfiles=2097152 kern.maxfilesperproc=1048576"
  echo "  sudo sysctl -w net.inet.ip.portrange.first=1024 net.inet.ip.portrange.hifirst=1024"
  echo "  sudo launchctl limit maxfiles 1048576 1048576"
fi
echo "Each source IP has ~64k ports per target ip:port; plan ~25k connections per machine per target address."
echo "Start (secrets via env, not argv, so they stay out of ps):"
echo "  LOADFLEET_TOKEN=<token> loadfleet agent --server wss://<server> --name \$(hostname)"
