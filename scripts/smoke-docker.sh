#!/usr/bin/env sh
# Runs the image as server + agent on a private network and checks the agent registers.
# Honors $DOCKER (default "docker"), e.g. DOCKER="docker --context desktop-linux".
set -eu
img=${1:-loadfleet:dev}
dk() { ${DOCKER:-docker} "$@"; }
cleanup() { dk rm -f lf-agent lf-server >/dev/null 2>&1 || true; dk network rm lf-smoke >/dev/null 2>&1 || true; dk volume rm lf-smoke-data >/dev/null 2>&1 || true; rm -f "${jar:-}"; }
trap cleanup EXIT
dk network create lf-smoke >/dev/null
dk run -d --name lf-server --network lf-smoke -p 127.0.0.1:18080:8080 -v lf-smoke-data:/data \
  -e LOADFLEET_ADMIN_PASSWORD=smoke-password-1 "$img" server --allow 127.0.0.1 >/dev/null
sleep 2
token=$(dk logs lf-server 2>&1 | sed -n 's/^agent token: //p' | head -n 1)
[ -n "$token" ] || { echo "no agent token in server log"; dk logs lf-server; exit 1; }
dk run -d --name lf-agent --network lf-smoke --ulimit nofile=1048576:1048576 -e LOADFLEET_TOKEN="$token" "$img" \
  agent --server ws://lf-server:8080 --name docker-agent >/dev/null
sleep 3
jar=$(mktemp)
curl -fsS -c "$jar" -o /dev/null -d password=smoke-password-1 http://127.0.0.1:18080/login
if curl -fsS -b "$jar" http://127.0.0.1:18080/api/agents | grep -q '"name":"docker-agent"'; then
  echo "docker smoke OK"
else
  echo "docker smoke FAILED"; dk logs lf-agent; exit 1
fi
