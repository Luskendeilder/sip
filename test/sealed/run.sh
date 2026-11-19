#!/usr/bin/env bash
# Sealed-network call test for the Tilbyderen livekit-sip fork.
#
# Starts the image under test next to a LiveKit server, Redis and three small
# stand-ins (a carrier, a registrar, a webhook receiver) on an INTERNAL docker
# network: nothing in it can reach the internet, so no real carrier is ever
# contacted. It then places real SIP calls through the image and checks every
# fork behaviour. See README.md.
#
#   IMAGE=tilbyderen/livekit-sip:v1.25.0-fork ./run.sh
#
# Exit code 0 = every check passed.
set -euo pipefail

H="$(cd "$(dirname "$0")" && pwd)"
IMAGE="${IMAGE:?set IMAGE to the livekit-sip image to test}"
LIVEKIT_IMAGE="${LIVEKIT_IMAGE:-livekit/livekit-server:v1.13.7}"
P="${PREFIX:-sipseal}"
NET="$P-net"
OUT="${OUT:-$H/out}"
PY=python:3.12-alpine
mkdir -p "$OUT"

cat > "$OUT/livekit.yaml" <<EOF
port: 7880
bind_addresses: ["0.0.0.0"]
rtc:
  tcp_port: 7881
  port_range_start: 50000
  port_range_end: 50100
  use_external_ip: false
redis:
  address: $P-redis:6379
keys:
  APItest: testsecrettestsecrettestsecret12
logging:
  level: info
webhook:
  api_key: APItest
  urls:
    - http://$P-hook:8080/
EOF

# The SHAPE of the production sip.yaml with every fork key; no real credentials.
cat > "$OUT/sip.yaml" <<EOF
api_key: APItest
api_secret: testsecrettestsecrettestsecret12
ws_url: ws://$P-lk:7880
redis:
  address: $P-redis:6379
sip_port: 5060
rtp_port: 30000-30100
use_external_ip: false
ignore_local_addr_in_sdp: true
sip_contact_user: u0047test
skip_inbound_subscribe_wait: true
inbound_ringback_until_subscribed: true
logging:
  level: info
  json: true
health_port: 7099
prometheus_port: 6790
event_stream:
  enabled: true
  key: test:sip:events
  max_len: 5000
  attribute_prefix: "tb."
outbound_registrations:
  - name: seal-a
    registrar_host: $P-registrar
    transport: udp
    auth_username: u0047a
    auth_password: not-a-real-password
    expires_sec: 180
  - name: seal-b
    registrar_host: $P-registrar
    auth_username: u0047b
    auth_password: not-a-real-password
EOF

names="$P-sip $P-lk $P-carrier $P-carrier0 $P-registrar $P-hook $P-redis $P-driver $P-driver2"
cleanup() {
  # shellcheck disable=SC2086
  docker rm -f $names >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
cleanup
trap cleanup EXIT
docker network create --internal "$NET" >/dev/null

run_py() { # name script [env...]
  local name="$1" script="$2"; shift 2
  docker run -d --name "$name" --network "$NET" "$@" -v "$H/$script:/x.py:ro" -v "$H/tonelib.py:/tonelib.py:ro" "$PY" python -u /x.py >/dev/null
}
docker run -d --name "$P-redis" --network "$NET" redis:7-alpine >/dev/null
run_py "$P-hook" hook.py
run_py "$P-registrar" fake_registrar.py
run_py "$P-carrier" fake_carrier.py
# A second carrier that replies with no delay at all, for the TCP race probe.
run_py "$P-carrier0" fake_carrier.py -e CARRIER_DELAY_MS=0
docker run -d --name "$P-lk" --network "$NET" -v "$OUT/livekit.yaml:/livekit.yaml:ro" "$LIVEKIT_IMAGE" --config /livekit.yaml >/dev/null
sleep 4
docker run -d --name "$P-sip" --network "$NET" -v "$OUT/sip.yaml:/sip/config.yaml:ro" "$IMAGE" >/dev/null
sleep 6

echo "### placing test calls (about a minute and a half)"
docker run --rm --name "$P-driver" --network "$NET" \
  -e LK_HOST="$P-lk" -e SIP_HOST="$P-sip" -e CARRIER_HOST="$P-carrier" \
  -v "$H/drive.py:/x.py:ro" -v "$H/tonelib.py:/tonelib.py:ro" "$PY" python -u /x.py > "$OUT/driver.log" 2>&1 || echo "driver exit code $?" >> "$OUT/driver.log"
sleep 2

echo "### dials that wait for the answer, as manual and consult legs do"
docker run --rm --name "$P-driver" --network "$NET" \
  -e LK_HOST="$P-lk" -e SIP_HOST="$P-sip" -e CARRIER_HOST="$P-carrier" \
  -v "$H/drive.py:/x.py:ro" -v "$H/tonelib.py:/tonelib.py:ro" "$PY" python -u /x.py waited > "$OUT/driver-waited.log" 2>&1 || echo "waited driver exit code $?" >> "$OUT/driver-waited.log"
sleep 1

echo "### 40 calls over TCP to a carrier that replies instantly"
docker run --rm --name "$P-driver" --network "$NET" \
  -e LK_HOST="$P-lk" -e SIP_HOST="$P-sip" -e CARRIER_HOST="$P-carrier0" \
  -v "$H/drive.py:/x.py:ro" -v "$H/tonelib.py:/tonelib.py:ro" "$PY" python -u /x.py tcpstress 40 > "$OUT/driver-stress.log" 2>&1 || echo "stress driver exit code $?" >> "$OUT/driver-stress.log"
sleep 1

docker run --rm --network "$NET" redis:7-alpine sh -c "wget -q -O - http://$P-sip:6790/metrics" > "$OUT/metrics-running.txt" 2>/dev/null || true
docker exec "$P-redis" redis-cli XRANGE test:sip:events - + | grep '^{' > "$OUT/stream.jsonl" || true

echo "### stopping the image under test while a call is live (SIGTERM, as a drain would)"
docker run -d --name "$P-driver2" --network "$NET" \
  -e LK_HOST="$P-lk" -e SIP_HOST="$P-sip" -e CARRIER_HOST="$P-carrier" \
  -v "$H/drive.py:/x.py:ro" -v "$H/tonelib.py:/tonelib.py:ro" "$PY" python -u /x.py drain >/dev/null
sleep 3
date -u +%H:%M:%S > "$OUT/sigterm-at.txt"  # UTC, like the container logs
t0=$(date +%s)
docker stop -t 60 "$P-sip" >/dev/null
echo "stop_seconds=$(( $(date +%s) - t0 )) exit_code=$(docker inspect -f '{{.State.ExitCode}}' "$P-sip")" > "$OUT/stop.txt"
date -u +%H:%M:%S > "$OUT/exited-at.txt"
sleep 8
docker logs "$P-driver2" > "$OUT/driver-drain.log" 2>&1 || true
docker exec "$P-redis" redis-cli XRANGE test:sip:events - + | grep '^{' > "$OUT/stream-after-drain.jsonl" || true

docker logs "$P-sip" > "$OUT/sip.log" 2>&1 || true
docker logs "$P-carrier" > "$OUT/carrier.log" 2>&1 || true
docker logs "$P-carrier0" > "$OUT/carrier0.log" 2>&1 || true
docker logs "$P-registrar" > "$OUT/registrar.log" 2>&1 || true
docker logs "$P-hook" > "$OUT/hook.log" 2>&1 || true
docker logs "$P-lk" > "$OUT/livekit.log" 2>&1 || true

docker run --rm -v "$H:/t:ro" -v "$OUT:/out:ro" "$PY" python /t/check.py /out
