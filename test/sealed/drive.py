"""Drive test calls through the SIP service in the sealed test network.

Creates trunks and a dispatch rule on the LiveKit server, then places:
  - three outbound calls over UDP (busy, ring-out, answered),
  - a warm-transfer room (customer leg, a consult leg told not to hear the
    customer, and a control leg),
  - the same three outbound calls over TCP plus three at once,
  - one inbound call, acting as the carrier itself: it listens for the ringing
    tone, then brings a second leg into the caller's room and listens for it.
In `waited` mode it places dials that wait for the answer, the way the
application places manual and consult legs.
It prints what the caller side and the room saw. run.sh collects the rest.
"""
import base64
import hashlib
import hmac
import json
import os
import random
import socket
import sys
import time
import urllib.request

from tonelib import ToneSession, media_address

LK = "http://%s:7880" % os.environ.get("LK_HOST", "sipseal-lk")
SIP_HOST = os.environ.get("SIP_HOST", "sipseal-sip")
CARRIER = os.environ.get("CARRIER_HOST", "sipseal-carrier") + ":5060"
KEY, SECRET = "APItest", "testsecrettestsecrettestsecret12"
MY_IP = socket.gethostbyname(socket.gethostname())


def b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=")


def token(room=""):
    now = int(time.time())
    header = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    video = {"roomCreate": True, "roomList": True}
    if room:
        # room admin calls need the exact room name in the grant
        video.update({"roomAdmin": True, "room": room})
    payload = b64(json.dumps({
        "iss": KEY, "sub": "driver", "nbf": now - 10, "exp": now + 600,
        "video": video,
        "sip": {"admin": True, "call": True},
    }).encode())
    sig = b64(hmac.new(SECRET.encode(), header + b"." + payload, hashlib.sha256).digest())
    return (header + b"." + payload + b"." + sig).decode()


def twirp(service, method, body, room="", timeout=20):
    req = urllib.request.Request(
        "%s/twirp/livekit.%s/%s" % (LK, service, method), data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + token(room)})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as err:
        text = err.read().decode()
        try:
            parsed = json.loads(text)
        except ValueError:
            parsed = {}
        return {"_error": err.code, "_body": text[:300], "_twirp": parsed}


def say(msg):
    print(msg, flush=True)


def now_ms():
    return int(time.time() * 1000)


def drain_scenario():
    """One live call, then a new dial while the service is draining. run.sh sends the stop signal."""
    listed = twirp("SIP", "ListSIPOutboundTrunk", {})
    items = listed.get("items", [])
    trunk = next((t for t in items if t.get("name") == "carrier"), None)
    if not trunk:
        say("drain: no trunk found: %s" % listed)
        sys.exit(1)
    tid = trunk.get("sip_trunk_id") or trunk.get("sipTrunkId")
    out = twirp("SIP", "CreateSIPParticipant", {
        "sip_trunk_id": tid, "sip_call_to": "+4790000208", "room_name": "predial-drainlive",
        "participant_identity": "sip-drainlive", "participant_name": "customer",
        "participant_attributes": {"tb.callId": "drainlive", "tb.orgId": "org1"}, "hide_phone_number": True})
    say("drain: live call placed -> %s" % (out.get("sip_call_id") or out.get("sipCallId") or out))
    time.sleep(5.5)  # run.sh sends SIGTERM about three seconds in
    t0 = time.time()
    out = twirp("SIP", "CreateSIPParticipant", {
        "sip_trunk_id": tid, "sip_call_to": "+4790001486", "room_name": "predial-draindial",
        "participant_identity": "sip-draindial", "participant_name": "customer",
        "participant_attributes": {"tb.callId": "draindial", "tb.orgId": "org1"}, "hide_phone_number": True})
    refused = "_error" in out
    say("drain: new dial while draining -> %s after %.1f s: %s" % (
        "REFUSED" if refused else "ACCEPTED", time.time() - t0, json.dumps(out)[:200]))
    time.sleep(6)
    say("drain done")


def waited_scenario():
    """Dials that wait for the answer (wait_until_answered), with the options the
    application sends. The application reads the carrier's code from the error of
    the request itself: the server SDK turns an error whose metadata has
    sip_status_code into a SipCallError."""
    trunk = twirp("SIP", "CreateSIPOutboundTrunk", {"trunk": {
        "name": "waited-tcp", "address": CARRIER, "numbers": ["+4700000004"],
        "transport": "SIP_TRANSPORT_TCP", "auth_username": "trunkuser", "auth_password": "trunkpass"}})
    tid = trunk.get("sip_trunk_id") or trunk.get("sipTrunkId")
    if not tid:
        say("waited: no trunk: %s" % trunk)
        sys.exit(1)
    cases = (("busy", "+4791000486", "30s", False),
             ("unavailable", "+4791000480", "30s", False),
             ("carrierdown", "+4791000503", "30s", False),
             ("declined", "+4791000603", "30s", False),
             ("ringout", "+4791000000", "3s", False),
             ("answered", "+4791000200", "30s", False),
             ("consult", "+4791001200", "30s", True))
    for name, number, ring, dialtone in cases:
        body = {
            "sip_trunk_id": tid, "sip_call_to": number, "sip_number": "+4700000004",
            "room_name": "manual-" + name, "participant_identity": "sip-waited-" + name,
            "participant_name": "consult" if dialtone else "customer",
            "participant_metadata": json.dumps({"callId": "waited-" + name}),
            "participant_attributes": {"tb.callId": "waited-" + name, "tb.orgId": "org1"},
            "hide_phone_number": True, "include_headers": "SIP_X_HEADERS",
            "ringing_timeout": ring, "max_call_duration": "7200s", "wait_until_answered": True}
        if dialtone:
            body["play_dialtone"] = True
        t0 = time.time()
        try:
            out = twirp("SIP", "CreateSIPParticipant", body, timeout=45)
        except Exception as err:  # no reply at all within the request timeout
            say("waited %s -> NO REPLY after %.1f s: %s" % (name, time.time() - t0, err))
            continue
        elapsed = time.time() - t0
        if "_error" in out:
            err = out.get("_twirp") or {}
            say("waited %s -> ERROR after %.1f s http=%s code=%s meta=%s msg=%s" % (
                name, elapsed, out["_error"], err.get("code"),
                json.dumps(err.get("meta") or {}, sort_keys=True), (err.get("msg") or out["_body"])[:200]))
        else:
            say("waited %s -> OK after %.1f s call=%s" % (
                name, elapsed, out.get("sip_call_id") or out.get("sipCallId") or out))
    time.sleep(5)  # the answered calls are hung up by the far end three seconds after the ACK
    say("waited done")


def tcp_stress(count):
    """Place `count` busy calls over TCP, one after another. The caller counts the outcomes in the logs."""
    trunk = twirp("SIP", "CreateSIPOutboundTrunk", {"trunk": {
        "name": "stress-tcp", "address": CARRIER, "numbers": ["+4700000003"],
        "transport": "SIP_TRANSPORT_TCP", "auth_username": "trunkuser", "auth_password": "trunkpass"}})
    tid = trunk.get("sip_trunk_id") or trunk.get("sipTrunkId")
    if not tid:
        say("stress: no trunk: %s" % trunk)
        sys.exit(1)
    placed = 0
    for i in range(count):
        out = twirp("SIP", "CreateSIPParticipant", {
            "sip_trunk_id": tid, "sip_call_to": "+47600%05d486" % i, "room_name": "predial-stress%d" % i,
            "participant_identity": "sip-stress%d" % i, "participant_name": "customer",
            "participant_attributes": {"tb.callId": "stress%d" % i}, "hide_phone_number": True})
        placed += 0 if "_error" in out else 1
        time.sleep(0.12)
    time.sleep(3)
    say("stress: placed %d of %d" % (placed, count))


def participants(room):
    out = twirp("RoomService", "ListParticipants", {"room": room}, room=room)
    if "_error" in out and out["_error"] != 404:
        say("  ListParticipants error: %s" % out)
    return out.get("participants", [])


if len(sys.argv) > 1 and sys.argv[1] == "waited":
    waited_scenario()
    sys.exit(0)

if len(sys.argv) > 2 and sys.argv[1] == "tcpstress":
    tcp_stress(int(sys.argv[2]))
    sys.exit(0)

if len(sys.argv) > 1 and sys.argv[1] == "drain":
    drain_scenario()
    sys.exit(0)

# ── setup ────────────────────────────────────────────────────────────────────
trunk = twirp("SIP", "CreateSIPOutboundTrunk", {"trunk": {
    "name": "carrier", "address": CARRIER, "numbers": ["+4700000001"],
    "auth_username": "trunkuser", "auth_password": "trunkpass"}})
trunk_id = trunk.get("sip_trunk_id") or trunk.get("sipTrunkId")
say("outbound trunk: %s" % (trunk_id or trunk))
inbound = twirp("SIP", "CreateSIPInboundTrunk", {"trunk": {
    "name": "in", "allowed_addresses": ["172.16.0.0/12"]}})
inbound_id = inbound.get("sip_trunk_id") or inbound.get("sipTrunkId")
say("inbound trunk: %s" % (inbound_id or inbound))
rule = twirp("SIP", "CreateSIPDispatchRule", {
    "rule": {"dispatch_rule_individual": {"room_prefix": "inbound-org1"}},
    "trunk_ids": [inbound_id], "name": "inbound"})
say("dispatch rule: %s" % (rule.get("sip_dispatch_rule_id") or rule.get("sipDispatchRuleId") or rule))
if not trunk_id or not inbound_id:
    sys.exit(1)


# ── outbound calls ───────────────────────────────────────────────────────────
def dial(name, number, ring_timeout="30s"):
    out = twirp("SIP", "CreateSIPParticipant", {
        "sip_trunk_id": trunk_id, "sip_call_to": number, "room_name": "predial-" + name,
        "participant_identity": "sip-" + name, "participant_name": "customer",
        "participant_attributes": {"tb.callId": name, "tb.orgId": "org1", "tb.role": "customer"},
        "hide_phone_number": True, "ringing_timeout": ring_timeout})
    say("dial %s -> %s" % (name, out.get("sip_call_id") or out.get("sipCallId") or out))


def watch(name, seconds):
    """Poll the room and print each change of the SIP participant's sip.* attributes."""
    last = None
    end = time.time() + seconds
    while time.time() < end:
        ps = [p for p in participants("predial-" + name) if p.get("identity") == "sip-" + name]
        attrs = ps[0].get("attributes", {}) if ps else None
        shown = None if attrs is None else {k: v for k, v in sorted(attrs.items())
                                             if k.startswith("sip.") or k.startswith("lk.sip.")}
        if shown != last:
            say("  %s room view: %s" % (name, "participant gone" if shown is None else json.dumps(shown)))
            last = shown
        time.sleep(0.15)


dial("busy", "+4711111486")
watch("busy", 4)
dial("ringout", "+4722222000", ring_timeout="3s")
watch("ringout", 6)
dial("answered", "+4733333200")
watch("answered", 8)

# Warm transfer: the customer leg is in the room; a consult leg joins the same
# room and is told not to listen to the customer (tilbyderen.consult_mute).
# A third leg without the attribute is the control: it must subscribe.
# Each far end sends its own tone (fake_carrier.TONES), so the carrier's log
# also says who hears whom.
dial("custA", "+4744444208")
time.sleep(1.6)
for identity, number, attrs in (
        ("xfer-muted", "+4755551208", {"tb.role": "consult", "tilbyderen.consult_mute": "sip-custA"}),
        ("xfer-control", "+4755552208", {"tb.role": "consult"})):
    out = twirp("SIP", "CreateSIPParticipant", {
        "sip_trunk_id": trunk_id, "sip_call_to": number, "room_name": "predial-custA",
        "participant_identity": identity, "participant_name": "consult",
        "participant_attributes": attrs, "hide_phone_number": True})
    say("dial %s into predial-custA -> %s" % (identity, out.get("sip_call_id") or out.get("sipCallId") or out))
time.sleep(10.5)  # every leg is hung up by its far end eight seconds after its answer


# ── the same three outbound calls over TCP (what production uses) ───────────
tcp = twirp("SIP", "CreateSIPOutboundTrunk", {"trunk": {
    "name": "carrier-tcp", "address": CARRIER, "numbers": ["+4700000002"],
    "transport": "SIP_TRANSPORT_TCP", "auth_username": "trunkuser", "auth_password": "trunkpass"}})
udp_trunk_id, trunk_id = trunk_id, (tcp.get("sip_trunk_id") or tcp.get("sipTrunkId"))
say("tcp outbound trunk: %s" % (trunk_id or tcp))
dial("tcpbusy", "+4766666486")
watch("tcpbusy", 4)
dial("tcpringout", "+4777777000", ring_timeout="3s")
watch("tcpringout", 6)
dial("tcpanswered", "+4788888200")
watch("tcpanswered", 8)
# three at once: they should share one connection to the carrier
for i in range(3):
    dial("tcpburst%d" % i, "+478888%d486" % i)
time.sleep(4)
trunk_id = udp_trunk_id

# ── one inbound call ─────────────────────────────────────────────────────────
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", 5060))
sock.settimeout(0.5)
caller = ToneSession("inbound-caller", 700, say, port=41000)  # the port the offer below names
target = (socket.gethostbyname(SIP_HOST), 5060)
call_id = "in%08x@%s" % (random.getrandbits(32), MY_IP)
branch = "z9hG4bK%08x" % random.getrandbits(32)
sdp = "\r\n".join(["v=0", "o=- 1 1 IN IP4 " + MY_IP, "s=-", "c=IN IP4 " + MY_IP, "t=0 0",
                   "m=audio 41000 RTP/AVP 0 8 101", "a=rtpmap:0 PCMU/8000", "a=rtpmap:8 PCMA/8000",
                   "a=rtpmap:101 telephone-event/8000", "a=fmtp:101 0-16", "a=sendrecv", ""])
base = ["Max-Forwards: 70",
        "From: <sip:+4799999999@%s>;tag=caller1" % MY_IP,
        "Call-ID: " + call_id,
        "Contact: <sip:caller@%s:5060>" % MY_IP]
invite = ["INVITE sip:u0047in@%s:5060 SIP/2.0" % target[0],
          "Via: SIP/2.0/UDP %s:5060;branch=%s" % (MY_IP, branch),
          "To: <sip:u0047in@%s>" % target[0], "CSeq: 1 INVITE"] + base + [
          "Content-Type: application/sdp", "Content-Length: %d" % len(sdp)]
t0 = time.time()
sock.sendto(("\r\n".join(invite) + "\r\n\r\n" + sdp).encode(), target)
say("inbound: INVITE sent")
to_with_tag = None
contact_200 = None
answer_sdp = ""
seen = set()
deadline = time.time() + 10
while time.time() < deadline and contact_200 is None:
    try:
        data, _ = sock.recvfrom(65535)
    except socket.timeout:
        continue
    text = data.decode("utf-8", "replace")
    first = text.split("\r\n")[0]
    headers = {}
    for line in text.split("\r\n\r\n")[0].split("\r\n")[1:]:
        k, _, v = line.partition(":")
        headers[k.strip().lower()] = v.strip()
    if first not in seen:
        seen.add(first)
        say("inbound: +%.2fs %s  contact=%s" % (time.time() - t0, first, headers.get("contact", "-")))
    if first.startswith("SIP/2.0 200") and "INVITE" in headers.get("cseq", ""):
        to_with_tag = headers.get("to")
        contact_200 = headers.get("contact", "")
        answer_sdp = text.partition("\r\n\r\n")[2]
if contact_200 is None:
    say("inbound: no 200 OK within 10 s")
    sys.exit(2)
uri = contact_200[contact_200.find("<") + 1:contact_200.find(">")] if "<" in contact_200 else contact_200
ack = ["ACK %s SIP/2.0" % uri,
       "Via: SIP/2.0/UDP %s:5060;branch=z9hG4bK%08x" % (MY_IP, random.getrandbits(32)),
       "To: " + to_with_tag, "CSeq: 1 ACK"] + base + ["Content-Length: 0"]
sock.sendto(("\r\n".join(ack) + "\r\n\r\n").encode(), target)
say("inbound: ACK sent to %s t=%d" % (uri, now_ms()))
media_host, media_port = media_address(answer_sdp)
if not media_host or not media_port:
    say("inbound: no media address in the answer")
    sys.exit(2)
caller.send_to(media_host, media_port)
time.sleep(3.5)  # nobody else is in the room: the caller should hear ringing

# The other party arrives: a second leg into the caller's room. Its far end
# (the stand-in carrier) answers after a second and sends 1000 Hz.
rooms = [r.get("name", "") for r in twirp("RoomService", "ListRooms", {}).get("rooms", [])]
inbound_room = next((name for name in rooms if name.startswith("inbound-org1")), None)
if not inbound_room:
    say("inbound: the caller's room was not found among %s" % rooms)
    sys.exit(2)
out = twirp("SIP", "CreateSIPParticipant", {
    "sip_trunk_id": trunk_id, "sip_call_to": "+4733335208", "room_name": inbound_room,
    "participant_identity": "agent-leg", "participant_name": "agent",
    "participant_attributes": {"tb.callId": "agentleg", "tb.orgId": "org1"}, "hide_phone_number": True})
say("inbound: agent leg dialled into the caller's room t=%d -> %s" % (
    now_ms(), out.get("sip_call_id") or out.get("sipCallId") or out))
time.sleep(7.5)
bye = ["BYE %s SIP/2.0" % uri,
       "Via: SIP/2.0/UDP %s:5060;branch=z9hG4bK%08x" % (MY_IP, random.getrandbits(32)),
       "To: " + to_with_tag, "CSeq: 2 BYE"] + base + [
       'Reason: Q.850;cause=16;text="Normal call clearing"', "Content-Length: 0"]
sock.sendto(("\r\n".join(bye) + "\r\n\r\n").encode(), target)
say("inbound: BYE sent t=%d" % now_ms())
caller.close()
try:
    data, _ = sock.recvfrom(65535)
    say("inbound: " + data.decode("utf-8", "replace").split("\r\n")[0])
except socket.timeout:
    say("inbound: no answer to BYE")
time.sleep(4)  # the agent leg is hung up by its far end
say("done")
