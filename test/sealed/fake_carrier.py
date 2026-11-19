"""A stand-in carrier for a sealed test network only. Speaks SIP over UDP and TCP.

It answers INVITEs the way a carrier would, picked by the last three digits of
the dialled number:
  ...486  ->  100, 180, then 486 Busy Here
  ...480, ...404, ...503, ...603  ->  100, 180, then that rejection
  ...000  ->  100, 180, then silence until the caller CANCELs (a ring-out)
  ...200  ->  100, 180, then 200 OK with an SDP answer; three seconds after
              the ACK it hangs up with a BYE carrying a Q.850 cause
  ...208  ->  the same, but it hangs up eight seconds after the ACK (a call
              that is still live when the service is told to stop)
Like Telavox it first challenges every INVITE with 407. Every message it sees
is printed, so a test can read what the SIP service really put on the wire.

An answered call has sound: from the ACK on, the carrier sends a sine wave to
the address in the INVITE's SDP (1000 Hz, or the frequency listed for the
number in TONES) and prints which tones it hears coming back (tonelib.py).
"""
import os
import random
import socket
import threading
import time

from tonelib import ToneSession, media_address

MY_IP = socket.gethostbyname(socket.gethostname())
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", 5060))
lock = threading.Lock()
calls = {}  # Call-ID -> dict
CRLF = "\r\n"
# A real carrier is some milliseconds away. Without a delay the reply to an
# INVITE over TCP can arrive before the SIP service has started waiting for it,
# which exposes a known race in upstream's response wait (livekit/sip PRs #923
# and #995) that a real network never triggers. CARRIER_DELAY_MS=0 switches the
# delay off to probe for exactly that.
DELAY = float(os.environ.get("CARRIER_DELAY_MS", "5")) / 1000.0
# Final rejections by the last three digits of the dialled number:
# (reason phrase, Q.850 cause, cause text).
REJECTIONS = {
    "486": ("Busy Here", 17, "User busy"),
    "480": ("Temporarily Unavailable", 18, "No user responding"),
    "404": ("Not Found", 1, "Unallocated number"),
    "503": ("Service Unavailable", 41, "Temporary failure"),
    "603": ("Decline", 21, "Call rejected"),
}
# What the far end of an answered call sounds like, by dialled number, so a
# test can tell who hears whom. 1000 Hz unless listed.
TONES = {"+4744444208": 600, "+4755551208": 900, "+4755552208": 1300}


def log(msg):
    print("%s %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def send(dest, text):
    """dest is ("udp", address) or ("tcp", connection): answer where the request came from."""
    kind, target = dest
    if kind == "udp":
        sock.sendto(text.encode(), target)
    else:
        target.sendall(text.encode())


def parse(text):
    head, _, body = text.partition(CRLF + CRLF)
    lines = head.split(CRLF)
    headers = []
    for line in lines[1:]:
        key, _, value = line.partition(":")
        headers.append((key.strip(), value.strip()))
    return lines[0], headers, body


def get(headers, name):
    for key, value in headers:
        if key.lower() == name.lower():
            return value
    return ""


def respond(dest, code, reason, headers, to_tag, extra=(), body=""):
    if DELAY:
        time.sleep(DELAY)
    out = ["SIP/2.0 %d %s" % (code, reason)]
    out += ["Via: " + v for k, v in headers if k.lower() == "via"]
    to = get(headers, "To")
    if ";tag=" not in to and code != 100:
        to += ";tag=" + to_tag
    out += ["From: " + get(headers, "From"), "To: " + to,
            "Call-ID: " + get(headers, "Call-ID"), "CSeq: " + get(headers, "CSeq")]
    out += list(extra)
    out.append("Content-Length: %d" % len(body))
    send(dest, CRLF.join(out) + CRLF + CRLF + body)


def sdp_answer(port):
    return CRLF.join([
        "v=0", "o=- 1 1 IN IP4 " + MY_IP, "s=-", "c=IN IP4 " + MY_IP, "t=0 0",
        "m=audio %d RTP/AVP 0 101" % port, "a=rtpmap:0 PCMU/8000",
        "a=rtpmap:101 telephone-event/8000", "a=fmtp:101 0-16", "a=sendrecv", ""])


def end_tone(c):
    with lock:
        tone = c.pop("tone", None)
    if tone:
        tone.close()


def run_call(call_id):
    c = calls[call_id]
    headers, dest, tag = c["headers"], c["dest"], c["tag"]
    number = c["number"]
    time.sleep(0.3)
    respond(dest, 180, "Ringing", headers, tag)
    time.sleep(0.7)
    if c.get("cancelled"):
        return
    if number[-3:] in REJECTIONS:
        phrase, cause, text = REJECTIONS[number[-3:]]
        respond(dest, int(number[-3:]), phrase, headers, tag,
                extra=['Reason: Q.850;cause=%d;text="%s"' % (cause, text)])
    elif number.endswith("200") or number.endswith("208"):
        tone = ToneSession(number, TONES.get(number, 1000), log)
        with lock:
            c["tone"] = tone
        respond(dest, 200, "OK", headers, tag,
                extra=["Contact: <sip:carrier@%s:5060%s>" % (MY_IP, ";transport=tcp" if dest[0] == "tcp" else ""),
                       "Content-Type: application/sdp"],
                body=sdp_answer(tone.port))
    # ...000: say nothing more; the caller's ring timeout ends it


def hang_up(call_id):
    c = calls[call_id]
    time.sleep(8 if c["number"].endswith("208") else 3)
    headers = c["headers"]
    contact = get(headers, "Contact").strip()
    uri = contact[contact.find("<") + 1:contact.find(">")] if "<" in contact else contact
    to = get(headers, "To")
    if ";tag=" not in to:
        to += ";tag=" + c["tag"]
    out = ["BYE %s SIP/2.0" % uri,
           "Via: SIP/2.0/%s %s:5060;branch=z9hG4bK%08x" % (c["dest"][0].upper(), MY_IP, random.getrandbits(32)),
           "Max-Forwards: 70",
           "From: " + to, "To: " + get(headers, "From"),
           "Call-ID: " + call_id, "CSeq: 1 BYE",
           'Reason: Q.850;cause=16;text="Normal call clearing"',
           "Content-Length: 0"]
    send(c["dest"], CRLF.join(out) + CRLF + CRLF)
    log("-> BYE sent (Q.850 cause 16) to %s for the call to %s" % (uri, c["number"]))
    end_tone(c)


def handle(text, dest):
    start, headers, body = parse(text)
    call_id = get(headers, "Call-ID")
    method = start.split(" ")[0]
    if start.startswith("SIP/2.0"):
        log("<- response %s (%s)" % (start, get(headers, "CSeq")))
        return
    if method == "INVITE":
        number = start.split(" ")[1].split("@")[0].replace("sip:", "")
        authed = bool(get(headers, "Proxy-Authorization"))
        log("<- INVITE over %s to=%s auth=%s contact=%s cseq=%s" % (
            dest[0], number, "yes" if authed else "no", get(headers, "Contact"), get(headers, "CSeq")))
        if not authed:
            respond(dest, 407, "Proxy Authentication Required", headers, "chal%04x" % random.getrandbits(16),
                    extra=['Proxy-Authenticate: Digest realm="example.test", nonce="n0nce-%d"' % int(time.time())])
            return
        with lock:
            if call_id in calls and calls[call_id]["cseq"] == get(headers, "CSeq"):
                return  # a retransmission
            calls[call_id] = {"headers": headers, "dest": dest, "number": number, "sdp": body,
                              "tag": "car%06x" % random.getrandbits(24), "cseq": get(headers, "CSeq")}
        respond(dest, 100, "Trying", headers, "")
        threading.Thread(target=run_call, args=(call_id,), daemon=True).start()
    elif method == "ACK":
        c = calls.get(call_id)
        log("<- ACK over %s (%s)" % (dest[0], get(headers, "CSeq")))
        if c and c["number"][-3:] in ("200", "208") and not c.get("acked"):
            c["acked"] = True
            host, port = media_address(c["sdp"])
            if c.get("tone") and host and port:
                c["tone"].send_to(host, port)
            threading.Thread(target=hang_up, args=(call_id,), daemon=True).start()
    elif method == "CANCEL":
        c = calls.get(call_id)
        log("<- CANCEL over %s" % dest[0])
        respond(dest, 200, "OK", headers, c["tag"] if c else "x")
        if c:
            c["cancelled"] = True
            end_tone(c)
            respond(dest, 487, "Request Terminated", c["headers"], c["tag"])
    elif method == "BYE":
        log("<- BYE over %s reason=%s" % (dest[0], get(headers, "Reason") or "-"))
        if call_id in calls:
            end_tone(calls[call_id])
        respond(dest, 200, "OK", headers, "x")
    else:
        log("<- %s" % start)
        respond(dest, 200, "OK", headers, "x")


def tcp_reader(conn):
    """Frame SIP messages on one TCP connection by Content-Length."""
    buf = b""
    sep = (CRLF + CRLF).encode()
    try:
        while True:
            chunk = conn.recv(65535)
            if not chunk:
                return
            buf += chunk
            while True:
                end = buf.find(sep)
                if end < 0:
                    break
                head = buf[:end].decode("utf-8", "replace")
                length = 0
                for line in head.split(CRLF)[1:]:
                    key, _, value = line.partition(":")
                    if key.strip().lower() in ("content-length", "l"):
                        length = int(value.strip() or 0)
                total = end + 4 + length
                if len(buf) < total:
                    break
                message, buf = buf[:total], buf[total:]
                if message.strip():
                    handle(message.decode("utf-8", "replace"), ("tcp", conn))
    except OSError:
        return


def tcp_listener():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", 5060))
    server.listen(16)
    while True:
        conn, peer = server.accept()
        log("tcp connection opened from %s:%d" % peer)
        threading.Thread(target=tcp_reader, args=(conn,), daemon=True).start()


log("fake carrier on %s udp/5060 and tcp/5060, reply delay %.0f ms" % (MY_IP, DELAY * 1000))
threading.Thread(target=tcp_listener, daemon=True).start()
while True:
    data, peer = sock.recvfrom(65535)
    handle(data.decode("utf-8", "replace"), ("udp", peer))
