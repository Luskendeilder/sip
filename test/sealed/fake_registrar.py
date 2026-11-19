"""A stand-in for a carrier registrar, for a sealed test network only.

It challenges an unauthenticated REGISTER with 401 and accepts one that
carries credentials with 200 (it does not check the digest). Every request is
printed, so a test can see what the SIP service really put on the wire.
"""
import socket
import time

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", 5060))
print("fake registrar listening on udp/5060", flush=True)

while True:
    data, addr = sock.recvfrom(65535)
    text = data.decode("utf-8", "replace")
    lines = text.split("\r\n")
    start = lines[0]
    now = time.strftime("%H:%M:%S")
    if not start.startswith("REGISTER "):
        print("%s ignored: %s" % (now, start[:60]), flush=True)
        continue

    headers = {}
    vias = []
    for line in lines[1:]:
        if not line:
            break
        key, _, value = line.partition(":")
        key = key.strip().lower()
        if key == "via":
            vias.append(line)
        headers[key] = value.strip()

    authed = "authorization" in headers
    expires = headers.get("expires", "?")
    print("%s REGISTER from %s:%d expires=%s auth=%s contact=%s call-id=%s cseq=%s" % (
        now, addr[0], addr[1], expires, "yes" if authed else "no",
        headers.get("contact", "?"), headers.get("call-id", "?")[:10], headers.get("cseq", "?")), flush=True)

    to = headers.get("to", "")
    if ";tag=" not in to:
        to += ";tag=fakereg"
    common = vias + [
        "From: " + headers.get("from", ""),
        "To: " + to,
        "Call-ID: " + headers.get("call-id", ""),
        "CSeq: " + headers.get("cseq", ""),
    ]
    if authed:
        response = ["SIP/2.0 200 OK"] + common + [
            "Contact: %s;expires=%s" % (headers.get("contact", ""), expires),
            "Content-Length: 0", "", ""]
    else:
        response = ["SIP/2.0 401 Unauthorized"] + common + [
            'WWW-Authenticate: Digest realm="example.test", nonce="n0nce-%d"' % int(time.time()),
            "Content-Length: 0", "", ""]
    sock.sendto("\r\n".join(response).encode(), addr)
