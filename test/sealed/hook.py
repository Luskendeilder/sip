"""Webhook receiver for the sealed test network: prints what LiveKit reports when a SIP participant leaves."""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self.send_response(200)
        self.end_headers()
        try:
            event = json.loads(body.decode())
        except ValueError:
            return
        name = event.get("event")
        p = event.get("participant") or {}
        if name in ("participant_left", "participant_joined") and p.get("kind") in ("SIP", 3):
            attrs = {k: v for k, v in sorted((p.get("attributes") or {}).items())
                     if k.startswith("sip.") or k.startswith("lk.sip.") or k.startswith("tb.")}
            print("%s room=%s identity=%s disconnectReason=%s attrs=%s" % (
                name, (event.get("room") or {}).get("name"), p.get("identity"),
                p.get("disconnectReason", "-"), json.dumps(attrs)), flush=True)

    def log_message(self, *args):
        pass


print("webhook receiver on :8080", flush=True)
HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
