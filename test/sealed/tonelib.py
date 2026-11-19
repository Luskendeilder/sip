"""Sound for the sealed test: send one tone over RTP and report which tones come back.

Every stand-in endpoint of a call (the carrier's side of an outbound call, the
caller's side of the inbound call) sends a sine wave of its own frequency as
G.711 mu-law RTP, and logs, five times a second, which of the known
frequencies it hears. check.py reads those lines to tell who heard whom, and
when: the ringing tone, the audio of the other party, or nothing.
"""
import math
import random
import socket
import struct
import threading
import time

RATE = 8000
FRAME = 160  # 20 ms
WINDOW = 1600  # 200 ms of audio per report
RINGBACK = 425  # ETSI ringing tone
# Every frequency a stand-in sends, plus the ringing tone. Far enough apart for
# a 200 ms window to tell them from each other.
KNOWN = (425, 600, 700, 900, 1000, 1300)
# A tone counts as heard above this amplitude (16-bit scale). The stand-ins
# send at 8000; the ringing tone is about 16000.
HEARD_ABOVE = 1200


def _encode_ulaw(sample):
    sign = 0x80 if sample < 0 else 0
    if sample < 0:
        sample = -sample
    sample = min(sample, 32635) + 0x84
    exponent, mask = 7, 0x4000
    while exponent > 0 and not sample & mask:
        exponent -= 1
        mask >>= 1
    return ~(sign | (exponent << 4) | ((sample >> (exponent + 3)) & 0x0F)) & 0xFF


def _decode_ulaw(byte):
    byte = ~byte & 0xFF
    sample = ((((byte & 0x0F) << 3) + 0x84) << ((byte >> 4) & 0x07)) - 0x84
    return -sample if byte & 0x80 else sample


def _decode_alaw(byte):
    byte ^= 0x55
    exponent, mantissa = (byte >> 4) & 0x07, byte & 0x0F
    sample = (mantissa << 4) + 8
    if exponent:
        sample = (sample + 0x100) << (exponent - 1)
    return sample if byte & 0x80 else -sample


ULAW = [_decode_ulaw(b) for b in range(256)]
ALAW = [_decode_alaw(b) for b in range(256)]


def amplitude(samples, freq):
    """Amplitude of `freq` in the samples (Goertzel)."""
    k = 2 * math.cos(2 * math.pi * freq / RATE)
    s1 = s2 = 0.0
    for x in samples:
        s1, s2 = x + k * s1 - s2, s1
    power = s1 * s1 + s2 * s2 - k * s1 * s2
    return 2 * math.sqrt(max(power, 0.0)) / len(samples)


class ToneSession:
    """One RTP endpoint: sends `freq` to `target` once it is known, reports what it hears."""

    def __init__(self, label, freq, log, port=0):
        self.label, self.freq, self.log = label, freq, log
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))
        self.sock.settimeout(0.2)
        self.port = self.sock.getsockname()[1]
        self.target = None
        self.stop = threading.Event()
        self.received = 0
        self.threads = [threading.Thread(target=self._receive, daemon=True)]
        self.threads[0].start()

    def send_to(self, host, port):
        if self.target is None:
            self.target = (host, port)
            sender = threading.Thread(target=self._send, daemon=True)
            self.threads.append(sender)
            sender.start()

    def close(self):
        self.stop.set()
        for t in self.threads:
            t.join(timeout=1)
        self.sock.close()
        self.log("tone %s closed after %d packets" % (self.label, self.received))

    def _send(self):
        ssrc, seq, ts = random.getrandbits(32), random.getrandbits(15), random.getrandbits(31)
        step = 2 * math.pi * self.freq / RATE
        n = 0
        start = time.time()
        while not self.stop.is_set():
            payload = bytes(_encode_ulaw(int(8000 * math.sin(step * (n + i)))) for i in range(FRAME))
            header = struct.pack("!BBHII", 0x80, 0, seq & 0xFFFF, ts & 0xFFFFFFFF, ssrc)
            try:
                self.sock.sendto(header + payload, self.target)
            except OSError:
                return
            n += FRAME
            seq += 1
            ts += FRAME
            delay = start + n / RATE - time.time()
            if delay > 0:
                time.sleep(delay)

    def _receive(self):
        window = []
        while not self.stop.is_set():
            try:
                data, _ = self.sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                return
            if len(data) < 12 or data[0] >> 6 != 2:
                continue
            payload_type = data[1] & 0x7F
            table = ULAW if payload_type == 0 else ALAW if payload_type == 8 else None
            if table is None:
                continue  # telephone-event and the like
            offset = 12 + 4 * (data[0] & 0x0F)
            self.received += 1
            window.extend(table[b] for b in data[offset:])
            while len(window) >= WINDOW:
                chunk, window = window[:WINDOW], window[WINDOW:]
                heard = [f for f in KNOWN if amplitude(chunk, f) > HEARD_ABOVE]
                self.log("tone %s t=%d heard=%s" % (
                    self.label, int(time.time() * 1000), ",".join(str(f) for f in heard) or "-"))


def media_address(sdp):
    """The address and audio port an SDP body asks for."""
    host, port = None, None
    for line in sdp.replace("\r\n", "\n").split("\n"):
        if line.startswith("c=IN IP4 "):
            host = line.split()[2]
        elif line.startswith("m=audio "):
            port = int(line.split()[1])
    return host, port
