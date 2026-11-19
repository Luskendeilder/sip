"""Read what the sealed-network run left in the output folder and check every fork behaviour.

Prints one line per check and exits 1 if any failed.
"""
import collections
import json
import re
import sys

out = sys.argv[1]


def read(name):
    try:
        return open("%s/%s" % (out, name), encoding="utf-8", errors="replace").read()
    except OSError:
        return ""


driver, sip, carrier = read("driver.log"), read("sip.log"), read("carrier.log")
registrar, hook, stop, metrics = read("registrar.log"), read("hook.log"), read("stop.txt"), read("metrics-running.txt")
stream_raw = read("stream.jsonl")
stream = [json.loads(line) for line in stream_raw.splitlines() if line.strip()]

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print("%s  %s%s" % ("PASS" if ok else "FAIL", name, (" (%s)" % detail) if detail else ""))


def events(call_attr, kind):
    return [r for r in stream if r.get("type") == kind and (r.get("attrs") or {}).get("tb.callId") == call_attr]


print("== the image under test")
check("the service wrote no error-level log line",
      not re.search(r'"level":"error"', sip), "see sip.log")
check("driver finished", driver.rstrip().endswith("done"), "see driver.log")

print("== Telavox login (REGISTER)")
check("both logins registered with credentials",
      len(re.findall(r"expires=(?:180|3600) auth=yes", registrar)) >= 2)
check("the registration metric shows 2 held logins",
      len(re.findall(r'livekit_sip_registrations_active\{[^}]*\} 1', metrics)) == 2)
check("on the stop signal both logins were signed out WITH credentials",
      len(re.findall(r"expires=0 auth=yes", registrar)) == 2)
check("it exited cleanly", "exit_code=0" in stop, stop.strip())

print("== Contact header")
check("outbound INVITE carries the user part",
      "contact=<sip:u0047test@" in carrier)
check("inbound 180 and 200 carry the user part",
      len(re.findall(r"inbound: \+[0-9.]+s SIP/2.0 (?:180|200)[^\n]*contact=<sip:u0047test@", driver)) >= 2)

print("== inbound: answer at once, with ringback")
m = re.search(r"inbound: \+([0-9.]+)s SIP/2.0 200 OK", driver)
check("inbound call answered within one second", bool(m) and float(m.group(1)) < 1.0,
      "%s s" % (m.group(1) if m else "no 200 OK"))
check("the subscribe wait was skipped", "waitSubscribe: skipped" in sip)
check("ringback started and stopped", "inbound ringback: started" in sip and "inbound ringback: stopped" in sip)
check("the ringing tone was played once: for the inbound call, and for no outbound call",
      sip.count("inbound ringback: started") == 1, "%d times" % sip.count("inbound ringback: started"))
check("it stopped because the other party arrived",
      bool(re.search(r'"msg":"inbound ringback: stopped"[^\n]*"reason":"subscribed"', sip)))

print("== sound on the inbound call")


def tone_windows(text, label):
    """What one endpoint heard: [(time in ms, set of frequencies)], one item per 200 ms."""
    found = re.findall(r"tone %s t=(\d+) heard=(\S+)" % re.escape(label), text)
    return [(int(t), set() if heard == "-" else {int(f) for f in heard.split(",")}) for t, heard in found]


def heard(windows, freq, start=0, end=float("inf")):
    return sum(1 for t, freqs in windows if start <= t < end and freq in freqs)


def stamp(text, what):
    m = re.search(what + r"[^\n]*? t=(\d+)", text)
    return int(m.group(1)) if m else None


caller = tone_windows(driver, "inbound-caller")
agent_far = tone_windows(carrier, "+4733335208")
acked, joined, hung_up = (stamp(driver, w) for w in ("inbound: ACK sent", "inbound: agent leg dialled", "inbound: BYE sent"))
timed = None not in (acked, joined, hung_up)
ringing = heard(caller, 425, acked, joined) if timed else 0
check("the caller hears the ringing tone while nobody else is in the room", ringing >= 3,
      "%d windows of 200 ms" % ringing)
late = heard(caller, 425, joined + 1000, hung_up) if timed else -1
check("the ringing tone is gone within a second of the other party arriving", late == 0,
      "%d windows with the tone after that" % late)
down = heard(caller, 1000, joined + 1000, hung_up) if timed else 0
check("after that the caller hears the other party", down >= 5, "%d windows" % down)
up = heard(agent_far, 700, joined, hung_up) if timed else 0
check("and the other party hears the caller", up >= 5, "%d windows" % up)
check("the caller does not hear itself", bool(caller) and heard(caller, 700) == 0,
      "%d windows listened to" % len(caller))

print("== hang-up code in the participant_left webhook")


def left(room):
    """The attributes a room's SIP leg had when it left."""
    m = re.search(r"participant_left room=%s [^\n]*attrs=(\{[^\n]*\})" % re.escape(room), hook)
    return json.loads(m.group(1)) if m else {}


check("busy call carries sip.statusCode 486", left("predial-busy").get("sip.statusCode") == "486")
check("our own ring-out carries sip.statusCode 408", left("predial-ringout").get("sip.statusCode") == "408")
wording = {"manual-busy": ("486", "Busy Here"), "manual-unavailable": ("480", "Temporarily Unavailable"),
           "manual-carrierdown": ("503", "Service Unavailable"), "manual-declined": ("603", "Decline"),
           "manual-ringout": ("408", "Request Timeout (ring timeout)")}
wrong = sorted(room for room, (code, text) in wording.items()
               if (left(room).get("sip.statusCode"), left(room).get("sip.status")) != (code, text))
check("every refused or unanswered dial leaves its code and wording (486, 480, 503, 603, 408)", not wrong,
      "wrong for: %s" % ", ".join(wrong) if wrong else "")
check("an answered call leaves no hang-up code",
      all(left(room) and "sip.statusCode" not in left(room) for room in ("predial-answered", "manual-answered")))
check("answered call carries the answer time",
      bool(re.search(r'participant_left room=predial-answered [^\n]*"lk.sip.answerTime": "\d+"', hook)))

print("== warm-transfer guard (tilbyderen.consult_mute)")
hears = collections.defaultdict(set)
skipped = collections.defaultdict(set)
for line in sip.splitlines():
    if '"room":"predial-custA"' not in line:
        continue
    who = re.findall(r'"participant":"([^"]+)"', line)
    if len(who) < 2:
        continue
    if '"msg":"track subscribed"' in line:
        hears[who[0]].add(who[-1])
    elif "consult-mute: skipping subscription" in line:
        skipped[who[0]].add(who[-1])
check("the muted consult leg does not hear the customer",
      "sip-custA" not in hears["xfer-muted"] and "sip-custA" in skipped["xfer-muted"],
      "hears %s" % sorted(hears["xfer-muted"]))
check("a consult leg without the attribute does hear the customer (control)",
      "sip-custA" in hears["xfer-control"])
# The same in sound. The customer's far end sends 600 Hz, the control leg's 1300 Hz.
muted_far, control_far = tone_windows(carrier, "+4755551208"), tone_windows(carrier, "+4755552208")
check("in sound: the muted consult leg never hears the customer",
      len(muted_far) >= 10 and heard(muted_far, 600) == 0,
      "%d windows listened to, %d with the customer" % (len(muted_far), heard(muted_far, 600)))
check("in sound: it does hear the other consult leg", heard(muted_far, 1300) >= 3,
      "%d windows" % heard(muted_far, 1300))
check("in sound: a consult leg without the attribute hears the customer", heard(control_far, 600) >= 3,
      "%d windows" % heard(control_far, 600))

print("== outbound over TCP")
check("all TCP calls shared one connection to the carrier",
      carrier.count("tcp connection opened") == 1, "%d opened" % carrier.count("tcp connection opened"))
check("cancel and hang-up travelled over TCP",
      "CANCEL over tcp" in carrier and bool(re.search(r"BYE sent[^\n]*transport=tcp", carrier)))

print("== dials that wait for the answer (manual and consult legs)")
waited = read("driver-waited.log")


def waited_line(name):
    m = re.search(r"waited %s -> ([^\n]*)" % name, waited)
    return m.group(1) if m else "no line"


def waited_seconds(name):
    m = re.search(r"after ([0-9.]+) s", waited_line(name))
    return float(m.group(1)) if m else -1.0


rejections = {"busy": 486, "unavailable": 480, "carrierdown": 503, "declined": 603}
missing = sorted(name for name, code in rejections.items()
                 if not re.search(r'^ERROR .*"sip_status_code": "%d"' % code, waited_line(name)))
check("a rejected dial hands the carrier's code to the caller in the error (486, 480, 503, 603)",
      not missing, "missing for: %s" % ", ".join(missing) if missing else "")
check("a ring-out ends the request when the ring time is up, without a SIP code",
      waited_line("ringout").startswith("ERROR") and "sip_status_code" not in waited_line("ringout")
      and 2.5 <= waited_seconds("ringout") <= 6, waited_line("ringout")[:60])
check("an answered dial returns right after the answer",
      waited_line("answered").startswith("OK") and 0.5 <= waited_seconds("answered") <= 5, waited_line("answered")[:40])
check("a consult leg with a dial tone is answered the same way",
      waited_line("consult").startswith("OK") and 0.5 <= waited_seconds("consult") <= 5, waited_line("consult")[:40])

print("== TCP with a carrier that replies instantly (the response-wait race, upstream PR #995)")
carrier0 = read("carrier0.log")
stress = read("driver-stress.log")
lost = len(re.findall(r"transaction failed to complete", sip))
check("all 40 calls were placed", "stress: placed 40 of 40" in stress, stress.strip()[-80:])
check("no call was lost to 'transaction failed to complete'", lost == 0, "%d log lines" % lost)
check("the carrier saw the authenticated INVITE of all 40",
      carrier0.count("auth=yes") == 40, "%d" % carrier0.count("auth=yes"))

print("== event stream")
calls = {r["callId"] for r in stream}
check("the stream has entries for every call", len(calls) >= 61, "%d calls, %d entries" % (len(calls), len(stream)))
# Every number the drivers dial or present, written out: a pattern for "any
# phone number" would also match the millisecond timestamps in the stream.
TEST_NUMBERS = ["4711111486", "4722222000", "4733333200", "4733335208", "4744444208", "4755551208", "4755552208",
                "4766666486", "4777777000", "4788888200", "47888880486", "47888881486", "47888882486",
                "4799999999", "4700000001", "4700000002", "4700000003", "4700000004",
                "4791000486", "4791000480", "4791000503", "4791000603", "4791000000",
                "4791000200", "4791001200"] + ["47600%05d486" % i for i in range(40)]
check("no phone number in the stream",
      bool(stream_raw) and not any(n in stream_raw for n in TEST_NUMBERS))
check("busy: failed with 486 and the carrier's cause",
      any(e.get("status") == 486 and "cause=17" in e.get("reason", "") for e in events("busy", "sip.failed")))
check("ring-out: cancel sent, reason timeout",
      any(e.get("reason") == "timeout" for e in events("ringout", "sip.cancel.sent")))
check("answered: answer, then the far end's hang-up with its cause",
      bool(events("answered", "sip.answered.received"))
      and any("cause=16" in e.get("reason", "") for e in events("answered", "sip.bye.received")))
check("every call has exactly one call.ended",
      all(sum(1 for r in stream if r["callId"] == c and r.get("type") == "sip.call.ended") == 1 for c in calls))
check("events over both transports",
      {r.get("transport") for r in stream if r.get("type") == "sip.invite.sent"} >= {"udp", "tcp"})
m = re.search(r'livekit_sip_event_stream_entries_total\{[^}]*result="published"[^}]*\} (\d+)', metrics)
dropped = re.findall(r'livekit_sip_event_stream_entries_total\{[^}]*result="dropped[^}]*\} (\d+)', metrics)
check("nothing was dropped on the way to Redis",
      bool(m) and int(m.group(1)) == len(stream) and all(int(d) == 0 for d in dropped),
      "published %s, in stream %d" % (m.group(1) if m else "?", len(stream)))

print("== stop signal while a call is live (the drain)")
drain = read("driver-drain.log")
after = [json.loads(line) for line in read("stream-after-drain.jsonl").splitlines() if line.strip()]


def clock(text):
    m = re.search(r"(\d\d):(\d\d):(\d\d)", text)
    return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3)) if m else None


sigterm = clock(read("sigterm-at.txt"))
exited = clock(read("exited-at.txt"))
signed_out = [clock(line) for line in registrar.splitlines() if "expires=0 auth=yes" in line]
# The far end's hang-up of the live call, found by the number the drain driver dialled.
far_end_bye = [clock(line) for line in carrier.splitlines() if "BYE sent" in line and "4790000208" in line]
last_bye = far_end_bye[-1] if far_end_bye else None
m = re.search(r"stop_seconds=(\d+)", stop)
stop_seconds = int(m.group(1)) if m else -1
check("the live call was placed before the stop signal", "drain: live call placed -> SCL_" in drain)
check("the logins were signed out right after the signal, while the call was still up",
      bool(signed_out) and sigterm is not None and last_bye is not None
      and max(signed_out) - sigterm <= 2 and max(signed_out) < last_bye,
      "signal %s, signed out %s, far end hung up %s" % (sigterm, signed_out[-1:] and signed_out[-1], last_bye))
check("the service waited for the live call instead of cutting it",
      stop_seconds >= 3 and exited is not None and last_bye is not None and exited >= last_bye,
      "stopped after %d s" % stop_seconds)
ended = [r for r in after if r.get("type") == "sip.call.ended" and (r.get("attrs") or {}).get("tb.callId") == "drainlive"]
check("that call ended normally: answered, then the far end hung up",
      len(ended) == 1 and ended[0].get("status") == 200 and ended[0].get("reason") == "bye",
      json.dumps(ended[0]) if ended else "no call.ended in the stream")
check("a new dial during the drain was refused", "new dial while draining -> REFUSED" in drain,
      (re.search(r"drain: new dial[^\n]*", drain) or [""])[0][:160] if drain else "no driver log")

print()
print("%d of %d checks passed" % (sum(results), len(results)))
sys.exit(0 if all(results) else 1)
