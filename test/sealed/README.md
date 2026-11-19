# Sealed-network call test (Tilbyderen fork)

One command that places real SIP calls, with sound, through a built
`livekit-sip` image and checks every behaviour this fork adds. It exists
because the fork lives next to code that upstream rewrites often, and neither a
clean merge nor a green build proves the fork's behaviour is still there.

    IMAGE=tilbyderen/livekit-sip:v1.25.0-fork ./run.sh

It needs Docker and nothing else, and takes about two and a half minutes. Exit
code 0 means every check passed. The output of the run is left in `out/`.

## Why "sealed"

Everything runs on a docker network created with `--internal`: no container on
it can reach the internet. The carrier, the registrar and the webhook receiver
are small stand-ins (Python, in this folder), and the config holds no real
credential. So the test can never register with, or dial through, a real
carrier. **Never run the image with the production `sip.yaml` anywhere but
production**: its `outbound_registrations` would take over the carrier's
routing of inbound calls.

## What runs

| Container | What it is |
|---|---|
| `<prefix>-sip` | the image under test, with the shape of the production `sip.yaml` and every fork key switched on |
| `<prefix>-lk` | `livekit/livekit-server` (default v1.13.7, the version production runs; override with `LIVEKIT_IMAGE`) |
| `<prefix>-redis` | Redis: LiveKit's bus, and the event stream |
| `<prefix>-carrier` | `fake_carrier.py`: answers INVITEs over UDP and TCP, challenges with 407 first like Telavox, and hangs up with a Q.850 cause |
| `<prefix>-registrar` | `fake_registrar.py`: challenges REGISTER with 401, accepts it with credentials |
| `<prefix>-hook` | `hook.py`: prints the `participant_joined` / `participant_left` webhooks |
| `<prefix>-driver` | `drive.py`: creates trunks and a dispatch rule, places the calls, and acts as the carrier for one inbound call |

`PREFIX` (default `sipseal`) names the containers and the network. `OUT`
(default `./out`) is where logs and the stream dump go.

## Sound

Every stand-in end of an answered call sends a sine wave of its own frequency
as G.711 RTP and prints, five times a second, which known frequencies it hears
coming back (`tonelib.py`). The carrier sends 1000 Hz, or the frequency listed
for the dialled number in `fake_carrier.TONES`; the inbound caller sends
700 Hz; the ringing tone is 425 Hz. So the logs say who heard whom, and when.

## The calls

- Outbound over UDP: busy (486), ring-out (our own ring timeout), answered then hung up by the far end.
- A warm-transfer room: a customer leg, a consult leg created with `tilbyderen.consult_mute` naming the customer, and a control leg without it. Each far end has its own tone.
- The same three outbound calls over TCP, then three at once.
- One inbound call. The caller is alone in its room for three and a half
  seconds, then a second leg is brought into the room, and seven and a half
  seconds later the caller hangs up.
- Seven dials that wait for the answer (`wait_until_answered`), with the options
  the application sends for manual and consult legs: busy, unavailable (480),
  carrier failure (503), declined (603), a ring-out, an answered call, and an
  answered consult leg with a dial tone.
- 40 busy calls over TCP to a second stand-in carrier that replies with no
  delay at all (the first one waits 5 ms, as any real network would).

Then one more call is answered and left running, and the image gets SIGTERM
while that call is live, as a drain would send. A new dial is attempted during
the drain.

## What is checked (`check.py`)

- **REGISTER:** both test logins register with credentials; the `livekit_sip_registrations_active` metric shows them; on SIGTERM both are signed out with credentials and the process exits 0.
- **Contact header:** the user part is on the outbound INVITE and on the inbound 180 and 200.
- **Inbound, answered at once:** 200 OK within a second (`skip_inbound_subscribe_wait`).
- **Inbound, ringing tone, in sound:** the caller hears 425 Hz while alone in the room; the tone is gone within a second of the second leg arriving; from then on the caller hears that leg's far end and that far end hears the caller; the caller never hears itself. The tone is played once, for the inbound call only, and it stops because the other party arrived.
- **Hang-up code:** `participant_left` carries `sip.statusCode` and `sip.status` with the carrier's code and wording for 486, 480, 503 and 603, and 408 with our own wording for a ring-out; an answered call carries none, and carries `lk.sip.answerTime`.
- **Warm-transfer guard:** the muted consult leg does not subscribe to the customer and the control leg does; and in sound, the muted leg's far end never hears the customer's tone, while it does hear the control leg's, and the control leg's far end hears the customer.
- **TCP:** every TCP call shares one connection; CANCEL and BYE travel over it.
- **Dials that wait for the answer:** a rejected dial fails with the carrier's code in the error itself (`sip_status_code` in the Twirp error metadata, which the server SDK turns into a `SipCallError`); a ring-out fails when the ring time is up, with no SIP code; an answered dial returns right after the answer. The application's circuit breaker and its manual-call and transfer results are built on exactly this, and v1.17.0 and v1.25.0 answer identically.
- **TCP with an instant carrier:** none of the 40 calls ends as "transaction failed to complete". Over TCP a transaction ends the instant its final response is delivered, and upstream's response wait could then pick "ended" over the response (upstream PR #995, carried in this fork). v1.25.0 without the fix loses about 15 % of such calls; a carrier more than about 2 ms away never triggers it.
- **Drain with a live call:** the logins are signed out within two seconds of the signal while the call is still up; the service waits for that call and exits 0 after the far end hangs up; the call's timeline ends normally; a new dial during the drain is refused.
- **Event stream:** entries for every call, no phone number in any entry, the busy / ring-out / answered timelines carry the right codes and reasons, exactly one `sip.call.ended` per call, nothing dropped.
- **The image itself:** it logs no error-level line during the whole run. The call extensions (`pkg/callext`) log one when a room cannot be tied to its call, which would switch three of the behaviours above off.

## What it cannot check

The real carrier. Telavox's own behaviour (its challenge, what it does with our
Contact header, how fast it stops routing after an un-REGISTER), the quality of
real speech over a real network, and load are only seen in production. The
sound here is sine waves between containers on one machine: it proves that
audio is connected the right way round and at the right moment, not how it
sounds.
