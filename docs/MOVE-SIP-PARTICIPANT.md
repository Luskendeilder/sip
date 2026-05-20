# MoveSIPParticipant — fork extension

## Why this fork exists

LiveKit OSS server (`livekit/livekit`) returns `errors.New("not implemented")`
for `MoveParticipant` and `ForwardParticipant` in
`pkg/service/roommanager.go`. The implementation only ships in LiveKit Cloud.

Tilbyderen needs to move SIP participants between rooms for:

- Cold transfer to internal user (move customer SIP from `shift-{A}` → `shift-{B}`)
- Warm transfer commit step (move customer SIP from `shift-{A}` → `shift-{B}`)
- AMD pre-bridge handoff (move customer SIP from `predial-{callId}` → `shift-{agent}`)
- Inbound DID dispatch (move customer SIP from `inbound-*` → `shift-{agent}`)

Every move is of a **SIP participant**, on a **single node**, with the
**carrier-side SIP/RTP session preserved**. The carrier never sees a hangup.

That scope is much narrower than what LiveKit Cloud's MoveParticipant does
(which handles WebRTC participants across distributed SFU nodes). It's
achievable inside `livekit/sip` alone, without touching `livekit/livekit`
core or `livekit/protocol`.

## What the fork adds

A single new HTTP endpoint on the existing health server:

```
POST http://<sip-host>:<health-port>/admin/move-sip-participant
Content-Type: application/json

{
  "sip_call_id":      "SCL_kC3rZiCMf6NZ",
  "destination_room": "shift-ODWiJQjV9HwyUp8CnEkmVRVez9B42s3J",
  "destination_token": "<lk JWT>"
}
```

We deliberately chose an HTTP/JSON endpoint over extending the Twirp
`SIPInternalServer` because:

- Adding a method to the Twirp service requires forking `livekit/protocol`,
  regenerating Go bindings, and wiring a `go.mod` replace directive — 3
  extra repos and a much larger maintenance surface.
- Our Node server already speaks HTTP/JSON fluently and doesn't need
  Twirp client code for this one method.
- Single endpoint, one handler, one JSON schema — easy to maintain.

## Implementation plan

### 1. HTTP handler (~30 LOC)

`pkg/service/service.go` — extend the existing `healthServer`'s mux:

```go
mux.HandleFunc("/admin/move-sip-participant", s.handleMoveSIPParticipant)
```

Handler validates JSON, looks up the call worker by SIP call ID in either
`client.activeCalls` (outbound) or `server.byLocalTag` (inbound), and
delegates to a new `(*outboundCall).SwapRoom` / `(*inboundCall).SwapRoom`.

### 2. Call worker `SwapRoom` (~50 LOC each, both directions)

`pkg/sip/outbound.go` and `pkg/sip/inbound.go` — new method on the call
worker that:

1. Acquires the call's existing `mu` mutex to serialize against hangup +
   REFER.
2. Verifies the call is in an active state (not closing/REFER/hung-up).
3. Calls `r.lkRoom.SwapToRoom(newRoomName, newToken)` on the embedded
   `Room`.
4. Updates internal logging context with the new room name.
5. Releases mutex.

### 3. Room swap logic (~150 LOC, the hard part)

`pkg/sip/room.go` — new method `(*Room).SwapToRoom(newRoomName, newToken string)`:

1. Acquire the Room's mutex.
2. Save reference to the existing PCM16Writer (the one feeding the SIP RTP
   output via `SwapOutput`). The mixer + this writer represent the audio
   path TO the carrier; we want to keep it alive across the swap.
3. Disconnect the current `lksdk.Room` with reason `CLIENT_INITIATED`.
   This triggers `OnDisconnected` callbacks; suppress their state mutations
   during the swap window via a flag on the Room struct.
4. Construct a new `lksdk.Room` with the same callback set (factored out
   into a helper that's already used by the constructor).
5. Connect to `newRoomName` with `newToken`.
6. Re-publish the audio track from the mixer's input side. The track's
   underlying RTP source is the same `media.PCM16Writer`; we just re-bind
   it to a new WebRTC sender in the new room.
7. The `subscribeTo` loop fires for existing tracks in the new room
   (handled by existing OnTrackPublished iteration at room.go:471-476).
8. Release the swap flag; re-enable normal disconnect handling.
9. Release Room mutex.

### 4. Edge cases

- **Identity collision**: if `newRoomName` already has a participant with
  the same identity, the LK server rejects the connect. Handler returns
  409 Conflict.
- **Concurrent move on same call**: serialized by the call's `mu` mutex;
  second caller waits.
- **Hangup during move**: hangup acquires the same mutex; one wins. If
  hangup runs first, the move handler sees a closed call and returns
  410 Gone.
- **Audio glitch**: ~100-300ms of silence on the SIP output is expected
  during the disconnect+reconnect window. Buffer with silence pad (use
  existing `silence_filler.go`).
- **Token expiry**: caller is responsible for issuing a fresh token with
  the right room grant. Handler does no token minting.

### 5. Build + deploy

Existing `Dockerfile` works unmodified. Tag scheme:

```
ghcr.io/luskendeilder/livekit-sip:v1.3.0-fork.1
```

`v1.3.0` = the upstream base; `fork.1` = our patch revision.

Build:

```bash
cd ../livekit-sip-fork
docker build -t ghcr.io/luskendeilder/livekit-sip:v1.3.0-fork.1 -f build/Dockerfile .
docker push ghcr.io/luskendeilder/livekit-sip:v1.3.0-fork.1
```

Update `Tilbyderen-web-enkel/infra/livekit/docker-compose.yml` to reference
the forked image. Deploy via the existing rack deploy script.

### 6. Integration in Tilbyderen

`apps/server/src/services/telephony/livekit/sip.ts` — add:

```ts
export async function moveSipParticipant(opts: {
  sipCallId: string;
  destinationRoom: string;
  destinationToken: string;
}): Promise<void> {
  const url = `http://${SIP_HOST}:${SIP_HEALTH_PORT}/admin/move-sip-participant`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(opts),
  });
  if (!res.ok) throw new Error(`move-sip-participant failed: ${res.status}`);
}
```

Then restore the original MoveParticipant-based transfer flows in
`transfer.ts`, but route via `moveSipParticipant` instead.

## Maintenance

- Subscribe to `livekit/sip` GitHub releases.
- Each upstream release: rebase `feature/move-sip-participant` onto the
  new tag, resolve conflicts (typically only in files we touched: the
  service.go health-server mux + room.go).
- Re-tag the image: `v<upstream>-fork.<patch_n>`.
- If upstream ever ships `MoveSIPParticipant` natively (subscribe to
  `livekit/sip` issues for "move participant" mentions), delete this
  fork.

## Status

- [x] Repo forked: `Luskendeilder/sip`
- [x] Local clone: `c:/Users/filip/Documents/Cursor prosjekter/livekit-sip-fork`
- [x] Feature branch: `feature/move-sip-participant`
- [x] `Room.SwapToRoom` (room.go) — disconnect old lksdk.Room, reconnect via existing Connect, suppressing the stopped fuse via a `swapping` atomic flag
- [x] `outboundCall.SwapRoom` (outbound.go) — orchestrates audio detach + Room swap + re-publish + connectMedia + Subscribe
- [x] `inboundCall.SwapRoom` (inbound.go) — mirror, uses simpler media wiring (media owns audioIn, no struct-cached writer)
- [x] `Client.GetActiveCall` + `Server.GetInboundCall` — public lookup by SIP call ID
- [x] `sip.Service.MoveSIPParticipant` — orchestrator that finds the call in either registry and delegates
- [x] HTTP handler at `POST /admin/move-sip-participant` wired to the orchestrator
- [x] Function pointer plumbing through `service.Service` from `main.go`
- [ ] Tests
- [ ] Docker image built + pushed to GHCR
- [ ] Integrated in `apps/server/src/services/telephony/livekit/sip.ts`
- [ ] Old MoveParticipant-based transfer flows restored in our Node code

## Design notes captured during implementation

**SDK already supports being moved.** [lksdk-go's room.go:1290](https://github.com/livekit/server-sdk-go/blob/main/room.go#L1290) has `OnRoomMoved` that handles a `RoomMovedResponse` signal — this is how Cloud's SFU triggers the move. We can't trigger that signal from the OSS server side (the SFU's MoveParticipant returns "not implemented"), so we explicitly `Disconnect + JoinWithContextAndToken` from the gateway side. Net effect from the SFU's POV is the same: participant leaves room A, joins room B.

**What survives the swap, what doesn't.** Survives: the `Room` Go struct, its `mix` (mixer), its `out` (SwitchWriter feeding the carrier's RTP encoder), the SIP carrier UDP socket, the call worker's mutex + state. Doesn't survive: the `lksdk.Room` itself (a new one is built), the local audio track (must be re-published via `NewParticipantTrack`), the subscribed-track decoder goroutines (rebuild on `Subscribe()` in the new room), the `ready`/`subscribed` fuses (reset to fresh `core.Fuse{}`).

**Why we suppress only the OUT going disconnect.** The `swapping` flag on Room is checked inside `OnDisconnectedWithReason`. It's set BEFORE we `DisconnectWithReason()` the outgoing lksdk.Room and cleared by `defer` in `SwapToRoom`. The new lksdk.Room's callback (created fresh in Connect) captures `r` and reads `r.swapping` at fire time; since the flag is back to false by then, normal disconnect handling resumes.

**Audio gap.** Disconnect → new room join is the bound on the audio gap. lksdk does a fresh WebSocket signal connect + WebRTC PeerConnection negotiation. Observed elsewhere as ~100-300ms; carrier hears silence in both directions during that window. SIP carrier never sees a BYE — its leg is preserved end-to-end.

**Why a `started` precondition.** Both `SwapRoom` impls reject calls where `started` isn't broken — i.e. before the SIP call is established. Moving a half-connected call would leak resources because the connect path isn't idempotent against the partial state.
