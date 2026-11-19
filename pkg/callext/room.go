package callext

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/media-sdk/tones"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/sip/pkg/config"
	"github.com/livekit/sip/pkg/sip"
)

const (
	// maxRingback bounds the ringing tone. Nothing should ring for longer than
	// the application keeps a caller queued (90 s today); this is a safety net.
	maxRingback = 10 * time.Minute
	// ringVolume is the amplitude of the ringing tone, as LiveKit's own
	// transfer tone uses.
	ringVolume = math.MaxInt16 / 2
	// statusEchoWait is how long a leaving leg waits for the server to confirm
	// its final SIP status attributes.
	statusEchoWait = 300 * time.Millisecond
	statusEchoPoll = 10 * time.Millisecond
)

// subscribedAlready is what Subscribed returns for a call that is answered at
// once: a channel that is always ready.
var subscribedAlready = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// attributes is the part of the room's local participant the final SIP status
// needs. *lksdk.LocalParticipant implements it.
type attributes interface {
	SetAttributes(attrs map[string]string)
	Attributes() map[string]string
}

// room is LiveKit's room with the fork's per-call behaviour around it. Every
// method not defined here is LiveKit's.
type room struct {
	sip.RoomInterface
	ext *Extensions
	log logger.Logger

	// participant returns the room's local participant, or nil before the room
	// is joined. A field so that a test can stand in for it.
	participant func() attributes

	mu             sync.Mutex
	call           *call     // set at Connect; nil when the call is not known
	answeredAtOnce bool      // LiveKit asked whether to wait for a subscriber, and was told not to
	attached       bool      // the leg's writer was handed over once; the tone is decided then
	ring           *ringback // the ringing tone, while it plays
	closed         bool
	statusOnce     sync.Once
}

func newRoom(ext *Extensions, inner sip.RoomInterface, log logger.Logger) *room {
	r := &room{RoomInterface: inner, ext: ext, log: log}
	r.participant = func() attributes {
		if lk := inner.Room(); lk != nil && lk.LocalParticipant != nil {
			return lk.LocalParticipant
		}
		return nil
	}
	return r
}

// ConsultMuteAttr is a participant attribute the application sets on the
// consult leg of a warm transfer: a comma-separated list of identities that
// leg must not hear. The leg never subscribes to their audio, so the customer
// stays inaudible to the transfer target until the agent completes the
// transfer and the application subscribes the target itself.
const ConsultMuteAttr = "tilbyderen.consult_mute"

// subscribeFilterer is LiveKit's room as far as the warm-transfer guard needs
// it: the one hook this fork keeps in pkg/sip/room.go.
type subscribeFilterer interface {
	SetSubscribeFilter(allow func(identity string) bool)
}

// guardConsult applies ConsultMuteAttr. It runs before the room is joined, so
// no track can be subscribed to before the guard is in place.
func (r *room) guardConsult(attrs map[string]string) {
	list := attrs[ConsultMuteAttr]
	if list == "" {
		return
	}
	muted := make(map[string]struct{})
	for _, id := range strings.Split(list, ",") {
		if id = strings.TrimSpace(id); id != "" {
			muted[id] = struct{}{}
		}
	}
	inner, ok := r.RoomInterface.(subscribeFilterer)
	if !ok {
		r.log.Errorw("call extensions: this room cannot filter subscriptions; the warm-transfer guard is off for it", nil,
			"attribute", ConsultMuteAttr, "value", list)
		return
	}
	inner.SetSubscribeFilter(func(identity string) bool {
		_, no := muted[identity]
		return !no
	})
}

// Connect joins the room. First it finds the call this room belongs to and
// puts the warm-transfer guard in place.
func (r *room) Connect(ctx context.Context, conf *config.Config, rconf sip.RoomConfig) error {
	r.guardConsult(rconf.Participant.Attributes)
	id := rconf.Participant.Attributes[livekit.AttrSIPCallID]
	c := r.ext.lookup(id)
	if c == nil {
		// Every call gets a state handler before it gets a room, and LiveKit
		// puts the call id in the participant's attributes. If either stops
		// being true, an outbound leg can no longer leave its final SIP status,
		// and that must be loud. Answering at once and the ringing tone do not
		// depend on it (see Subscribed).
		r.log.Errorw("call extensions: no call is known for this room participant; "+
			"the final SIP status cannot be left for it", nil,
			"attribute", livekit.AttrSIPCallID, "value", id)
	}
	r.mu.Lock()
	r.call = c
	r.mu.Unlock()
	return r.RoomInterface.Connect(ctx, conf, rconf)
}

// Subscribed is what LiveKit's inbound call waits on before it answers: it
// keeps the caller ringing until someone in the room listens to the call's
// track, and gives up when the dispatch rule's ring time runs out.
//
// With skip_inbound_subscribe_wait an inbound call does not wait. Telavox
// cancels an INVITE after 30 seconds, and a cancelled INVITE makes it send the
// next callers to voicemail for hours, which is far worse than a caller who
// waits on an answered line: the application routes the call, and the ringing
// tone below covers the wait. So the answer here is "ready".
//
// Only LiveKit's inbound call asks, and waiting is the one answer that must
// never be given by mistake. So a room whose call is not known is answered
// "ready" as well; only a call known to be outbound gets the room's own
// answer. The ringing tone reads the real channel, from the room underneath.
func (r *room) Subscribed() <-chan struct{} {
	if !r.ext.conf.SkipInboundSubscribeWait {
		return r.RoomInterface.Subscribed()
	}
	r.mu.Lock()
	outbound := r.call != nil && !r.call.inbound
	first := !outbound && !r.answeredAtOnce
	if !outbound {
		r.answeredAtOnce = true
	}
	r.mu.Unlock()
	if outbound {
		return r.RoomInterface.Subscribed()
	}
	if first {
		r.log.Infow("waitSubscribe: skipped (skip_inbound_subscribe_wait=true)")
	}
	return subscribedAlready
}

// WriteOutboundAudioTo is how a call connects the room's audio to its SIP leg:
// LiveKit's inbound call does it right after the call is answered.
//
// With inbound_ringback_until_subscribed, a call that was answered without
// waiting gets the ETSI ringing tone (425 Hz, one second on, four off) if
// nobody in the room is being listened to yet: the leg's writer is kept from
// the room and the tone is played into it instead. The moment the room has
// someone to listen to, the tone stops and the writer goes to the room. A
// caller on an answered line otherwise sits in silence while the application
// finds an agent, and hangs up.
//
// The tone is decided once, the first time a leg is handed over. A call that
// is answered before it is asked to wait (a PIN prompt), and a leg handed
// back later (LiveKit's own transfer), never ring.
//
// The room's mixer never writes to the leg while the tone plays, so its
// silence frames and the tone do not interleave. The tone is not published to
// the room: recordings and the agent hear nothing of it.
func (r *room) WriteOutboundAudioTo(w msdk.PCM16Writer) msdk.PCM16Writer {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rb := r.ring; rb != nil {
		// The output changes hands while the tone plays. To the caller of this
		// method the writer in place is the one the tone holds.
		r.ring = nil
		rb.stop()
		r.RoomInterface.WriteOutboundAudioTo(w)
		r.logStopped(rb, "output-changed")
		return rb.leg
	}
	first := w != nil && !r.attached
	if w != nil {
		r.attached = true
	}
	if !first || !r.shouldRingLocked() {
		return r.RoomInterface.WriteOutboundAudioTo(w)
	}
	old := r.RoomInterface.WriteOutboundAudioTo(nil)
	r.ring = r.startRingback(w)
	return old
}

func (r *room) shouldRingLocked() bool {
	if !r.ext.conf.InboundRingbackUntilSubscribed || !r.answeredAtOnce || r.closed {
		return false
	}
	select {
	case <-r.RoomInterface.Subscribed():
		return false // someone is already there; nothing to ring for
	default:
		return true
	}
}

// ringback is one playing of the ringing tone into a call's SIP leg.
type ringback struct {
	leg     msdk.PCM16Writer // the leg's writer, kept from the room while the tone plays
	started time.Time
	cancel  context.CancelFunc
	played  chan struct{} // closed when the player has written its last frame
}

// stop ends the tone and returns once no further frame can reach the leg, so
// that a late tone frame never lands on top of the agent's first words.
func (rb *ringback) stop() {
	rb.cancel()
	<-rb.played
}

func (r *room) startRingback(leg msdk.PCM16Writer) *ringback {
	ctx, cancel := context.WithCancel(context.Background())
	rb := &ringback{leg: leg, started: time.Now(), cancel: cancel, played: make(chan struct{})}
	go func() {
		defer close(rb.played)
		err := tones.Play(ctx, leg, ringVolume, tones.ETSIRinging)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.log.Infow("inbound ringback: cannot play tone", "error", err)
		}
	}()
	r.log.Infow("inbound ringback: started (until first subscribed track)")
	go func() {
		reason := "subscribed"
		select {
		case <-r.RoomInterface.Subscribed():
		case <-r.RoomInterface.Closed():
			reason = "room-closed"
		case <-time.After(maxRingback):
			reason = "max-duration"
		case <-ctx.Done():
			return // stopped by a change of output, or by the call closing
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.ring != rb {
			return
		}
		r.ring = nil
		rb.stop()
		r.RoomInterface.WriteOutboundAudioTo(rb.leg)
		r.logStopped(rb, reason)
	}()
	return rb
}

func (r *room) logStopped(rb *ringback, reason string) {
	r.log.Infow("inbound ringback: stopped", "reason", reason, "afterMs", time.Since(rb.started).Milliseconds())
}

func (r *room) Close() error {
	return r.CloseWithReason(livekit.DisconnectReason_UNKNOWN_REASON)
}

// CloseWithReason is how a call leaves its room.
//
// A ringing tone still playing is stopped, and the leg's writer goes back to
// the room, which closes it as it closes every writer it holds.
//
// An outbound leg that ended on a SIP final response first leaves that
// response behind as participant attributes (see leaveFinalStatus).
func (r *room) CloseWithReason(reason livekit.DisconnectReason) error {
	r.mu.Lock()
	r.closed = true
	if rb := r.ring; rb != nil {
		r.ring = nil
		rb.stop()
		r.RoomInterface.WriteOutboundAudioTo(rb.leg)
		r.logStopped(rb, "call-ended")
	}
	r.mu.Unlock()
	r.statusOnce.Do(r.leaveFinalStatus)
	return r.RoomInterface.CloseWithReason(reason)
}

// leaveFinalStatus publishes the SIP final response that ended an outbound leg
// as the participant attributes sip.statusCode and sip.status, and waits
// (bounded) for the server to confirm them.
//
// Why: LiveKit folds 486, 603 and 600 into one disconnect reason and 480 and
// 408 into another, and a dial that does not wait for the answer has returned
// long before the final response arrives. Without the attributes the code
// dies with the leg.
//
// Why the wait: setting attributes is fire-and-forget, and the leave that
// follows usually overtakes it. Measured in production on 18 August 2026, an
// attribute written right before the leave reached the participant_left
// webhook 5 times out of 199. The SDK updates its own copy of the attributes
// only when the server confirms them, so polling that copy tells us when it is
// safe to leave.
//
// Which calls: those LiveKit recorded a failed INVITE's response for, and
// those our own ring timeout ended (408). An answered call, however it ended,
// leaves nothing.
func (r *room) leaveFinalStatus() {
	r.mu.Lock()
	c := r.call
	r.mu.Unlock()
	if c == nil {
		return
	}
	st, ok := c.finalStatus()
	if !ok {
		return
	}
	p := r.participant()
	if p == nil {
		return // the room was never joined
	}
	code := strconv.Itoa(st.code)
	p.SetAttributes(map[string]string{
		AttrSIPStatusCode: code,
		AttrSIPStatus:     st.text,
	})
	deadline := time.Now().Add(statusEchoWait)
	for time.Now().Before(deadline) {
		if p.Attributes()[AttrSIPStatusCode] == code {
			return
		}
		time.Sleep(statusEchoPoll)
	}
	r.log.Debugw("final SIP status attribute not echoed before close", "code", code)
}
