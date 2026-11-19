// Package callext is a Tilbyderen fork extension: the per-call behaviour this
// fork adds to LiveKit's SIP service, plugged into the call manager instead of
// edited into its files.
//
// It uses two plug-in points of sip.NewCallManager:
//
//   - the per-call state handler (the GetStateHandler argument). It tells us
//     what every call is (its id, its direction) and how it ended, and it is
//     where the call event stream (pkg/eventsink) listens;
//   - the room (sip.WithGetRoom). Every call reaches its LiveKit room through
//     a sip.RoomInterface, and GetRoom hands out a wrapper around LiveKit's.
//
// What the wrapped room does, each behind the config key that names it:
//
//   - skip_inbound_subscribe_wait: an inbound call is answered at once,
//     without waiting for someone in the room to listen (see Subscribed);
//   - inbound_ringback_until_subscribed: the caller of such a call hears a
//     ringing tone until someone is in the room (see WriteOutboundAudioTo);
//   - always: an outbound leg that ended on a SIP final response leaves that
//     response as participant attributes before it leaves the room (see
//     CloseWithReason);
//   - always: a leg created with the tilbyderen.consult_mute attribute never
//     subscribes to the identities it lists (see guardConsult). This is the
//     one behaviour that needs a hook inside LiveKit's room, because only the
//     room sees a track being published: Room.SetSubscribeFilter in
//     pkg/sip/room.go.
//
// The two halves meet through the call id: the state handler is created with
// it, and the room learns it at Connect from the sip.callID attribute of its
// participant.
package callext

import (
	"sync"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/sip/pkg/config"
	"github.com/livekit/sip/pkg/eventsink"
	"github.com/livekit/sip/pkg/sip"
)

const (
	// callTTL is how long a call is remembered when its end is never reported.
	// LiveKit caps a call at 24 hours.
	callTTL = 25 * time.Hour
	// sweepEvery is how often remember looks for calls past callTTL.
	sweepEvery = 10 * time.Minute
)

// Extensions holds the calls in progress and hands out the two plug-ins.
type Extensions struct {
	conf *config.Config
	log  logger.Logger
	sink *eventsink.Sink // nil when the event stream is off

	mu        sync.Mutex
	calls     map[string]*call // by LiveKit SIP call id, from creation until sip.call.ended
	lastSweep time.Time
}

// New returns the extensions for one SIP service. sink may be nil.
func New(conf *config.Config, log logger.Logger, sink *eventsink.Sink) *Extensions {
	if log == nil {
		log = logger.GetLogger()
	}
	return &Extensions{
		conf:      conf,
		log:       log,
		sink:      sink,
		calls:     make(map[string]*call),
		lastSweep: time.Now(),
	}
}

// StateHandler wraps the state handler of one new call. Call it from the
// GetStateHandler function given to sip.NewCallManager.
func (e *Extensions) StateHandler(inner sip.StateHandler, initial *livekit.SIPCallInfo) sip.StateHandler {
	if inner == nil {
		return nil
	}
	inner = e.sink.Wrap(inner, initial)
	c := &call{
		id:      initial.GetCallId(),
		inbound: initial.GetCallDirection() == livekit.SIPCallDirection_SCD_INBOUND,
		created: time.Now(),
	}
	e.remember(c)
	h := &handler{StateHandler: inner, ext: e, call: c}
	h.events, _ = inner.(sip.SignalingEventHandler)
	return h
}

// GetRoom is a sip.GetRoomFunc: LiveKit's room, wrapped. Pass it to
// sip.WithGetRoom.
func (e *Extensions) GetRoom(log logger.Logger, st *sip.RoomStats) sip.RoomInterface {
	return newRoom(e, sip.DefaultGetRoomFunc(log, st), log)
}

func (e *Extensions) remember(c *call) {
	if c.id == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls[c.id] = c
	if now := time.Now(); now.Sub(e.lastSweep) >= sweepEvery {
		e.lastSweep = now
		for id, old := range e.calls {
			if now.Sub(old.created) > callTTL {
				delete(e.calls, id)
			}
		}
	}
}

// lookup returns the call in progress with this id, or nil.
func (e *Extensions) lookup(id string) *call {
	if id == "" {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls[id]
}

// forget drops a call that has ended. Its room keeps its own pointer.
func (e *Extensions) forget(c *call) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.calls[c.id] == c {
		delete(e.calls, c.id)
	}
}
