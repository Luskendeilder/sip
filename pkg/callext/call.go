package callext

import (
	"sync"
	"time"

	"github.com/livekit/protocol/livekit"

	"github.com/livekit/sip/pkg/sip"
)

const (
	// AttrSIPStatusCode and AttrSIPStatus are the participant attributes an
	// outbound leg leaves behind when it ended on a SIP final response: the
	// code ("486") and its wording ("Busy Here"). The participant_left webhook
	// carries them, which is how the application learns why a call it did not
	// wait for was not answered.
	AttrSIPStatusCode = livekit.AttrSIPPrefix + "statusCode"
	AttrSIPStatus     = livekit.AttrSIPPrefix + "status"

	// reasonNoAnswer is the termination reason of an outbound call that our
	// own ring timeout ended: no final response arrived in time.
	reasonNoAnswer = "no-answer"
	// ringTimeoutCode and ringTimeoutText are what such a leg leaves behind.
	// 408 is the RFC 3261 code for exactly that, so the application can tell a
	// plain ring-out from a 480 (switched off, no coverage).
	ringTimeoutCode = 408
	ringTimeoutText = "Request Timeout (ring timeout)"
)

// sipStatus is a SIP final response: its code and its wording.
type sipStatus struct {
	code int
	text string
}

// call is what the extensions know about one call. The state handler writes
// it, the room reads it.
type call struct {
	id      string
	inbound bool
	created time.Time

	mu       sync.Mutex
	recorded sipStatus // the final response in the call record, if the INVITE failed with one
	final    sipStatus // what the leg leaves behind; decided when the call ends
}

// record keeps the final response LiveKit put in the call record. It does so
// exactly when an outbound INVITE fails with a SIP response.
func (c *call) record(st *livekit.SIPStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recorded = sipStatus{code: int(st.GetCode()), text: st.GetStatus()}
}

// end decides what an outbound leg leaves behind, from the sip.call.ended
// event and the call record before it.
func (c *call) end(ev sip.SignalingEvent) {
	if c.inbound {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.recorded.code >= 300:
		c.final = c.recorded
	case ev.Reason == reasonNoAnswer:
		c.final = sipStatus{code: ringTimeoutCode, text: ringTimeoutText}
	}
}

// finalStatus returns what the leg leaves behind, if anything.
func (c *call) finalStatus() (sipStatus, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.final, c.final.code != 0
}

// handler is the per-call state handler. LiveKit invokes it while the call
// state holds its mutex, so it only takes notes and passes everything on.
type handler struct {
	sip.StateHandler                           // LiveKit's handler, or the event stream's around it
	events           sip.SignalingEventHandler // the same, when it takes signaling events
	ext              *Extensions
	call             *call
}

func (h *handler) HandleUpdate(info *livekit.SIPCallInfo) {
	h.StateHandler.HandleUpdate(info)
	if st := info.GetCallStatusCode(); st != nil {
		h.call.record(st)
	}
}

func (h *handler) HandleSignalingEvent(ev sip.SignalingEvent) {
	if h.events != nil {
		h.events.HandleSignalingEvent(ev)
	}
	if ev.Type == sip.EventCallEnded {
		h.call.end(ev)
		h.ext.forget(h.call)
	}
}
