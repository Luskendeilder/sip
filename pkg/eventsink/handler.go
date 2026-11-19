package eventsink

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/livekit/protocol/livekit"

	"github.com/livekit/sip/pkg/sip"
)

// eventRecord is the "data" of a stream entry of kind "event": one signaling
// step of one call. Times are Unix milliseconds on this node's clock.
type eventRecord struct {
	// Type is the sip.SignalingEventType, e.g. "sip.invite.sent",
	// "sip.provisional.received", "sip.answered.received", "sip.failed",
	// "sip.cancel.sent", "sip.bye.received", "sip.bye.sent", "sip.call.ended".
	Type string `json:"type"`
	// TimeMs is when the step happened (not when it was written).
	TimeMs int64  `json:"timeMs"`
	NodeID string `json:"nodeId"`
	// CallID is the LiveKit SIP call id (SCL_...), the value of the sip.callID
	// participant attribute. SIPCallID is the SIP Call-ID header
	// (sip.callIDFull).
	CallID    string `json:"callId"`
	SIPCallID string `json:"sipCallId,omitempty"`
	// Direction is "inbound" or "outbound".
	Direction      string `json:"direction"`
	TrunkID        string `json:"trunkId,omitempty"`
	RoomID         string `json:"roomId,omitempty"`
	DispatchRuleID string `json:"dispatchRuleId,omitempty"`
	ParticipantID  string `json:"participantId,omitempty"`
	Method         string `json:"method,omitempty"`
	// Status is the SIP status code of the response the event is about. On
	// "sip.call.ended" it is the final response to the INVITE (200 for every
	// answered call, 0 when there was none, e.g. our own cancel).
	Status    int    `json:"status,omitempty"`
	CSeq      uint32 `json:"cseq,omitempty"`
	Transport string `json:"transport,omitempty"`
	// Reason: why the step happened. "sip.cancel.sent": "timeout" (our ring
	// timeout), "hangup" or "shutdown". "sip.failed" and "sip.bye.received":
	// the cause of the Reason header (e.g. "q.850;cause=16"), never its free
	// text. "sip.call.ended": the termination reason used in the metrics.
	Reason string `json:"reason,omitempty"`
	// Result: the outcome of the step. On "sip.call.ended" one of success,
	// client_error, server_error, upstream_error, indeterminate.
	Result     string `json:"result,omitempty"`
	DurationMs int64  `json:"durationMs,omitempty"`
	WithPIN    bool   `json:"withPin,omitempty"`
	// Attrs are the participant attributes of the call whose key starts with
	// the configured attribute_prefix.
	Attrs map[string]string `json:"attrs,omitempty"`
}

// callRecord is the "data" of a stream entry of kind "call": the state of
// the per-call record (livekit.SIPCallInfo) after one update. The last entry
// of a call has EndedAtMs set.
type callRecord struct {
	// TimeMs is when this update was handed to the sink.
	TimeMs         int64  `json:"timeMs"`
	NodeID         string `json:"nodeId"`
	CallID         string `json:"callId"`
	SIPCallID      string `json:"sipCallId,omitempty"`
	Direction      string `json:"direction"`
	TrunkID        string `json:"trunkId,omitempty"`
	DispatchRuleID string `json:"dispatchRuleId,omitempty"`
	RoomID         string `json:"roomId,omitempty"`
	// CallStatus is the livekit.SIPCallStatus name: SCS_CALL_INCOMING,
	// SCS_PARTICIPANT_JOINED, SCS_ACTIVE, SCS_DISCONNECTED or SCS_ERROR.
	CallStatus string `json:"callStatus"`
	// StatusCode and StatusText are the final SIP response of a failed INVITE.
	StatusCode int    `json:"statusCode,omitempty"`
	StatusText string `json:"statusText,omitempty"`
	// DisconnectReason is the livekit.DisconnectReason name, when one is set.
	DisconnectReason string            `json:"disconnectReason,omitempty"`
	Error            string            `json:"error,omitempty"`
	CreatedAtMs      int64             `json:"createdAtMs,omitempty"`
	StartedAtMs      int64             `json:"startedAtMs,omitempty"`
	EndedAtMs        int64             `json:"endedAtMs,omitempty"`
	AudioCodec       string            `json:"audioCodec,omitempty"`
	Attrs            map[string]string `json:"attrs,omitempty"`
}

// Wrap returns a handler that passes everything to inner and also publishes
// it to the sink. A nil Sink returns inner unchanged, and so does an inbound
// call that matched no trunk: those are internet scanners on the SIP port.
//
// initial is the call record at creation; for outbound calls it already
// carries the participant attributes of the request.
func (s *Sink) Wrap(inner sip.StateHandler, initial *livekit.SIPCallInfo) sip.StateHandler {
	if s == nil || inner == nil {
		return inner
	}
	if initial.GetCallDirection() == livekit.SIPCallDirection_SCD_INBOUND && initial.GetTrunkId() == "" {
		return inner
	}
	return &handler{
		StateHandler: inner,
		sink:         s,
		attrs:        s.pickAttrs(initial.GetParticipantAttributes()),
	}
}

// handler is the per-call wrapper. The call state invokes every method while
// holding its mutex, so the methods are serialized per call and must not
// block: they encode and hand off.
type handler struct {
	sip.StateHandler
	sink *Sink
	// attrs is the latest filtered attribute set of the call. Inbound calls
	// get their attributes after dispatch, so it is refreshed on each update.
	attrs map[string]string
}

func (h *handler) HandleUpdate(info *livekit.SIPCallInfo) {
	h.StateHandler.HandleUpdate(info)
	if info == nil {
		return
	}
	if attrs := h.sink.pickAttrs(info.GetParticipantAttributes()); attrs != nil {
		h.attrs = attrs
	}
	h.sink.publish(KindCall, h.sink.encodeCall(info, h.attrs))
}

func (h *handler) HandleSignalingEvent(ev sip.SignalingEvent) {
	if next, ok := h.StateHandler.(sip.SignalingEventHandler); ok {
		next.HandleSignalingEvent(ev)
	}
	h.sink.publish(KindEvent, h.sink.encodeEvent(ev, h.attrs))
}

// pickAttrs returns a copy of the attributes whose key starts with the
// configured prefix, or nil when there is no prefix or no match.
func (s *Sink) pickAttrs(attrs map[string]string) map[string]string {
	if s.attrPrefix == "" || len(attrs) == 0 {
		return nil
	}
	var out map[string]string
	for k, v := range attrs {
		if strings.HasPrefix(k, s.attrPrefix) {
			if out == nil {
				out = make(map[string]string)
			}
			out[k] = v
		}
	}
	return out
}

func (s *Sink) encodeEvent(ev sip.SignalingEvent, attrs map[string]string) []byte {
	rec := eventRecord{
		Type:           string(ev.Type),
		TimeMs:         ev.Time.UnixMilli(),
		NodeID:         s.nodeID,
		CallID:         ev.CallID,
		SIPCallID:      ev.SIPCallID,
		Direction:      directionName(ev.Direction),
		TrunkID:        ev.TrunkID,
		RoomID:         ev.RoomID,
		DispatchRuleID: ev.DispatchRuleID,
		ParticipantID:  ev.ParticipantID,
		Method:         ev.SIPMethod,
		Status:         ev.StatusCode,
		CSeq:           ev.CSeq,
		Transport:      string(ev.Transport),
		Reason:         ev.Reason,
		Result:         ev.Result,
		DurationMs:     ev.Duration.Milliseconds(),
		WithPIN:        ev.WithPIN,
		Attrs:          attrs,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return nil
	}
	return data
}

func (s *Sink) encodeCall(info *livekit.SIPCallInfo, attrs map[string]string) []byte {
	rec := callRecord{
		TimeMs:         time.Now().UnixMilli(),
		NodeID:         s.nodeID,
		CallID:         info.GetCallId(),
		SIPCallID:      info.GetSipCallId(),
		Direction:      directionName(info.GetCallDirection()),
		TrunkID:        info.GetTrunkId(),
		DispatchRuleID: info.GetDispatchRuleId(),
		RoomID:         info.GetRoomId(),
		CallStatus:     info.GetCallStatus().String(),
		Error:          info.GetError(),
		CreatedAtMs:    nsToMs(info.GetCreatedAtNs()),
		StartedAtMs:    nsToMs(info.GetStartedAtNs()),
		EndedAtMs:      nsToMs(info.GetEndedAtNs()),
		AudioCodec:     info.GetAudioCodec(),
		Attrs:          attrs,
	}
	if st := info.GetCallStatusCode(); st != nil {
		rec.StatusCode = int(st.GetCode())
		rec.StatusText = st.GetStatus()
	}
	if r := info.GetDisconnectReason(); r != livekit.DisconnectReason_UNKNOWN_REASON {
		rec.DisconnectReason = r.String()
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return nil
	}
	return data
}

func directionName(d livekit.SIPCallDirection) string {
	switch d {
	case livekit.SIPCallDirection_SCD_INBOUND:
		return "inbound"
	case livekit.SIPCallDirection_SCD_OUTBOUND:
		return "outbound"
	}
	return "unknown"
}

func nsToMs(ns int64) int64 {
	if ns <= 0 {
		return 0
	}
	return ns / int64(time.Millisecond)
}
