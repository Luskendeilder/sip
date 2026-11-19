package eventsink

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"

	"github.com/livekit/sip/pkg/config"
	"github.com/livekit/sip/pkg/sip"
)

// recorder is the inner handler: it counts what reaches it.
type recorder struct {
	updates, transfers, contexts, events atomic.Int32
}

func (r *recorder) HandleUpdate(*livekit.SIPCallInfo)              { r.updates.Add(1) }
func (r *recorder) HandleTransfer(*livekit.SIPTransferInfo)        { r.transfers.Add(1) }
func (r *recorder) HandleCallContextRecorded(*livekit.SIPCallInfo) { r.contexts.Add(1) }

// eventRecorder also accepts signaling events, like a future upstream handler might.
type eventRecorder struct{ recorder }

func (r *eventRecorder) HandleSignalingEvent(sip.SignalingEvent) { r.events.Add(1) }

// stream collects what the sink writes.
type stream struct {
	mu      sync.Mutex
	entries []map[string]any
}

func (st *stream) do(_ context.Context, args ...any) error {
	// XADD key MAXLEN ~ n * field value ...
	fields := map[string]any{"_cmd": args[0], "_key": args[1], "_maxlen": args[4]}
	for i := 6; i+1 < len(args); i += 2 {
		fields[args[i].(string)] = args[i+1]
	}
	st.mu.Lock()
	st.entries = append(st.entries, fields)
	st.mu.Unlock()
	return nil
}

func (st *stream) wait(t *testing.T, n int) []map[string]any {
	t.Helper()
	require.Eventually(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.entries) >= n
	}, 3*time.Second, 5*time.Millisecond)
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]map[string]any(nil), st.entries...)
}

func decode(t *testing.T, e map[string]any) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(e["data"].([]byte), &out))
	return out
}

func counter(c prometheus.Counter) float64 {
	var m dto.Metric
	_ = c.Write(&m)
	return m.GetCounter().GetValue()
}

func enabled() config.EventStreamConfig {
	return config.EventStreamConfig{Enabled: true, Key: "test:events", MaxLen: 1000, AttributePrefix: "tb."}
}

func outboundCall() *livekit.SIPCallInfo {
	return &livekit.SIPCallInfo{
		CallId:              "SCL_out",
		SipCallId:           "abc@203.0.113.7",
		TrunkId:             "ST_trunk",
		CallDirection:       livekit.SIPCallDirection_SCD_OUTBOUND,
		CallStatus:          livekit.SIPCallStatus_SCS_ACTIVE,
		RoomName:            "inbound-org_+4712345678_x",
		RoomId:              "RM_room",
		ParticipantIdentity: "sip_+4712345678",
		FromUri:             &livekit.SIPUri{User: "+4712345678", Host: "example.com"},
		ToUri:               &livekit.SIPUri{User: "+4712345678", Host: "carrier.example"},
		CreatedAtNs:         1_700_000_000_000_000_000,
		ParticipantAttributes: map[string]string{
			"tb.callId":       "call-1",
			"tb.orgId":        "org-1",
			"sip.phoneNumber": "+4712345678",
		},
	}
}

func TestDisabledSinkIsNil(t *testing.T) {
	st := &stream{}
	require.Nil(t, New(config.EventStreamConfig{}, "NE_x", nil, st.do))
	require.Nil(t, New(enabled(), "NE_x", nil, nil))

	var s *Sink
	inner := &recorder{}
	require.Same(t, sip.StateHandler(inner), s.Wrap(inner, outboundCall()))
	s.publish(KindEvent, []byte("{}"))
	s.Close()
}

func TestUnmatchedInboundIsNotWrapped(t *testing.T) {
	st := &stream{}
	s := New(enabled(), "NE_x", nil, st.do)
	defer s.Close()
	inner := &recorder{}

	scanner := &livekit.SIPCallInfo{CallId: "SCL_scan", CallDirection: livekit.SIPCallDirection_SCD_INBOUND}
	require.Same(t, sip.StateHandler(inner), s.Wrap(inner, scanner))

	matched := &livekit.SIPCallInfo{CallId: "SCL_in", TrunkId: "ST_in", CallDirection: livekit.SIPCallDirection_SCD_INBOUND}
	require.NotSame(t, sip.StateHandler(inner), s.Wrap(inner, matched))
}

func TestCallRecord(t *testing.T) {
	st := &stream{}
	s := New(enabled(), "NE_node", nil, st.do)
	defer s.Close()
	inner := &recorder{}
	info := outboundCall()
	h := s.Wrap(inner, info)

	info.CallStatus = livekit.SIPCallStatus_SCS_ERROR
	info.CallStatusCode = &livekit.SIPStatus{Code: livekit.SIPStatusCode_SIP_STATUS_BUSY_HERE, Status: "Busy Here"}
	info.DisconnectReason = livekit.DisconnectReason_USER_REJECTED
	info.EndedAtNs = 1_700_000_030_000_000_000
	h.HandleUpdate(info)
	h.HandleTransfer(nil)
	h.HandleCallContextRecorded(info)

	require.EqualValues(t, 1, inner.updates.Load(), "the update still reaches the inner handler")
	require.EqualValues(t, 1, inner.transfers.Load())
	require.EqualValues(t, 1, inner.contexts.Load())

	entries := st.wait(t, 1)
	require.Len(t, entries, 1)
	e := entries[0]
	require.Equal(t, "XADD", e["_cmd"])
	require.Equal(t, "test:events", e["_key"])
	require.EqualValues(t, 1000, e["_maxlen"])
	require.Equal(t, SchemaVersion, e["v"])
	require.Equal(t, KindCall, e["kind"])

	// No phone number leaves through the stream.
	require.NotContains(t, string(e["data"].([]byte)), "4712345678")

	rec := decode(t, e)
	require.Equal(t, "NE_node", rec["nodeId"])
	require.Equal(t, "SCL_out", rec["callId"])
	require.Equal(t, "abc@203.0.113.7", rec["sipCallId"])
	require.Equal(t, "outbound", rec["direction"])
	require.Equal(t, "ST_trunk", rec["trunkId"])
	require.Equal(t, "RM_room", rec["roomId"])
	require.Equal(t, "SCS_ERROR", rec["callStatus"])
	require.EqualValues(t, 486, rec["statusCode"])
	require.Equal(t, "Busy Here", rec["statusText"])
	require.Equal(t, "USER_REJECTED", rec["disconnectReason"])
	require.EqualValues(t, 1_700_000_000_000, rec["createdAtMs"])
	require.EqualValues(t, 1_700_000_030_000, rec["endedAtMs"])
	require.Equal(t, map[string]any{"tb.callId": "call-1", "tb.orgId": "org-1"}, rec["attrs"])
	require.NotContains(t, rec, "startedAtMs")
}

func TestEventRecord(t *testing.T) {
	st := &stream{}
	s := New(enabled(), "NE_node", nil, st.do)
	defer s.Close()
	inner := &eventRecorder{}
	h := s.Wrap(inner, outboundCall())

	at := time.UnixMilli(1_700_000_012_345)
	h.(sip.SignalingEventHandler).HandleSignalingEvent(sip.SignalingEvent{
		Type:       sip.EventCancelSent,
		Time:       at,
		CallID:     "SCL_out",
		SIPCallID:  "abc@203.0.113.7",
		Direction:  livekit.SIPCallDirection_SCD_OUTBOUND,
		TrunkID:    "ST_trunk",
		RoomID:     "RM_room",
		SIPMethod:  "CANCEL",
		StatusCode: 0,
		CSeq:       2,
		Transport:  sip.TransportTCP,
		Reason:     "timeout",
		Duration:   1500 * time.Millisecond,
	})
	require.EqualValues(t, 1, inner.events.Load(), "an inner handler that takes events still gets them")

	entries := st.wait(t, 1)
	e := entries[0]
	require.Equal(t, KindEvent, e["kind"])
	rec := decode(t, e)
	require.Equal(t, "sip.cancel.sent", rec["type"])
	require.EqualValues(t, 1_700_000_012_345, rec["timeMs"])
	require.Equal(t, "SCL_out", rec["callId"])
	require.Equal(t, "outbound", rec["direction"])
	require.Equal(t, "CANCEL", rec["method"])
	require.EqualValues(t, 2, rec["cseq"])
	require.Equal(t, "tcp", rec["transport"])
	require.Equal(t, "timeout", rec["reason"])
	require.EqualValues(t, 1500, rec["durationMs"])
	require.Equal(t, map[string]any{"tb.callId": "call-1", "tb.orgId": "org-1"}, rec["attrs"])
	require.NotContains(t, rec, "status", "a zero status is left out")
}

// Inbound calls get their attributes after dispatch: events after the update carry them.
func TestInboundAttributesArriveLater(t *testing.T) {
	st := &stream{}
	s := New(enabled(), "NE_node", nil, st.do)
	defer s.Close()
	info := &livekit.SIPCallInfo{CallId: "SCL_in", TrunkId: "ST_in", CallDirection: livekit.SIPCallDirection_SCD_INBOUND}
	h := s.Wrap(&recorder{}, info)
	ev := sip.SignalingEvent{Type: sip.EventInviteReceived, Time: time.UnixMilli(1), CallID: "SCL_in", Direction: livekit.SIPCallDirection_SCD_INBOUND}

	h.(sip.SignalingEventHandler).HandleSignalingEvent(ev)
	info.ParticipantAttributes = map[string]string{"tb.orgId": "org-9", "other": "x"}
	h.HandleUpdate(info)
	h.(sip.SignalingEventHandler).HandleSignalingEvent(ev)

	entries := st.wait(t, 3)
	require.NotContains(t, decode(t, entries[0]), "attrs")
	require.Equal(t, map[string]any{"tb.orgId": "org-9"}, decode(t, entries[1])["attrs"])
	require.Equal(t, map[string]any{"tb.orgId": "org-9"}, decode(t, entries[2])["attrs"])
}

func TestEntriesKeepTheirOrder(t *testing.T) {
	st := &stream{}
	s := New(enabled(), "NE_node", nil, st.do)
	defer s.Close()
	h := s.Wrap(&recorder{}, outboundCall()).(sip.SignalingEventHandler)
	const n = 200
	for i := 0; i < n; i++ {
		h.HandleSignalingEvent(sip.SignalingEvent{Type: sip.EventInviteSent, Time: time.UnixMilli(int64(i + 1)), CSeq: uint32(i + 1)})
	}
	entries := st.wait(t, n)
	for i, e := range entries {
		require.EqualValues(t, i+1, decode(t, e)["cseq"])
	}
}

// A slow or dead Redis must never hold up a call: publish returns at once and
// entries beyond the queue are dropped and counted.
func TestPublishNeverBlocks(t *testing.T) {
	release := make(chan struct{})
	var wrote atomic.Int32
	s := New(enabled(), "NE_node", nil, func(context.Context, ...any) error {
		<-release
		wrote.Add(1)
		return nil
	})
	dropped := counter(s.entries.WithLabelValues("dropped_full"))

	start := time.Now()
	for i := 0; i < queueSize+500; i++ {
		s.publish(KindEvent, []byte("{}"))
	}
	require.Less(t, time.Since(start), time.Second, "publish must not wait for Redis")
	require.GreaterOrEqual(t, counter(s.entries.WithLabelValues("dropped_full"))-dropped, float64(499))

	close(release)
	s.Close()
	require.Greater(t, wrote.Load(), int32(0))
}

func TestCloseWritesWhatIsQueued(t *testing.T) {
	st := &stream{}
	s := New(enabled(), "NE_node", nil, st.do)
	for i := 0; i < 50; i++ {
		s.publish(KindEvent, []byte("{}"))
	}
	s.Close()
	s.Close() // twice is fine
	require.Len(t, st.wait(t, 50), 50)

	// After Close nothing more is accepted, and nothing panics.
	closed := counter(s.entries.WithLabelValues("dropped_closed"))
	s.publish(KindEvent, []byte("{}"))
	require.Equal(t, closed+1, counter(s.entries.WithLabelValues("dropped_closed")))
}

func TestAppendRetriesThenDrops(t *testing.T) {
	var calls atomic.Int32
	s := New(enabled(), "NE_node", nil, func(context.Context, ...any) error {
		if calls.Add(1) < 3 {
			return errors.New("redis is away")
		}
		return nil
	})
	published := counter(s.entries.WithLabelValues("published"))
	s.publish(KindEvent, []byte("{}"))
	require.Eventually(t, func() bool {
		return counter(s.entries.WithLabelValues("published")) == published+1
	}, 3*time.Second, 10*time.Millisecond)
	require.EqualValues(t, 3, calls.Load(), "two failures, then success")
	s.Close()

	failing := New(enabled(), "NE_node", nil, func(context.Context, ...any) error { return errors.New("redis is away") })
	lost := counter(failing.entries.WithLabelValues("dropped_error"))
	failing.publish(KindEvent, []byte("{}"))
	require.Eventually(t, func() bool {
		return counter(failing.entries.WithLabelValues("dropped_error")) == lost+1
	}, 3*time.Second, 10*time.Millisecond)
	failing.Close()
}
