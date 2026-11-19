package callext

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"

	"github.com/livekit/sip/pkg/config"
	"github.com/livekit/sip/pkg/sip"
)

// innerRoom stands in for LiveKit's room. A method the wrapper is not expected
// to call is left to the nil embedded interface, and panics.
type innerRoom struct {
	sip.RoomInterface

	mu         sync.Mutex
	subscribed chan struct{}
	gone       chan struct{}
	joined     bool
	out        msdk.PCM16Writer
	outAtClose msdk.PCM16Writer
	closes     []livekit.DisconnectReason
	connects   int
	filter     func(identity string) bool
}

func (f *innerRoom) SetSubscribeFilter(allow func(identity string) bool) { f.filter = allow }

func newInnerRoom() *innerRoom {
	return &innerRoom{subscribed: make(chan struct{}), gone: make(chan struct{}), joined: true}
}

func (f *innerRoom) Connect(context.Context, *config.Config, sip.RoomConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connects++
	return nil
}
func (f *innerRoom) Subscribed() <-chan struct{} { return f.subscribed }
func (f *innerRoom) Closed() <-chan struct{}     { return f.gone }
func (f *innerRoom) Room() *lksdk.Room {
	if !f.joined {
		return nil
	}
	return &lksdk.Room{} // only its being non-nil is read
}
func (f *innerRoom) WriteOutboundAudioTo(w msdk.PCM16Writer) msdk.PCM16Writer {
	f.mu.Lock()
	defer f.mu.Unlock()
	old := f.out
	f.out = w
	return old
}
func (f *innerRoom) CloseWithReason(reason livekit.DisconnectReason) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes = append(f.closes, reason)
	f.outAtClose = f.out
	return nil
}
func (f *innerRoom) output() msdk.PCM16Writer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.out
}

// leg stands in for the SIP leg's audio writer and counts what it is given.
type leg struct {
	rate   int // 8000 when zero
	frames atomic.Int64
	loud   atomic.Int64 // frames that are not silence
	closes atomic.Int64
}

func (l *leg) String() string { return "leg" }
func (l *leg) SampleRate() int {
	if l.rate == 0 {
		return 8000
	}
	return l.rate
}
func (l *leg) Close() error { l.closes.Add(1); return nil }
func (l *leg) WriteSample(s msdk.PCM16Sample) error {
	l.frames.Add(1)
	for _, v := range s {
		if v != 0 {
			l.loud.Add(1)
			break
		}
	}
	return nil
}

// participant stands in for the room's local participant. The server's echo
// arrives after echoAfter; a negative value means never.
type participant struct {
	mu        sync.Mutex
	set       []map[string]string
	echoed    map[string]string
	echoAfter time.Duration
}

func (p *participant) SetAttributes(attrs map[string]string) {
	p.mu.Lock()
	p.set = append(p.set, attrs)
	p.mu.Unlock()
	if p.echoAfter < 0 {
		return
	}
	time.AfterFunc(p.echoAfter, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.echoed = attrs
	})
}
func (p *participant) Attributes() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.echoed
}
func (p *participant) sets() []map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.set
}

type recorder struct {
	updates   atomic.Int64
	transfers atomic.Int64
	events    atomic.Int64
}

func (r *recorder) HandleUpdate(*livekit.SIPCallInfo)              { r.updates.Add(1) }
func (r *recorder) HandleTransfer(*livekit.SIPTransferInfo)        { r.transfers.Add(1) }
func (r *recorder) HandleCallContextRecorded(*livekit.SIPCallInfo) {}
func (r *recorder) HandleSignalingEvent(sip.SignalingEvent)        { r.events.Add(1) }

func bothOn() *config.Config {
	conf := &config.Config{}
	conf.SkipInboundSubscribeWait = true
	conf.InboundRingbackUntilSubscribed = true
	return conf
}

func info(id string, dir livekit.SIPCallDirection) *livekit.SIPCallInfo {
	return &livekit.SIPCallInfo{CallId: id, CallDirection: dir}
}

// setup starts a call of the given direction and connects a wrapped room to it.
func setup(t *testing.T, conf *config.Config, dir livekit.SIPCallDirection) (*Extensions, sip.StateHandler, *room, *innerRoom) {
	t.Helper()
	ext := New(conf, logger.GetLogger(), nil)
	h := ext.StateHandler(&recorder{}, info("SCL_1", dir))
	inner := newInnerRoom()
	r := newRoom(ext, inner, logger.GetLogger())
	require.NoError(t, r.Connect(context.Background(), conf, sip.RoomConfig{
		Participant: sip.ParticipantConfig{Attributes: map[string]string{livekit.AttrSIPCallID: "SCL_1"}},
	}))
	return ext, h, r, inner
}

func ready(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestInboundIsAnsweredAtOnce(t *testing.T) {
	_, _, r, inner := setup(t, bothOn(), livekit.SIPCallDirection_SCD_INBOUND)
	require.True(t, ready(r.Subscribed()), "an inbound call must not wait for a subscriber")
	require.False(t, ready(inner.Subscribed()), "the room underneath still tells the truth")
}

func TestOnlyInboundAndOnlyWhenConfigured(t *testing.T) {
	_, _, out, _ := setup(t, bothOn(), livekit.SIPCallDirection_SCD_OUTBOUND)
	require.False(t, ready(out.Subscribed()), "an outbound call keeps LiveKit's behaviour")

	_, _, off, _ := setup(t, &config.Config{}, livekit.SIPCallDirection_SCD_INBOUND)
	require.False(t, ready(off.Subscribed()), "without the config key an inbound call waits, as upstream")
}

func TestUnknownCallIsStillAnsweredAtOnce(t *testing.T) {
	conf := bothOn()
	ext := New(conf, logger.GetLogger(), nil)
	inner := newInnerRoom()
	r := newRoom(ext, inner, logger.GetLogger())
	require.NoError(t, r.Connect(context.Background(), conf, sip.RoomConfig{}))
	require.True(t, ready(r.Subscribed()), "waiting is the one answer that must never be given by mistake")
	l := &leg{}
	require.Nil(t, r.WriteOutboundAudioTo(l))
	require.Nil(t, inner.output())
	require.Eventually(t, func() bool { return l.loud.Load() >= 1 }, time.Second, 5*time.Millisecond, "and the caller still gets the tone")
	require.NoError(t, r.Close())
}

func TestRingingToneUntilSomeoneIsThere(t *testing.T) {
	_, _, r, inner := setup(t, bothOn(), livekit.SIPCallDirection_SCD_INBOUND)
	require.True(t, ready(r.Subscribed())) // LiveKit asks before it answers
	l := &leg{}
	require.Nil(t, r.WriteOutboundAudioTo(l))
	require.Nil(t, inner.output(), "the room's mixer must not write to the leg while the tone plays")
	require.Eventually(t, func() bool { return l.loud.Load() >= 3 }, time.Second, 5*time.Millisecond, "the tone must reach the leg")

	close(inner.subscribed) // an agent's track is being listened to
	require.Eventually(t, func() bool { return inner.output() == msdk.PCM16Writer(l) }, time.Second, 5*time.Millisecond,
		"the leg must go to the room once someone is there")
	after := l.frames.Load()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, after, l.frames.Load(), "no tone frame may follow the handover")
	require.Zero(t, l.closes.Load())
}

func TestNoToneWhenSomeoneIsAlreadyThere(t *testing.T) {
	_, _, r, inner := setup(t, bothOn(), livekit.SIPCallDirection_SCD_INBOUND)
	close(inner.subscribed)
	require.True(t, ready(r.Subscribed()))
	l := &leg{}
	require.Nil(t, r.WriteOutboundAudioTo(l))
	require.Same(t, l, inner.output())
	time.Sleep(60 * time.Millisecond)
	require.Zero(t, l.frames.Load())
}

func TestNoToneForOutbound(t *testing.T) {
	_, _, out, inner := setup(t, bothOn(), livekit.SIPCallDirection_SCD_OUTBOUND)
	require.False(t, ready(out.Subscribed()))
	l := &leg{}
	out.WriteOutboundAudioTo(l)
	require.Same(t, l, inner.output())
	time.Sleep(60 * time.Millisecond)
	require.Zero(t, l.frames.Load())
}

// A PIN call is answered before LiveKit asks whether to wait, so its leg is
// handed over first. It never rings: not then, and not when LiveKit's own
// transfer takes the leg away and hands it back later.
func TestNoToneWhenAnsweredBeforeBeingAskedToWait(t *testing.T) {
	_, _, r, inner := setup(t, bothOn(), livekit.SIPCallDirection_SCD_INBOUND)
	l := &leg{}
	r.WriteOutboundAudioTo(l)
	require.Same(t, l, inner.output())

	require.True(t, ready(r.Subscribed()))
	require.Same(t, l, r.WriteOutboundAudioTo(nil))
	require.Nil(t, r.WriteOutboundAudioTo(l))
	require.Same(t, l, inner.output())

	time.Sleep(60 * time.Millisecond)
	require.Zero(t, l.frames.Load())
}

func TestToneOnlyOncePerCall(t *testing.T) {
	_, _, r, inner := setup(t, bothOn(), livekit.SIPCallDirection_SCD_INBOUND)
	require.True(t, ready(r.Subscribed()))
	l := &leg{}
	r.WriteOutboundAudioTo(l)
	require.Eventually(t, func() bool { return l.frames.Load() > 0 }, time.Second, 5*time.Millisecond)

	// LiveKit takes the output away (as its transfer does) and gives it back.
	require.Same(t, l, r.WriteOutboundAudioTo(nil), "the writer in place is the one the tone held")
	stopped := l.frames.Load()
	require.Nil(t, inner.output())
	require.Nil(t, r.WriteOutboundAudioTo(l))
	require.Same(t, l, inner.output(), "the second time the leg goes to the room")
	time.Sleep(80 * time.Millisecond)
	require.Equal(t, stopped, l.frames.Load(), "and nothing rings again")
}

func TestCloseWhileRinging(t *testing.T) {
	_, _, r, inner := setup(t, bothOn(), livekit.SIPCallDirection_SCD_INBOUND)
	require.True(t, ready(r.Subscribed()))
	l := &leg{}
	r.WriteOutboundAudioTo(l)
	require.Eventually(t, func() bool { return l.frames.Load() > 0 }, time.Second, 5*time.Millisecond)

	require.NoError(t, r.Close())
	require.Len(t, inner.closes, 1)
	require.Same(t, l, inner.outAtClose, "the room must hold the leg's writer when it closes, so that it closes it")
	after := l.frames.Load()
	time.Sleep(80 * time.Millisecond)
	require.Equal(t, after, l.frames.Load())

	// A leg handed over after the close goes straight through.
	l2 := &leg{}
	r.WriteOutboundAudioTo(l2)
	require.Same(t, l2, inner.output())
}

// The same hand-over against LiveKit's real room, not a stand-in: its output
// switch takes the leg at LiveKit's sample rate, and closing the room closes
// the leg's writer exactly once.
func TestRealRoomClosesTheLegItWasGiven(t *testing.T) {
	conf := bothOn()
	ext := New(conf, logger.GetLogger(), nil)
	ext.StateHandler(&recorder{}, info("SCL_1", livekit.SIPCallDirection_SCD_INBOUND))
	r := ext.GetRoom(logger.GetLogger(), nil).(*room)
	r.call = ext.lookup("SCL_1") // what Connect does; joining needs a LiveKit server

	require.True(t, ready(r.Subscribed()))
	l := &leg{rate: sip.RoomSampleRate}
	require.Nil(t, r.WriteOutboundAudioTo(l))
	require.Eventually(t, func() bool { return l.loud.Load() >= 1 }, time.Second, 5*time.Millisecond)

	require.NoError(t, r.Close())
	require.EqualValues(t, 1, l.closes.Load(), "the room must close the leg's writer, once")
	after := l.frames.Load()
	time.Sleep(80 * time.Millisecond)
	require.Equal(t, after, l.frames.Load())
}

func TestRoomGoneWhileRinging(t *testing.T) {
	_, _, r, inner := setup(t, bothOn(), livekit.SIPCallDirection_SCD_INBOUND)
	require.True(t, ready(r.Subscribed()))
	l := &leg{}
	r.WriteOutboundAudioTo(l)
	require.Eventually(t, func() bool { return l.frames.Load() > 0 }, time.Second, 5*time.Millisecond)
	close(inner.gone)
	require.Eventually(t, func() bool { return inner.output() == msdk.PCM16Writer(l) }, time.Second, 5*time.Millisecond)
}

func failedWith(code int, text string) *livekit.SIPCallInfo {
	i := info("SCL_1", livekit.SIPCallDirection_SCD_OUTBOUND)
	i.CallStatusCode = &livekit.SIPStatus{Code: livekit.SIPStatusCode(code), Status: text}
	return i
}

func ended(reason string, status int) sip.SignalingEvent {
	return sip.SignalingEvent{Type: sip.EventCallEnded, Time: time.Now(), Reason: reason, StatusCode: status}
}

func TestFinalStatusOfARejectedCall(t *testing.T) {
	ext, h, r, inner := setup(t, bothOn(), livekit.SIPCallDirection_SCD_OUTBOUND)
	p := &participant{echoAfter: 30 * time.Millisecond}
	r.participant = func() attributes { return p }

	h.HandleUpdate(failedWith(486, "Busy Here"))
	h.(sip.SignalingEventHandler).HandleSignalingEvent(ended("busy", 486))
	require.Nil(t, ext.lookup("SCL_1"), "an ended call is forgotten")

	start := time.Now()
	require.NoError(t, r.CloseWithReason(livekit.DisconnectReason_USER_REJECTED))
	require.Equal(t, []map[string]string{{AttrSIPStatusCode: "486", AttrSIPStatus: "Busy Here"}}, p.sets())
	require.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond, "the leg must wait for the server's confirmation")
	require.Less(t, time.Since(start), statusEchoWait, "and leave as soon as it has it")
	require.Equal(t, []livekit.DisconnectReason{livekit.DisconnectReason_USER_REJECTED}, inner.closes)

	require.NoError(t, r.Close())
	require.Len(t, p.sets(), 1, "the attributes are written once")
}

func TestFinalStatusOfOurOwnRingOut(t *testing.T) {
	_, h, r, _ := setup(t, bothOn(), livekit.SIPCallDirection_SCD_OUTBOUND)
	p := &participant{echoAfter: 0}
	r.participant = func() attributes { return p }
	h.HandleUpdate(info("SCL_1", livekit.SIPCallDirection_SCD_OUTBOUND))
	h.(sip.SignalingEventHandler).HandleSignalingEvent(ended("no-answer", 0))
	require.NoError(t, r.Close())
	require.Equal(t, []map[string]string{{AttrSIPStatusCode: "408", AttrSIPStatus: "Request Timeout (ring timeout)"}}, p.sets())
}

func TestFinalStatusIsBoundedWhenTheServerNeverConfirms(t *testing.T) {
	_, h, r, inner := setup(t, bothOn(), livekit.SIPCallDirection_SCD_OUTBOUND)
	p := &participant{echoAfter: -1}
	r.participant = func() attributes { return p }
	h.HandleUpdate(failedWith(480, "Temporarily Unavailable"))
	h.(sip.SignalingEventHandler).HandleSignalingEvent(ended("unavailable", 480))
	start := time.Now()
	require.NoError(t, r.Close())
	require.GreaterOrEqual(t, time.Since(start), statusEchoWait)
	require.Less(t, time.Since(start), statusEchoWait+200*time.Millisecond)
	require.Len(t, inner.closes, 1)
}

func TestNoFinalStatusForOtherEndings(t *testing.T) {
	cases := []struct {
		name string
		dir  livekit.SIPCallDirection
		rec  *livekit.SIPCallInfo
		ev   sip.SignalingEvent
	}{
		{"answered, then hung up", livekit.SIPCallDirection_SCD_OUTBOUND, info("SCL_1", livekit.SIPCallDirection_SCD_OUTBOUND), ended("bye", 200)},
		{"hung up by us while ringing", livekit.SIPCallDirection_SCD_OUTBOUND, info("SCL_1", livekit.SIPCallDirection_SCD_OUTBOUND), ended("hangup", 0)},
		{"inbound, with a status in its record", livekit.SIPCallDirection_SCD_INBOUND, func() *livekit.SIPCallInfo {
			i := failedWith(486, "Busy Here")
			i.CallDirection = livekit.SIPCallDirection_SCD_INBOUND
			return i
		}(), ended("no-answer", 486)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, h, r, inner := setup(t, bothOn(), tc.dir)
			p := &participant{}
			r.participant = func() attributes { return p }
			h.HandleUpdate(tc.rec)
			h.(sip.SignalingEventHandler).HandleSignalingEvent(tc.ev)
			require.NoError(t, r.Close())
			require.Empty(t, p.sets())
			require.Len(t, inner.closes, 1)
		})
	}
}

func TestCloseBeforeTheRoomWasJoined(t *testing.T) {
	_, h, r, inner := setup(t, bothOn(), livekit.SIPCallDirection_SCD_OUTBOUND)
	r.participant = func() attributes { return nil }
	h.HandleUpdate(failedWith(486, "Busy Here"))
	h.(sip.SignalingEventHandler).HandleSignalingEvent(ended("busy", 486))
	require.NoError(t, r.Close())
	require.Len(t, inner.closes, 1)
}

func TestWarmTransferGuard(t *testing.T) {
	conf := &config.Config{}
	ext := New(conf, logger.GetLogger(), nil)
	ext.StateHandler(&recorder{}, info("SCL_1", livekit.SIPCallDirection_SCD_OUTBOUND))
	inner := newInnerRoom()
	r := newRoom(ext, inner, logger.GetLogger())
	require.NoError(t, r.Connect(context.Background(), conf, sip.RoomConfig{
		Participant: sip.ParticipantConfig{Attributes: map[string]string{
			livekit.AttrSIPCallID: "SCL_1",
			ConsultMuteAttr:       "sip-customer , other,",
		}},
	}))
	require.NotNil(t, inner.filter, "the guard must be in place before the room is joined")
	require.Equal(t, 1, inner.connects)
	require.False(t, inner.filter("sip-customer"))
	require.False(t, inner.filter("other"))
	require.True(t, inner.filter("agent-7"))
	require.True(t, inner.filter(""))
}

func TestNoGuardWithoutTheAttribute(t *testing.T) {
	_, _, _, inner := setup(t, bothOn(), livekit.SIPCallDirection_SCD_OUTBOUND)
	require.Nil(t, inner.filter)
}

func TestHandlerPassesEverythingOn(t *testing.T) {
	ext := New(&config.Config{}, logger.GetLogger(), nil)
	rec := &recorder{}
	h := ext.StateHandler(rec, info("SCL_9", livekit.SIPCallDirection_SCD_OUTBOUND))
	require.NotNil(t, ext.lookup("SCL_9"))

	h.HandleUpdate(info("SCL_9", livekit.SIPCallDirection_SCD_OUTBOUND))
	h.HandleTransfer(&livekit.SIPTransferInfo{})
	h.(sip.SignalingEventHandler).HandleSignalingEvent(sip.SignalingEvent{Type: sip.EventInviteSent, Time: time.Now()})
	require.EqualValues(t, 1, rec.updates.Load())
	require.EqualValues(t, 1, rec.transfers.Load())
	require.EqualValues(t, 1, rec.events.Load())
	require.NotNil(t, ext.lookup("SCL_9"), "only the end of the call drops it")

	h.(sip.SignalingEventHandler).HandleSignalingEvent(ended("bye", 200))
	require.EqualValues(t, 2, rec.events.Load())
	require.Nil(t, ext.lookup("SCL_9"))

	require.Nil(t, ext.StateHandler(nil, info("SCL_0", livekit.SIPCallDirection_SCD_INBOUND)))
}

func TestCallsWhoseEndIsNeverReportedAreSwept(t *testing.T) {
	ext := New(&config.Config{}, logger.GetLogger(), nil)
	ext.StateHandler(&recorder{}, info("SCL_old", livekit.SIPCallDirection_SCD_INBOUND))
	ext.lookup("SCL_old").created = time.Now().Add(-callTTL - time.Minute)
	ext.lastSweep = time.Now().Add(-sweepEvery)
	ext.StateHandler(&recorder{}, info("SCL_new", livekit.SIPCallDirection_SCD_INBOUND))
	require.Nil(t, ext.lookup("SCL_old"))
	require.NotNil(t, ext.lookup("SCL_new"))
}
