package sip

import (
	"context"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livekit/sipgo"
	"github.com/livekit/sipgo/sip"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/logger"

	"github.com/livekit/sip/pkg/config"
)

// fakeRegistrar stands in for the carrier: it challenges an unauthenticated
// REGISTER with 401 and answers an authenticated one with 200 (or 403 when
// refusing, or not at all when silent). It records every request it gets.
type fakeRegistrar struct {
	mu       sync.Mutex
	requests []*sip.Request
	refuse   atomic.Bool
	silent   atomic.Bool
}

func (f *fakeRegistrar) TransactionRequest(req *sip.Request, _ ...sipgo.ClientRequestOption) (sip.ClientTransaction, error) {
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.mu.Unlock()

	tx := &fakeClientTx{responses: make(chan *sip.Response, 1), done: make(chan struct{})}
	switch {
	case f.silent.Load():
		// no answer: the caller's deadline has to end the wait
	case req.GetHeader("Authorization") == nil:
		resp := sip.NewResponseFromRequest(req, sip.StatusUnauthorized, "Unauthorized", nil)
		resp.AppendHeader(sip.NewHeader("WWW-Authenticate", `Digest realm="telavox.se", nonce="abc123", algorithm=MD5`))
		tx.responses <- resp
	case f.refuse.Load():
		tx.responses <- sip.NewResponseFromRequest(req, sip.StatusForbidden, "Forbidden", nil)
	default:
		resp := sip.NewResponseFromRequest(req, sip.StatusOK, "OK", nil)
		resp.AppendHeader(sip.NewHeader("Expires", req.GetHeader("Expires").Value()))
		tx.responses <- resp
	}
	return tx, nil
}

func (f *fakeRegistrar) WriteRequest(*sip.Request, ...sipgo.ClientRequestOption) error { return nil }
func (f *fakeRegistrar) Close() error                                                  { return nil }
func (f *fakeRegistrar) ResolveAddrPreferSRV(context.Context, string, string, int, string) (netip.AddrPort, error) {
	return netip.AddrPort{}, nil
}

func (f *fakeRegistrar) seen() []*sip.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*sip.Request(nil), f.requests...)
}

// withdrawals returns the authenticated REGISTERs with Expires: 0.
func (f *fakeRegistrar) withdrawals() []*sip.Request {
	var out []*sip.Request
	for _, req := range f.seen() {
		if req.GetHeader("Expires").Value() == "0" && req.GetHeader("Authorization") != nil {
			out = append(out, req)
		}
	}
	return out
}

type fakeClientTx struct {
	responses chan *sip.Response
	done      chan struct{}
	once      sync.Once
}

func (t *fakeClientTx) Terminate()                      { t.once.Do(func() { close(t.done) }) }
func (t *fakeClientTx) Done() <-chan struct{}           { return t.done }
func (t *fakeClientTx) Err() error                      { return nil }
func (t *fakeClientTx) Responses() <-chan *sip.Response { return t.responses }
func (t *fakeClientTx) Cancel() error                   { return nil }

func newTestRegistrar(t *testing.T, carrier *fakeRegistrar, names ...string) *Registrar {
	t.Helper()
	conf := &config.Config{SIPPort: 5060, NodeID: "NE_test"}
	for _, name := range names {
		conf.OutboundRegistrations = append(conf.OutboundRegistrations, config.OutboundRegistration{
			Name:          name,
			RegistrarHost: "sip.carrier.test",
			AuthUsername:  "u0047" + name,
			AuthPassword:  "secret",
			AuthRealm:     "telavox.se",
			ContactUser:   "u0047" + name,
			ExpiresSec:    180,
		})
	}
	cli := &Client{
		conf:   conf,
		sconf:  &ServiceConfig{SignalingIP: netip.MustParseAddr("203.0.113.7")},
		log:    logger.GetLogger(),
		sipCli: carrier,
	}
	r, err := NewRegistrar(cli, nil, logger.GetLogger())
	require.NoError(t, err)
	return r
}

func registrationGauge(t *testing.T, r *Registrar, name string) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, r.active.WithLabelValues(name).(prometheus.Metric).Write(&m))
	return m.GetGauge().GetValue()
}

func TestRegistrarRegistersAndWithdraws(t *testing.T) {
	carrier := &fakeRegistrar{}
	r := newTestRegistrar(t, carrier, "one")
	r.Start()
	require.Eventually(t, r.AllRegistered, 3*time.Second, 5*time.Millisecond)

	reqs := carrier.seen()
	require.Len(t, reqs, 2, "one challenge round: an unauthenticated REGISTER, then one with credentials")
	require.Nil(t, reqs[0].GetHeader("Authorization"))
	require.NotNil(t, reqs[1].GetHeader("Authorization"))
	for _, req := range reqs {
		require.Equal(t, sip.REGISTER, req.Method)
		require.Equal(t, "180", req.GetHeader("Expires").Value())
		contact := req.Contact()
		require.NotNil(t, contact)
		require.Equal(t, "u0047one", contact.Address.User)
		require.Equal(t, "203.0.113.7", contact.Address.Host)
		require.Equal(t, 5060, contact.Address.Port)
		_, hasTransport := contact.Address.UriParams.Get("transport")
		require.False(t, hasTransport, "a UDP binding carries no transport parameter")
		tag, ok := req.From().Params.Get("tag")
		require.True(t, ok, "From needs a tag")
		require.NotEmpty(t, tag)
	}
	require.NotEqual(t, reqs[0].CallID().Value(), "", "Call-ID is set")
	require.Equal(t, reqs[0].CallID().Value(), reqs[1].CallID().Value(), "the challenge round keeps one Call-ID")
	require.Equal(t, 1.0, registrationGauge(t, r, "one"))
	state := r.Snapshot()[0]
	require.True(t, state.Registered)
	require.Equal(t, 180, state.ExpiresSeconds)

	// Withdraw must finish the whole un-REGISTER: challenge AND the request
	// with credentials. The v1.17.0 fork stopped after the challenge.
	r.Withdraw()
	withdrawn := carrier.withdrawals()
	require.Len(t, withdrawn, 1, "an authenticated REGISTER with Expires: 0 reached the carrier")
	require.Equal(t, "u0047one", withdrawn[0].Contact().Address.User)
	require.False(t, r.AllRegistered())
	require.Equal(t, 0.0, registrationGauge(t, r, "one"))

	// Later calls do nothing: the drain hook and Close both call Withdraw.
	before := len(carrier.seen())
	r.Withdraw()
	r.Close()
	require.Len(t, carrier.seen(), before)
}

func TestRegistrarWithdrawsEveryBinding(t *testing.T) {
	carrier := &fakeRegistrar{}
	r := newTestRegistrar(t, carrier, "a", "b", "c")
	r.Start()
	require.Eventually(t, r.AllRegistered, 3*time.Second, 5*time.Millisecond)

	r.Close()
	users := map[string]bool{}
	for _, req := range carrier.withdrawals() {
		users[req.Contact().Address.User] = true
	}
	require.Equal(t, map[string]bool{"u0047a": true, "u0047b": true, "u0047c": true}, users)
	for _, name := range []string{"a", "b", "c"} {
		require.Equal(t, 0.0, registrationGauge(t, r, name))
	}
}

// A carrier that stops answering must not hold up the shutdown.
func TestRegistrarWithdrawIsBounded(t *testing.T) {
	old := registerWithdrawTimeout
	registerWithdrawTimeout = 300 * time.Millisecond
	defer func() { registerWithdrawTimeout = old }()

	carrier := &fakeRegistrar{}
	r := newTestRegistrar(t, carrier, "slow1", "slow2")
	r.Start()
	require.Eventually(t, r.AllRegistered, 3*time.Second, 5*time.Millisecond)

	carrier.silent.Store(true)
	start := time.Now()
	r.Withdraw()
	require.Less(t, time.Since(start), 2*time.Second, "both withdrawals share one deadline")
}

func TestRegistrarRefusedCredentials(t *testing.T) {
	carrier := &fakeRegistrar{}
	carrier.refuse.Store(true)
	r := newTestRegistrar(t, carrier, "refused")
	r.Start()
	require.Eventually(t, func() bool { return r.Snapshot()[0].Failures > 0 }, 3*time.Second, 5*time.Millisecond)
	require.False(t, r.AllRegistered())
	require.Equal(t, 0.0, registrationGauge(t, r, "refused"))
	require.Contains(t, r.Snapshot()[0].LastError, "403")

	// Not registered, so Close has nothing to withdraw, and it does not wait
	// out the retry backoff.
	start := time.Now()
	r.Close()
	require.Less(t, time.Since(start), 2*time.Second)
	require.Empty(t, carrier.withdrawals())
}

// A TCP binding says so in its Contact, so the carrier calls back over TCP.
func TestRegistrarContactTransport(t *testing.T) {
	carrier := &fakeRegistrar{}
	conf := &config.Config{SIPPort: 5060, NodeID: "NE_test"}
	conf.OutboundRegistrations = []config.OutboundRegistration{{
		Name: "tcp", RegistrarHost: "sip.carrier.test:5070", Transport: "TCP",
		AuthUsername: "u0047tcp", AuthPassword: "secret",
	}}
	cli := &Client{conf: conf, sconf: &ServiceConfig{SignalingIP: netip.MustParseAddr("203.0.113.7")}, log: logger.GetLogger(), sipCli: carrier}
	r, err := NewRegistrar(cli, nil, logger.GetLogger())
	require.NoError(t, err)
	r.Start()
	require.Eventually(t, r.AllRegistered, 3*time.Second, 5*time.Millisecond)
	defer r.Close()

	req := carrier.seen()[0]
	transport, ok := req.Contact().Address.UriParams.Get("transport")
	require.True(t, ok)
	require.Equal(t, "tcp", transport)
	require.Equal(t, "u0047tcp", req.Contact().Address.User, "contact_user defaults to the auth user")
	require.Equal(t, "sip.carrier.test", req.Recipient.Host)
	require.Equal(t, 5070, req.Recipient.Port)
	require.Equal(t, "3600", req.GetHeader("Expires").Value(), "expires_sec defaults to an hour")
}

func TestNilRegistrarIsSafe(t *testing.T) {
	var r *Registrar
	r.Withdraw()
	r.Close()

	// No outbound_registrations: nothing to start.
	s := &Service{conf: &config.Config{}}
	reg, err := s.StartRegistrar()
	require.NoError(t, err)
	require.Nil(t, reg)
}
