// Copyright 2026 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Native SIP REGISTER support — Tilbyderen fork extension.
//
// Why: some carriers (Telavox and most Nordic ITSPs) route inbound
// INVITEs to the Contact URI advertised in REGISTER. Without an active
// REGISTER, those carriers have no idea where to deliver inbound calls
// and the dialog dies before LK-SIP ever sees it. Mainline LK-SIP only
// answers — it doesn't register. This file fills the gap.
//
// Each entry in config.OutboundRegistrations runs as one goroutine that
// REGISTERs on startup, handles the 401 digest challenge, and re-
// registers 30 s before the server-honored expiry. On failure it backs
// off exponentially (capped at 60 s) and keeps trying. State is exposed
// for health / metrics via Snapshot() and the registration_state gauge.

package sip

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/frostbyte73/core"
	"github.com/icholy/digest"
	"github.com/livekit/sipgo/sip"

	"github.com/livekit/protocol/logger"

	"github.com/livekit/sip/pkg/config"
	"github.com/livekit/sip/pkg/stats"
)

// registerReRegisterLead controls how early before expiry we attempt
// to re-register. Telavox honors expiry as a hard cutoff — leaving
// less than ~10 s of margin risks a window where inbound INVITEs are
// dropped server-side.
const registerReRegisterLead = 30 * time.Second

// registerBackoffMin and registerBackoffMax bound the retry sleep after
// a failed REGISTER. We jitter inside the bound to avoid lockstep across
// multiple registrations restarted together.
const (
	registerBackoffMin = 5 * time.Second
	registerBackoffMax = 60 * time.Second
)

// registerDefaultExpires is requested when the config omits expires_sec.
// Most carriers honor 3600 directly; some downgrade to 600-1800. We
// follow whatever the server returns.
const registerDefaultExpires = 3600

// registerTxTimeout caps how long we wait for a REGISTER transaction
// to produce a final response. sipgo's default would block indefinitely
// on packet loss.
const registerTxTimeout = 8 * time.Second

// RegistrationState describes the current health of one registered
// trunk. Updated atomically under Registrar.mu.
type RegistrationState struct {
	Name           string
	Registered     bool
	LastError      string
	LastSuccess    time.Time
	NextRefresh    time.Time
	ExpiresSeconds int
	ContactURI     string
	Attempts       uint64
	Failures       uint64
}

// Registrar maintains REGISTER loops for the configured trunks. One
// per OutboundRegistration entry. Started by Service.Start after the
// SIP client's UA is up; stopped via Close(), which signals all
// goroutines through closing.Watch().
type Registrar struct {
	cli     *Client // for sipCli, sconf
	log     logger.Logger
	mon     *stats.Monitor // may be nil (test setups)
	regs    []*registration
	closing core.Fuse
	wg      sync.WaitGroup
}

type registration struct {
	cfg config.OutboundRegistration

	// resolved at construction time
	registrarAddr string // "host:port" sent to sipCli
	transport     Transport
	realm         string
	contactUser   string
	fromUser      string
	requestedExp  int

	mu    sync.Mutex
	state RegistrationState

	// per-registration SIP transaction state
	callID  string
	fromTag string
	cseq    uint32
}

// NewRegistrar builds the Registrar but does NOT start any goroutines.
// Call Start once the SIP Client is ready (its UA must exist so we can
// send transactions through cli.sipCli).
func NewRegistrar(cli *Client, mon *stats.Monitor, log logger.Logger) (*Registrar, error) {
	if cli == nil {
		return nil, errors.New("registrar: client is required")
	}
	if log == nil {
		log = logger.GetLogger()
	}
	r := &Registrar{
		cli: cli,
		log: log,
		mon: mon,
	}
	for i, cfg := range cli.conf.OutboundRegistrations {
		reg, err := r.buildRegistration(cfg)
		if err != nil {
			return nil, fmt.Errorf("registrar: entry %d (%q) invalid: %w", i, cfg.Name, err)
		}
		r.regs = append(r.regs, reg)
	}
	return r, nil
}

// Start kicks off one goroutine per configured registration. Safe to
// call with zero registrations (no-op). Call Close() to stop.
func (r *Registrar) Start() {
	if len(r.regs) == 0 {
		return
	}
	r.log.Infow("starting SIP registrar", "registrations", len(r.regs))
	for _, reg := range r.regs {
		r.wg.Add(1)
		go r.runRegistration(reg)
	}
}

// Close signals all registration goroutines to stop and waits for them.
// Best-effort sends an unREGISTER (Expires: 0) for each currently-
// registered entry so the carrier flushes its routing cache cleanly.
func (r *Registrar) Close() {
	r.closing.Break()
	r.wg.Wait()
	for _, reg := range r.regs {
		reg.mu.Lock()
		registered := reg.state.Registered
		reg.mu.Unlock()
		if !registered {
			continue
		}
		// Best-effort unregister with a short context so a hung carrier
		// doesn't block shutdown.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := r.sendRegister(ctx, reg, 0); err != nil {
			r.log.Warnw("unregister failed on shutdown", err, "registration", reg.cfg.Name)
		}
		cancel()
	}
}

// Snapshot returns the current state of all registrations. Used by the
// health endpoint and any external observability. Copy-on-return is
// intentional — callers should not race on the live state.
func (r *Registrar) Snapshot() []RegistrationState {
	out := make([]RegistrationState, 0, len(r.regs))
	for _, reg := range r.regs {
		reg.mu.Lock()
		out = append(out, reg.state)
		reg.mu.Unlock()
	}
	return out
}

// AllRegistered returns true when every configured registration is
// currently in the registered state. Used by the health endpoint.
func (r *Registrar) AllRegistered() bool {
	for _, reg := range r.regs {
		reg.mu.Lock()
		ok := reg.state.Registered
		reg.mu.Unlock()
		if !ok {
			return false
		}
	}
	return true
}

func (r *Registrar) buildRegistration(cfg config.OutboundRegistration) (*registration, error) {
	if cfg.Name == "" {
		return nil, errors.New("name is required")
	}
	if cfg.RegistrarHost == "" {
		return nil, errors.New("registrar_host is required")
	}
	if cfg.AuthUsername == "" || cfg.AuthPassword == "" {
		return nil, errors.New("auth_username and auth_password are required")
	}
	tr := parseTransportName(cfg.Transport)
	// Split host[:port] so we can apply registrar_port default
	host := cfg.RegistrarHost
	port := cfg.RegistrarPort
	if i := strings.LastIndex(host, ":"); i > 0 && !strings.HasSuffix(host, "]") {
		// strip explicit port from host
		portStr := host[i+1:]
		if p, err := strconv.Atoi(portStr); err == nil && p > 0 {
			port = p
			host = host[:i]
		}
	}
	if port == 0 {
		if tr == TransportTLS {
			port = config.DefaultSIPPortTLS
		} else {
			port = config.DefaultSIPPort
		}
	}
	realm := cfg.AuthRealm
	if realm == "" {
		realm = host
	}
	contactUser := cfg.ContactUser
	if contactUser == "" {
		contactUser = cfg.AuthUsername
	}
	fromUser := cfg.From
	if fromUser == "" {
		fromUser = cfg.AuthUsername
	}
	exp := cfg.ExpiresSec
	if exp <= 0 {
		exp = registerDefaultExpires
	}
	reg := &registration{
		cfg:           cfg,
		registrarAddr: net_JoinHostPort(host, port),
		transport:     tr,
		realm:         realm,
		contactUser:   contactUser,
		fromUser:      fromUser,
		requestedExp:  exp,
	}
	reg.state.Name = cfg.Name
	return reg, nil
}

// runRegistration is the long-lived per-trunk loop. Exits when the
// Registrar.closing fuse is broken.
func (r *Registrar) runRegistration(reg *registration) {
	defer r.wg.Done()

	log := r.log.WithValues("registration", reg.cfg.Name, "registrar", reg.registrarAddr)
	log.Infow("starting REGISTER loop",
		"contact_user", reg.contactUser,
		"requested_expires", reg.requestedExp,
	)

	backoff := registerBackoffMin
	for {
		// Bail if shutdown was requested between iterations.
		if r.closing.IsBroken() {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), registerTxTimeout)
		expires, err := r.registerOnce(ctx, reg, log)
		cancel()

		if err != nil {
			reg.mu.Lock()
			reg.state.Registered = false
			reg.state.LastError = err.Error()
			reg.state.Failures++
			reg.state.Attempts++
			reg.mu.Unlock()
			log.Warnw("REGISTER failed", err, "retry_in", backoff)
			if !r.sleep(backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}

		// Success path.
		backoff = registerBackoffMin
		now := time.Now()
		var sleep time.Duration
		if expires > registerReRegisterLead {
			sleep = expires - registerReRegisterLead
		} else if expires > 0 {
			// Tiny expiry — re-register half-way through.
			sleep = expires / 2
		} else {
			// Server gave us nothing — try again at a safe default.
			sleep = registerBackoffMax
		}

		reg.mu.Lock()
		reg.state.Registered = true
		reg.state.LastError = ""
		reg.state.LastSuccess = now
		reg.state.NextRefresh = now.Add(sleep)
		reg.state.ExpiresSeconds = int(expires / time.Second)
		reg.state.ContactURI = r.contactURIString(reg)
		reg.state.Attempts++
		reg.mu.Unlock()

		log.Infow("REGISTER ok",
			"expires_seconds", int(expires/time.Second),
			"next_refresh_in", sleep,
			"contact", reg.state.ContactURI,
		)

		if !r.sleep(sleep) {
			return
		}
	}
}

// sleep returns false if the fuse fires while we were sleeping.
func (r *Registrar) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-r.closing.Watch():
		return false
	}
}

// nextBackoff doubles the previous interval up to the max, with ±20% jitter.
func nextBackoff(prev time.Duration) time.Duration {
	next := prev * 2
	if next > registerBackoffMax {
		next = registerBackoffMax
	}
	jitter := time.Duration(rand.Int63n(int64(next) / 5))
	if rand.Intn(2) == 0 {
		next -= jitter
	} else {
		next += jitter
	}
	if next < registerBackoffMin {
		next = registerBackoffMin
	}
	return next
}

// registerOnce sends ONE REGISTER transaction (with up to one digest
// retry) and returns the server-honored expiry duration on success.
// `expireOverride > 0` means "register for exactly this many seconds"
// — used internally for unregister (expires=0). Otherwise the
// registration's requestedExp is used.
func (r *Registrar) registerOnce(ctx context.Context, reg *registration, log logger.Logger) (time.Duration, error) {
	return r.sendRegisterReturningExpires(ctx, reg, reg.requestedExp, log)
}

// sendRegister is a thin wrapper around the inner transaction so Close()
// can issue an unregister without going through the loop.
func (r *Registrar) sendRegister(ctx context.Context, reg *registration, expires int) error {
	log := r.log.WithValues("registration", reg.cfg.Name, "registrar", reg.registrarAddr)
	_, err := r.sendRegisterReturningExpires(ctx, reg, expires, log)
	return err
}

func (r *Registrar) sendRegisterReturningExpires(ctx context.Context, reg *registration, requestedExpires int, log logger.Logger) (time.Duration, error) {
	if r.cli == nil || r.cli.sipCli == nil {
		return 0, errors.New("SIP client not started")
	}
	// Fresh per-attempt callID + fromTag — Telavox treats a refresh that
	// reuses the previous dialog identity differently than a new one and
	// may reject a re-REGISTER that looks like a stale dialog refresh.
	reg.callID = sip.GenerateTagN(16)
	reg.fromTag = sip.GenerateTagN(8)
	reg.cseq = 0

	// First attempt — unauthenticated. Expect 401 unless the registrar
	// has remembered us (some do, e.g. across short reconnects).
	req := r.buildRegisterRequest(reg, requestedExpires, "", "", "")
	resp, err := r.transaction(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("first REGISTER: %w", err)
	}

	switch resp.StatusCode {
	case 200:
		return parseRegisterExpires(resp, requestedExpires), nil
	case 401, 407:
		// fall through to auth retry
	default:
		return 0, fmt.Errorf("unexpected REGISTER status %d %s", resp.StatusCode, resp.Reason)
	}

	authHeaderName := "WWW-Authenticate"
	authRespName := "Authorization"
	if resp.StatusCode == 407 {
		authHeaderName = "Proxy-Authenticate"
		authRespName = "Proxy-Authorization"
	}

	chHdr := resp.GetHeader(authHeaderName)
	if chHdr == nil {
		return 0, fmt.Errorf("%d response missing %s header", resp.StatusCode, authHeaderName)
	}
	challenge, err := digest.ParseChallenge(chHdr.Value())
	if err != nil {
		return 0, fmt.Errorf("invalid challenge %q: %w", chHdr.Value(), err)
	}

	// Force realm from config if the server's challenge realm is empty
	// (rare, but valid per RFC).
	if challenge.Realm == "" {
		challenge.Realm = reg.realm
	}

	cred, err := digest.Digest(challenge, digest.Options{
		Method:   "REGISTER",
		URI:      req.Recipient.String(),
		Username: reg.cfg.AuthUsername,
		Password: reg.cfg.AuthPassword,
	})
	if err != nil {
		return 0, fmt.Errorf("digest compute: %w", err)
	}

	// Second attempt — with Authorization header.
	req2 := r.buildRegisterRequest(reg, requestedExpires, authRespName, cred.String(), "")
	resp2, err := r.transaction(ctx, req2)
	if err != nil {
		return 0, fmt.Errorf("authed REGISTER: %w", err)
	}
	if resp2.StatusCode != 200 {
		return 0, fmt.Errorf("REGISTER auth failed: %d %s", resp2.StatusCode, resp2.Reason)
	}
	return parseRegisterExpires(resp2, requestedExpires), nil
}

func (r *Registrar) buildRegisterRequest(reg *registration, expires int, authHeaderName, authValue, _ string) *sip.Request {
	// Request-URI for REGISTER is the registrar (host only, no user).
	reqURI := sip.Uri{
		Scheme: "sip",
		Host:   hostOf(reg.registrarAddr),
		Port:   portOf(reg.registrarAddr),
	}
	req := sip.NewRequest(sip.REGISTER, reqURI)

	reg.cseq++
	setCSeq(req, reg.cseq)

	// Call-ID stays stable across the digest retry within one REGISTER
	// attempt; runRegistration assigns a fresh one per attempt.
	req.RemoveHeader("Call-ID")
	cid := sip.CallIDHeader(reg.callID + "@" + r.cli.sconf.SignalingIP.String())
	req.AppendHeader(&cid)

	fromURI := sip.Uri{
		Scheme: "sip",
		User:   reg.fromUser,
		Host:   hostOf(reg.registrarAddr),
	}
	req.AppendHeader(&sip.FromHeader{
		Address: fromURI,
		Params:  sip.HeaderParams{{"tag", reg.fromTag}},
	})
	toURI := sip.Uri{
		Scheme: "sip",
		User:   reg.fromUser,
		Host:   hostOf(reg.registrarAddr),
	}
	req.AppendHeader(&sip.ToHeader{Address: toURI})

	// Contact = where we want inbound INVITEs delivered. This is the
	// crux of the whole feature — the registrar uses this URI as the
	// delivery address for the registered AOR.
	contactURI := sip.Uri{
		Scheme: "sip",
		User:   reg.contactUser,
		Host:   r.cli.sconf.SignalingIP.String(),
		Port:   r.cli.conf.SIPPort,
	}
	if reg.transport != TransportUDP {
		contactURI.UriParams = sip.HeaderParams{{"transport", strings.ToLower(string(reg.transport))}}
	}
	req.AppendHeader(&sip.ContactHeader{Address: contactURI})

	req.AppendHeader(sip.NewHeader("Expires", strconv.Itoa(expires)))
	req.AppendHeader(sip.NewHeader("Max-Forwards", "70"))
	req.AppendHeader(sip.NewHeader("Allow", "INVITE, ACK, CANCEL, BYE, NOTIFY, REFER, MESSAGE, OPTIONS, INFO, SUBSCRIBE"))
	req.AppendHeader(sip.NewHeader("User-Agent", UserAgent))
	req.AppendHeader(sip.NewHeader("Content-Length", "0"))

	if authHeaderName != "" && authValue != "" {
		req.AppendHeader(sip.NewHeader(authHeaderName, authValue))
	}
	return req
}

// transaction sends one request through the shared sipgo Client and
// waits for the final response, bounded by ctx and by the registrar's
// shutdown fuse. Reuses pkg/sip's sipResponse helper so the cancel
// behavior and 1xx skipping match the rest of the codebase.
func (r *Registrar) transaction(ctx context.Context, req *sip.Request) (*sip.Response, error) {
	tx, err := r.cli.sipCli.TransactionRequest(req)
	if err != nil {
		return nil, err
	}
	defer tx.Terminate()
	return sipResponse(ctx, tx, r.closing.Watch(), nil)
}

func (r *Registrar) contactURIString(reg *registration) string {
	uri := sip.Uri{
		Scheme: "sip",
		User:   reg.contactUser,
		Host:   r.cli.sconf.SignalingIP.String(),
		Port:   r.cli.conf.SIPPort,
	}
	return uri.String()
}

// parseRegisterExpires extracts the server-honored expiry. Carriers
// place it in either the Expires header or as a `;expires=N` parameter
// inside the Contact header — Telavox uses the latter. Returns the
// fallback in seconds if neither is present.
func parseRegisterExpires(resp *sip.Response, fallbackSec int) time.Duration {
	if h := resp.GetHeader("Expires"); h != nil {
		if n, err := strconv.Atoi(strings.TrimSpace(h.Value())); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	if c := resp.Contact(); c != nil {
		if v, ok := c.Params.Get("expires"); ok {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				return time.Duration(n) * time.Second
			}
		}
	}
	return time.Duration(fallbackSec) * time.Second
}

// Small helpers below — package-local to keep registrar.go self-contained.

func hostOf(hostPort string) string {
	if i := strings.LastIndex(hostPort, ":"); i > 0 {
		return hostPort[:i]
	}
	return hostPort
}

func portOf(hostPort string) int {
	if i := strings.LastIndex(hostPort, ":"); i > 0 {
		if p, err := strconv.Atoi(hostPort[i+1:]); err == nil {
			return p
		}
	}
	return config.DefaultSIPPort
}

// net_JoinHostPort is std net.JoinHostPort but accepts an int port.
// Avoids importing "net" just for one helper.
func net_JoinHostPort(host string, port int) string {
	return host + ":" + strconv.Itoa(port)
}

// parseTransportName accepts "udp" / "tcp" / "tls" (case-insensitive)
// and returns the matching Transport. Defaults to UDP, which is what
// every registered carrier we've seen so far requires for REGISTER.
func parseTransportName(s string) Transport {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "tcp":
		return TransportTCP
	case "tls":
		return TransportTLS
	}
	return TransportUDP
}
