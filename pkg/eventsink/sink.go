// Package eventsink is a Tilbyderen fork extension: it writes every SIP
// signaling event and every call-record update of this node to a Redis
// stream, so an application outside LiveKit can follow each call step by step.
//
// Why it exists: the SIP service reports signaling events (INVITE sent, 180,
// 200, BYE, call ended, ...) to an optional sip.SignalingEventHandler, and the
// per-call record (livekit.SIPCallInfo) to a sip.StateHandler. The open-source
// build has no receiver for the events, and open-source livekit-server
// discards the call record (its UpdateSIPCallState handler is a placeholder).
// This package is that receiver.
//
// What it guarantees:
//   - Publishing never blocks a call. The handlers are invoked while the call
//     state holds its mutex, so they only encode and hand off to a buffered
//     channel; a full channel drops the entry and counts it.
//   - Entries of one call keep the order in which the call produced them.
//   - No phone number leaves the process through the stream: the record has
//     the call ids, the trunk, timings, SIP status and the tb.* participant
//     attributes of the request, but neither From/To URIs nor room name or
//     participant identity (both can embed a caller number on inbound calls).
//
// Stream entry: fields "v" (schema version), "kind" ("event" or "call") and
// "data" (one JSON object; see eventRecord and callRecord in handler.go).
//
// It is wired in cmd/livekit-sip/main.go and switched on with the
// event_stream section of the config file. With the section absent the
// service behaves exactly as upstream.
package eventsink

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/livekit/protocol/logger"

	"github.com/livekit/sip/pkg/config"
)

const (
	// SchemaVersion is the value of the "v" field of every stream entry.
	SchemaVersion = "1"

	// DefaultKey is the Redis stream key used when the config names none.
	DefaultKey = "sip:events"
	// DefaultMaxLen is the approximate cap on stream entries (XADD MAXLEN ~).
	// One call produces roughly 15 entries.
	DefaultMaxLen = 200_000

	// queueSize bounds the entries waiting for Redis. At roughly 15 entries
	// per call this absorbs several hundred calls while Redis is slow.
	queueSize = 8192
	// appendTimeout bounds one XADD.
	appendTimeout = 2 * time.Second
	// appendAttempts is how often one entry is tried before it is dropped.
	appendAttempts = 3
	// appendRetryDelay is the pause between attempts.
	appendRetryDelay = 200 * time.Millisecond
	// closeTimeout bounds how long Close waits for the queue to empty.
	closeTimeout = 3 * time.Second
	// warnInterval rate-limits the "dropped" warnings.
	warnInterval = 10 * time.Second
)

// Kinds of stream entry.
const (
	KindEvent = "event"
	KindCall  = "call"
)

// DoFunc runs one Redis command and reports its error. main.go passes a
// closure over the service's Redis client, so this package needs no Redis
// dependency of its own.
type DoFunc func(ctx context.Context, args ...any) error

type entry struct {
	kind string
	data []byte
}

// Sink queues entries and appends them to the stream from one goroutine.
// A nil *Sink is valid and does nothing, which is how a disabled
// event_stream is represented.
type Sink struct {
	log    logger.Logger
	nodeID string
	key    string
	maxLen int64
	// attrPrefix selects the participant attributes copied into every entry
	// (keys starting with it). Empty copies none.
	attrPrefix string
	do         DoFunc

	ch        chan entry    // never closed: publish may race with Close
	quit      chan struct{} // closed by Close: write what is queued, then exit
	stopped   chan struct{} // closed when the writer goroutine has exited
	closing   atomic.Bool
	closeOnce sync.Once
	lastWarn  atomic.Int64 // unix nanos of the last "dropped" warning

	entries *prometheus.CounterVec
}

// New returns a running Sink, or nil when the event stream is not enabled.
func New(conf config.EventStreamConfig, nodeID string, log logger.Logger, do DoFunc) *Sink {
	if !conf.Enabled || do == nil {
		return nil
	}
	if log == nil {
		log = logger.GetLogger()
	}
	key := conf.Key
	if key == "" {
		key = DefaultKey
	}
	maxLen := conf.MaxLen
	if maxLen <= 0 {
		maxLen = DefaultMaxLen
	}
	s := &Sink{
		log:        log,
		nodeID:     nodeID,
		key:        key,
		maxLen:     maxLen,
		attrPrefix: conf.AttributePrefix,
		do:         do,

		ch:      make(chan entry, queueSize),
		quit:    make(chan struct{}),
		stopped: make(chan struct{}),
		entries: entriesCounter(nodeID),
	}
	go s.run()
	log.Infow("SIP event stream enabled", "key", key, "maxLen", maxLen, "attributePrefix", conf.AttributePrefix)
	return s
}

// publish queues one entry. It never blocks: when the queue is full, or the
// sink is closing, the entry is dropped and counted.
func (s *Sink) publish(kind string, data []byte) {
	if s == nil || data == nil {
		return
	}
	if s.closing.Load() {
		s.entries.WithLabelValues("dropped_closed").Inc()
		return
	}
	select {
	case s.ch <- entry{kind: kind, data: data}:
	default:
		s.entries.WithLabelValues("dropped_full").Inc()
		s.warn("SIP event stream queue is full, dropping entries", nil)
	}
}

func (s *Sink) run() {
	defer close(s.stopped)
	for {
		select {
		case e := <-s.ch:
			s.write(e)
		case <-s.quit:
			// Write what is already queued, then stop.
			for {
				select {
				case e := <-s.ch:
					s.write(e)
				default:
					return
				}
			}
		}
	}
}

func (s *Sink) write(e entry) {
	if err := s.append(e); err != nil {
		s.entries.WithLabelValues("dropped_error").Inc()
		s.warn("cannot write to the SIP event stream, dropping entries", err)
		return
	}
	s.entries.WithLabelValues("published").Inc()
}

func (s *Sink) append(e entry) error {
	var err error
	for attempt := 0; attempt < appendAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(appendRetryDelay)
		}
		ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)
		err = s.do(ctx, "XADD", s.key, "MAXLEN", "~", s.maxLen, "*",
			"v", SchemaVersion, "kind", e.kind, "data", e.data)
		cancel()
		if err == nil {
			return nil
		}
	}
	return err
}

// Close stops accepting entries and waits (bounded) until the queued ones are
// written. Safe on a nil Sink and safe to call more than once.
func (s *Sink) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.closing.Store(true)
		close(s.quit)
	})
	select {
	case <-s.stopped:
	case <-time.After(closeTimeout):
		s.log.Warnw("SIP event stream did not drain before shutdown", nil, "queued", len(s.ch))
	}
}

func (s *Sink) warn(msg string, err error) {
	now := time.Now().UnixNano()
	last := s.lastWarn.Load()
	if now-last < int64(warnInterval) || !s.lastWarn.CompareAndSwap(last, now) {
		return
	}
	s.log.Warnw(msg, err, "key", s.key)
}

// The counter is created once per process: Prometheus refuses a second
// collector with the same name, and tests build several sinks.
var (
	entriesOnce sync.Once
	entriesVec  *prometheus.CounterVec
)

func entriesCounter(nodeID string) *prometheus.CounterVec {
	entriesOnce.Do(func() {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "livekit",
			Subsystem:   "sip",
			Name:        "event_stream_entries_total",
			Help:        "Entries handed to the SIP event stream, by result (published, dropped_full, dropped_error, dropped_closed)",
			ConstLabels: prometheus.Labels{"node_id": nodeID},
		}, []string{"result"})
		if err := prometheus.Register(c); err != nil {
			var already prometheus.AlreadyRegisteredError
			if errors.As(err, &already) {
				if existing, ok := already.ExistingCollector.(*prometheus.CounterVec); ok {
					c = existing
				}
			}
		}
		entriesVec = c
	})
	return entriesVec
}
