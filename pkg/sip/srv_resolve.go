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

package sip

import (
	"context"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// RFC 3263 server resolution for outbound SIP targets.
//
// Why this exists: sipgo's transport layer resolves a target by calling
// net.ResolveIPAddr first and only falls back to SRV when that fails
// (transport/layer.go resolveAddr). Because a carrier's SIP domain almost
// always also has an A record, the SRV branch is unreachable in practice, and
// even when reached it takes addrs[0] without honouring priority/weight and
// feeds the SRV target *hostname* into net.ParseIP (which yields nil).
//
// The consequence is that we pin every call to a single A-record address with
// no failover. On 2026-08-03 that address (the carrier's trunk VIP) degraded
// and outbound calling went to zero while the carrier's own SRV pool was
// reachable. See the SRV_FAILOVER notes in the deploy runbook.
//
// We therefore resolve targets ourselves and hand sipgo a literal ip:port via
// Request.SetDestination, which short-circuits its resolver entirely.

const (
	defaultSIPPort    = 5060
	defaultSIPTLSPort = 5061

	// srvCacheTTL bounds how often we hit DNS. Deliberately short: the whole
	// point is to react when a carrier pulls a proxy out of rotation.
	srvCacheTTL = 30 * time.Second
)

// Target is one resolved candidate to send a request to.
type Target struct {
	// Addr is "ip:port" and is what goes into Request.SetDestination.
	Addr string
	// Host is the SRV target (or the original host) purely for logging.
	Host string
}

type srvCacheEntry struct {
	targets []Target
	expires time.Time
}

var (
	srvCacheMu sync.Mutex
	srvCache   = make(map[string]srvCacheEntry)
)

// srvService maps a SIP transport to the RFC 3263 service/proto labels.
// Returns ok=false for transports that have no SRV form (ws/wss are
// discovered out of band, not via _sip._udp and friends).
func srvService(transport string) (service, proto string, ok bool) {
	switch strings.ToLower(transport) {
	case "", "udp":
		return "sip", "udp", true
	case "tcp":
		return "sip", "tcp", true
	case "tls":
		return "sips", "tcp", true
	default:
		return "", "", false
	}
}

func defaultPortFor(transport string) int {
	if strings.EqualFold(transport, "tls") {
		return defaultSIPTLSPort
	}
	return defaultSIPPort
}

// sortSRV orders records by ascending priority, and within a priority group
// performs the weighted random selection described in RFC 2782. Records with
// weight 0 still participate; they simply have a low chance of winning.
func sortSRV(recs []*net.SRV, rnd *rand.Rand) []*net.SRV {
	sort.SliceStable(recs, func(i, j int) bool {
		return recs[i].Priority < recs[j].Priority
	})

	out := make([]*net.SRV, 0, len(recs))
	for i := 0; i < len(recs); {
		j := i
		for j < len(recs) && recs[j].Priority == recs[i].Priority {
			j++
		}
		group := make([]*net.SRV, j-i)
		copy(group, recs[i:j])

		// Repeatedly draw one record from the group, weighted by Weight.
		for len(group) > 0 {
			total := 0
			for _, r := range group {
				total += int(r.Weight)
			}
			pick := 0
			if total > 0 {
				target := rnd.Intn(total + 1)
				run := 0
				for k, r := range group {
					run += int(r.Weight)
					if run >= target {
						pick = k
						break
					}
				}
			} else {
				pick = rnd.Intn(len(group))
			}
			out = append(out, group[pick])
			group = append(group[:pick], group[pick+1:]...)
		}
		i = j
	}
	return out
}

// ResolveTargets returns an ordered list of candidates for host, following
// RFC 3263: a literal IP or an explicit port short-circuits SRV; otherwise we
// try SRV and fall back to A/AAAA. The list is ordered best-first and callers
// should walk it on transport failure.
//
// It never returns an empty list without an error.
func ResolveTargets(ctx context.Context, res *net.Resolver, host string, port int, transport string) ([]Target, error) {
	if res == nil {
		res = net.DefaultResolver
	}
	if port == 0 {
		port = defaultPortFor(transport)
	}

	// A literal IP is already a final answer.
	if ip := net.ParseIP(host); ip != nil {
		return []Target{{Addr: net.JoinHostPort(ip.String(), itoa(port)), Host: host}}, nil
	}

	// RFC 3263 s4.2: an explicit port means no SRV lookup.
	explicitPort := port != defaultPortFor(transport)

	cacheKey := host + "|" + strings.ToLower(transport) + "|" + itoa(port)
	if t, ok := srvCacheGet(cacheKey); ok {
		return t, nil
	}

	var targets []Target
	if service, proto, ok := srvService(transport); ok && !explicitPort {
		if _, recs, err := res.LookupSRV(ctx, service, proto, host); err == nil && len(recs) > 0 {
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			for _, rec := range sortSRV(recs, rnd) {
				// SRV targets are host names; they still need A resolution.
				name := strings.TrimSuffix(rec.Target, ".")
				if name == "" || name == "." {
					continue // RFC 2782 "." means explicitly no service here
				}
				ips, err := res.LookupIPAddr(ctx, name)
				if err != nil {
					continue
				}
				for _, ip := range ips {
					targets = append(targets, Target{
						Addr: net.JoinHostPort(ip.IP.String(), itoa(int(rec.Port))),
						Host: name,
					})
				}
			}
		}
	}

	// Fallback (and the explicit-port path): plain address records.
	if len(targets) == 0 {
		ips, err := res.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, ip := range ips {
			targets = append(targets, Target{
				Addr: net.JoinHostPort(ip.IP.String(), itoa(port)),
				Host: host,
			})
		}
	}

	if len(targets) == 0 {
		return nil, &net.DNSError{Err: "no addresses found", Name: host, IsNotFound: true}
	}

	srvCachePut(cacheKey, targets)
	return targets, nil
}

func srvCacheGet(key string) ([]Target, bool) {
	srvCacheMu.Lock()
	defer srvCacheMu.Unlock()
	e, ok := srvCache[key]
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	out := make([]Target, len(e.targets))
	copy(out, e.targets)
	return out, true
}

func srvCachePut(key string, targets []Target) {
	cp := make([]Target, len(targets))
	copy(cp, targets)
	srvCacheMu.Lock()
	defer srvCacheMu.Unlock()
	srvCache[key] = srvCacheEntry{targets: cp, expires: time.Now().Add(srvCacheTTL)}
}

// srvCacheInvalidate drops a cached entry so the next call re-queries DNS.
// Used when every candidate failed, so a rotation change is picked up promptly.
func srvCacheInvalidate(host, transport string, port int) {
	if port == 0 {
		port = defaultPortFor(transport)
	}
	key := host + "|" + strings.ToLower(transport) + "|" + itoa(port)
	srvCacheMu.Lock()
	defer srvCacheMu.Unlock()
	delete(srvCache, key)
}

func itoa(i int) string {
	// strconv without the import churn in this file's hot path.
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
