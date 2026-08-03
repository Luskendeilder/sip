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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSRVServiceMapping(t *testing.T) {
	for _, c := range []struct {
		transport string
		service   string
		proto     string
		ok        bool
	}{
		{"", "sip", "udp", true},
		{"udp", "sip", "udp", true},
		{"UDP", "sip", "udp", true},
		{"tcp", "sip", "tcp", true},
		{"tls", "sips", "tcp", true},
		{"ws", "", "", false},
		{"wss", "", "", false},
	} {
		s, p, ok := srvService(c.transport)
		require.Equal(t, c.ok, ok, "transport %q", c.transport)
		require.Equal(t, c.service, s, "transport %q", c.transport)
		require.Equal(t, c.proto, p, "transport %q", c.transport)
	}
}

func TestDefaultPortFor(t *testing.T) {
	require.Equal(t, defaultSIPPort, defaultPortFor("udp"))
	require.Equal(t, defaultSIPPort, defaultPortFor("tcp"))
	require.Equal(t, defaultSIPTLSPort, defaultPortFor("tls"))
	require.Equal(t, defaultSIPTLSPort, defaultPortFor("TLS"))
}

// Lower priority value must always win, regardless of weight.
func TestSortSRVPriorityOrdering(t *testing.T) {
	recs := []*net.SRV{
		{Target: "low.example.", Priority: 20, Weight: 100, Port: 5060},
		{Target: "high.example.", Priority: 10, Weight: 1, Port: 5060},
		{Target: "mid.example.", Priority: 15, Weight: 50, Port: 5060},
	}
	rnd := rand.New(rand.NewSource(1))
	got := sortSRV(recs, rnd)
	require.Len(t, got, 3)
	require.Equal(t, "high.example.", got[0].Target)
	require.Equal(t, "mid.example.", got[1].Target)
	require.Equal(t, "low.example.", got[2].Target)
}

// Every record in a priority group must appear exactly once, in some order.
func TestSortSRVKeepsAllRecords(t *testing.T) {
	recs := []*net.SRV{
		{Target: "a.example.", Priority: 0, Weight: 30, Port: 5060},
		{Target: "b.example.", Priority: 0, Weight: 30, Port: 5060},
		{Target: "c.example.", Priority: 0, Weight: 30, Port: 5060},
		{Target: "d.example.", Priority: 0, Weight: 30, Port: 5060},
		{Target: "e.example.", Priority: 0, Weight: 30, Port: 5060},
	}
	rnd := rand.New(rand.NewSource(7))
	got := sortSRV(recs, rnd)
	require.Len(t, got, len(recs))
	seen := map[string]int{}
	for _, r := range got {
		seen[r.Target]++
	}
	require.Len(t, seen, len(recs))
	for tgt, n := range seen {
		require.Equal(t, 1, n, "target %s appeared %d times", tgt, n)
	}
}

// Zero-weight records must not be dropped or cause a panic.
func TestSortSRVAllZeroWeight(t *testing.T) {
	recs := []*net.SRV{
		{Target: "a.example.", Priority: 0, Weight: 0, Port: 5060},
		{Target: "b.example.", Priority: 0, Weight: 0, Port: 5060},
	}
	rnd := rand.New(rand.NewSource(3))
	got := sortSRV(recs, rnd)
	require.Len(t, got, 2)
}

// Weighted selection should favour the heavy record over many draws, while
// still returning both.
func TestSortSRVWeightBias(t *testing.T) {
	heavyFirst := 0
	const runs = 400
	for i := 0; i < runs; i++ {
		recs := []*net.SRV{
			{Target: "heavy.example.", Priority: 0, Weight: 99, Port: 5060},
			{Target: "light.example.", Priority: 0, Weight: 1, Port: 5060},
		}
		got := sortSRV(recs, rand.New(rand.NewSource(int64(i))))
		require.Len(t, got, 2)
		if got[0].Target == "heavy.example." {
			heavyFirst++
		}
	}
	require.Greater(t, heavyFirst, runs/2, "heavy record should usually be picked first")
}

// A literal IP short-circuits DNS entirely.
func TestResolveTargetsLiteralIP(t *testing.T) {
	got, err := ResolveTargets(context.Background(), nil, "80.83.208.59", 0, "udp")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "80.83.208.59:5060", got[0].Addr)
}

func TestResolveTargetsLiteralIPExplicitPort(t *testing.T) {
	got, err := ResolveTargets(context.Background(), nil, "80.83.208.59", 5080, "tcp")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "80.83.208.59:5080", got[0].Addr)
}

func TestResolveTargetsLiteralIPTLSDefaultPort(t *testing.T) {
	got, err := ResolveTargets(context.Background(), nil, "10.0.0.1", 0, "tls")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:5061", got[0].Addr)
}

func TestSRVCacheRoundTrip(t *testing.T) {
	key := "cache.example|udp|5060"
	_, ok := srvCacheGet(key)
	require.False(t, ok)

	srvCachePut(key, []Target{{Addr: "1.2.3.4:5060", Host: "a.example"}})
	got, ok := srvCacheGet(key)
	require.True(t, ok)
	require.Len(t, got, 1)
	require.Equal(t, "1.2.3.4:5060", got[0].Addr)

	// Mutating the returned slice must not corrupt the cache.
	got[0].Addr = "9.9.9.9:5060"
	again, ok := srvCacheGet(key)
	require.True(t, ok)
	require.Equal(t, "1.2.3.4:5060", again[0].Addr)

	srvCacheInvalidate("cache.example", "udp", 0)
	_, ok = srvCacheGet(key)
	require.False(t, ok)
}

func TestItoa(t *testing.T) {
	require.Equal(t, "0", itoa(0))
	require.Equal(t, "5060", itoa(5060))
	require.Equal(t, "5061", itoa(5061))
	require.Equal(t, "-7", itoa(-7))
}
