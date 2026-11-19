package sip

import (
	"net/netip"
	"testing"

	"github.com/livekit/sipgo/sip"
	"github.com/stretchr/testify/require"

	"github.com/livekit/sip/pkg/config"
)

func TestContactUser(t *testing.T) {
	require.Equal(t, "dialled", contactUser(&config.Config{}, "dialled"))
	require.Equal(t, "dialled", contactUser(nil, "dialled"))
	require.Equal(t, "", contactUser(&config.Config{}, ""))
	fixed := &config.Config{}
	fixed.SIPContactUser = "u0047"
	require.Equal(t, "u0047", contactUser(fixed, "dialled"))

	require.Equal(t, "", fromUser(nil))
	require.Equal(t, "+4712345678", fromUser(&sip.FromHeader{Address: sip.Uri{User: "+4712345678", Host: "example.com"}}))
}

// The user part set on the URI must reach the Contact header as sent.
func TestContactURICarriesUser(t *testing.T) {
	conf := &config.Config{SIPPort: 5060}
	contact := getContactURI(conf, netip.MustParseAddr("203.0.113.7"), TransportUDP)
	require.Equal(t, "", contact.GetContactURI().User, "upstream sends no user part")

	contact.User = contactUser(conf, "u004700000000")
	uri := contact.GetContactURI()
	require.Equal(t, "u004700000000", uri.User)
	require.Equal(t, "203.0.113.7", uri.Host)
	require.Equal(t, 5060, uri.Port)
}
