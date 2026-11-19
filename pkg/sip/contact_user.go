// Tilbyderen fork: user part of the Contact header.
//
// Upstream builds the Contact as sip:<signaling ip>:<port>;transport=<t>, with
// no user part. Telavox's SBC silently dropped the in-dialog ACK for our 200 OK
// to an inbound INVITE when the Contact had no user part, so inbound calls
// ended with "no ACK". Asterisk and FreeSWITCH both send a user part.
//
// The two places that build a Contact (newInbound in inbound.go, newCall in
// outbound.go) set URI.User from the helpers below; nothing else changes.

package sip

import (
	"github.com/livekit/sipgo/sip"

	"github.com/livekit/sip/pkg/config"
)

// contactUser returns the user part to put in a Contact URI: the configured
// sip_contact_user when set, otherwise fallback (the To user of an inbound
// INVITE, or our From user on an outbound one).
func contactUser(conf *config.Config, fallback string) string {
	if conf != nil && conf.SIPContactUser != "" {
		return conf.SIPContactUser
	}
	return fallback
}

// fromUser returns the user part of a From header, or "" when there is none.
func fromUser(from *sip.FromHeader) string {
	if from == nil {
		return ""
	}
	return from.Address.User
}
