// Tilbyderen fork: every configuration key that upstream livekit/sip does not
// have lives in this file. Config embeds TilbyderenConfig inline, so the keys
// stay top-level in the YAML file and are read as conf.<Field>, while
// upstream's own field block in config.go is left exactly as upstream wrote
// it (no merge conflicts there when upstream adds or realigns a field).
//
// The config file is parsed non-strictly, so a stock upstream image ignores
// all of these keys.

package config

// TilbyderenConfig holds the fork-only keys. All default to upstream
// behaviour when absent.
type TilbyderenConfig struct {
	// OutboundRegistrations are SIP REGISTER bindings kept alive from startup,
	// one per entry. See OutboundRegistration and pkg/sip/registrar.go.
	OutboundRegistrations []OutboundRegistration `yaml:"outbound_registrations,omitempty"`
}

// OutboundRegistration tells LK-SIP to keep an active SIP REGISTER
// with a remote registrar so the registrar (carrier-side SBC) knows
// where to deliver inbound INVITEs for this account.
//
// Without this, providers that route inbound calls to the registered
// Contact URI (Telavox and most Nordic ITSPs do) have no idea where to
// send the INVITE and the call silently dies. Historically you'd run a
// sidecar like FreeSWITCH (sofia `register=true`) or Asterisk
// (`type=registration`) just to keep this REGISTER alive — this config
// folds that role into LK-SIP itself.
//
// Each entry runs as its own goroutine: REGISTER on startup, handle
// 401 digest challenge, re-register 30 s before the server-honored
// expiry, retry with backoff on failure.
type OutboundRegistration struct {
	// Name is a short label used in logs and metrics. Should be unique
	// across the list. Example: "telavox-prod".
	Name string `yaml:"name"`

	// RegistrarHost is the SIP registrar to send REGISTER to, without
	// scheme. May include a port (`sip.example.com:5060`). Example:
	// sip.telavox.se.
	RegistrarHost string `yaml:"registrar_host"`

	// RegistrarPort is the UDP port to send REGISTER to. Defaults to
	// 5060 (or 5061 for TLS). Ignored if RegistrarHost contains an
	// explicit port.
	RegistrarPort int `yaml:"registrar_port"`

	// Transport: "udp" (default), "tcp", or "tls". Most carriers
	// require UDP for REGISTER. Falls back to UDP if unset.
	Transport string `yaml:"transport"`

	// AuthUsername and AuthPassword: digest auth credentials. Realm
	// defaults to the host portion of RegistrarHost; override with
	// AuthRealm if the carrier returns a different realm in the 401
	// challenge.
	AuthUsername string `yaml:"auth_username"`
	AuthPassword string `yaml:"auth_password"`
	AuthRealm    string `yaml:"auth_realm,omitempty"`

	// ContactUser overrides the user-part of the Contact URI we
	// advertise in REGISTER (and in subsequent in-dialog Contact
	// headers for calls on this trunk). Defaults to AuthUsername.
	// Carriers like Telavox use the Contact to route inbound INVITEs
	// AND validate it against the registered account on in-dialog
	// traffic — keep this matching the AuthUsername unless your
	// provider documents otherwise.
	ContactUser string `yaml:"contact_user,omitempty"`

	// ExpiresSec is the registration lifetime we request. The server
	// may downgrade this in the 200 OK; we honor whatever it returns
	// and re-register 30 s before it expires. Defaults to 3600.
	ExpiresSec int `yaml:"expires_sec,omitempty"`

	// From overrides the From URI user-part if set. Defaults to
	// AuthUsername. Useful if the carrier expects a From distinct
	// from the auth account (rare).
	From string `yaml:"from,omitempty"`
}
