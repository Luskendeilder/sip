// Tilbyderen fork extension: MoveSIPParticipant HTTP endpoint.
//
// Exposes POST /admin/move-sip-participant on the existing health server
// so we can swap a SIP participant from one room to another while
// keeping the carrier-side SIP/RTP session alive.
//
// This sidesteps the missing MoveParticipant API in LiveKit OSS server
// (returns "not implemented" — Cloud-only). See docs/MOVE-SIP-PARTICIPANT.md
// in this repo for the full design.

package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/livekit/sip/pkg/sip"
)

// MoveSIPParticipantRequest is the JSON body for the move endpoint.
//
// Exactly one of SipCallId or ParticipantIdentity must be set:
//   - SipCallId          : LiveKit-SIP's internal SCL_ id (==LocalTag).
//                          O(1) lookup, but caller must have persisted
//                          the auto-generated value.
//   - ParticipantIdentity : the identity passed to CreateSIPParticipant
//                          (e.g. "sip-{ourCallId}"). O(n) lookup but no
//                          persistence required.
//
// DestinationRoom + DestinationToken describe the room to swap into.
// Caller mints the token with the SIP participant identity and the
// destination room granted. We do no token issuance here.
type MoveSIPParticipantRequest struct {
	SipCallId           string `json:"sip_call_id,omitempty"`
	ParticipantIdentity string `json:"participant_identity,omitempty"`
	DestinationRoom     string `json:"destination_room"`
	DestinationToken    string `json:"destination_token"`
}

// MoveSIPParticipantResponse is currently empty; we may add stats later.
type MoveSIPParticipantResponse struct{}

func (s *Service) handleMoveSIPParticipant(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req MoveSIPParticipantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.SipCallId == "" && req.ParticipantIdentity == "" {
		http.Error(w, "exactly one of sip_call_id or participant_identity is required", http.StatusBadRequest)
		return
	}
	if req.DestinationRoom == "" || req.DestinationToken == "" {
		http.Error(w, "destination_room and destination_token are required", http.StatusBadRequest)
		return
	}

	if s.sipMoveSIPParticipant == nil {
		// Function pointer wasn't wired — likely a misconfigured fork build.
		s.log.Errorw("move-sip-participant requested but handler not wired", nil)
		http.Error(w, "MoveSIPParticipant handler not wired into service", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), moveSIPRequestTimeout)
	defer cancel()

	err := s.sipMoveSIPParticipant(ctx, sip.MoveSIPParticipantQuery{
		SipCallId:           req.SipCallId,
		ParticipantIdentity: req.ParticipantIdentity,
	}, req.DestinationRoom, req.DestinationToken)
	switch {
	case err == nil:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(MoveSIPParticipantResponse{})

	case errors.Is(err, sip.MoveSIPParticipantNotFoundError):
		http.Error(w, err.Error(), http.StatusNotFound)

	default:
		s.log.Warnw("move-sip-participant failed", err,
			"sipCallId", req.SipCallId,
			"destinationRoom", req.DestinationRoom,
		)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// moveSIPRequestTimeout caps the time the HTTP handler waits for the
// SwapRoom call to complete. SwapRoom does an lksdk reconnect to
// LiveKit Server which is normally sub-second; a few seconds gives
// generous margin without leaving HTTP clients hanging on a stuck
// rendezvous.
const moveSIPRequestTimeout = 5 * time.Second
