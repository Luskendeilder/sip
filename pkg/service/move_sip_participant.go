// Tilbyderen fork extension: MoveSIPParticipant HTTP endpoint.
//
// Exposes POST /admin/move-sip-participant on the existing health server
// so we can swap a SIP participant from one room to another while
// keeping the carrier-side SIP/RTP session alive.
//
// This sidesteps the missing MoveParticipant API in LiveKit OSS server
// (returns "not implemented" — Cloud-only). See docs/MOVE-SIP-PARTICIPANT.md
// in this repo for the full design.
//
// This file contains the HTTP plumbing only. The actual room-swap logic
// lives in pkg/sip/room.go (Room.SwapToRoom) and is invoked via the call
// worker registries in client.go (outbound) and server.go (inbound).

package service

import (
	"encoding/json"
	"net/http"
)

// MoveSIPParticipantRequest is the JSON body for the move endpoint.
//
// SipCallId identifies the existing SIP call to move — same value that
// CreateSIPParticipant returned and that the caller has been tracking
// alongside our internal Call.id.
//
// DestinationRoom + DestinationToken describe the room to swap into.
// Caller mints the token with the SIP participant identity and the
// destination room granted. We do no token issuance here.
type MoveSIPParticipantRequest struct {
	SipCallId        string `json:"sip_call_id"`
	DestinationRoom  string `json:"destination_room"`
	DestinationToken string `json:"destination_token"`
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
	if req.SipCallId == "" || req.DestinationRoom == "" || req.DestinationToken == "" {
		http.Error(w, "sip_call_id, destination_room, destination_token all required", http.StatusBadRequest)
		return
	}

	// TODO(fork): wire to call worker registries. See docs/MOVE-SIP-PARTICIPANT.md.
	//
	// Pseudocode:
	//   if call := s.client.GetActiveCall(LocalTag(req.SipCallId)); call != nil {
	//       err := call.SwapRoom(r.Context(), req.DestinationRoom, req.DestinationToken)
	//       writeMoveResult(w, err)
	//       return
	//   }
	//   if call := s.server.GetInboundCallByLocalTag(LocalTag(req.SipCallId)); call != nil {
	//       err := call.SwapRoom(r.Context(), req.DestinationRoom, req.DestinationToken)
	//       writeMoveResult(w, err)
	//       return
	//   }
	//   http.Error(w, "no active call with that sip_call_id", http.StatusNotFound)
	s.log.Warnw("MoveSIPParticipant called (stub — not yet implemented)", nil,
		"sip_call_id", req.SipCallId,
		"destination_room", req.DestinationRoom,
	)
	http.Error(w, "MoveSIPParticipant: not yet implemented in fork (see docs/MOVE-SIP-PARTICIPANT.md)", http.StatusNotImplemented)
}
