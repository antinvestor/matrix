// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package routing

import (
	"fmt"
	"net/http"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/eventutil"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
)

// MakeLeave implements the /make_leave API
func MakeLeave(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	roomID spec.RoomID, userID spec.UserID,
) util.JSONResponse {
	roomVersion, err := rsAPI.QueryRoomVersionForRoom(httpReq.Context(), roomID.String())
	if err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("failed obtaining room version")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	req := api.QueryServerJoinedToRoomRequest{
		ServerName: request.Destination(),
		RoomID:     roomID.String(),
	}
	res := api.QueryServerJoinedToRoomResponse{}
	if err = rsAPI.QueryServerJoinedToRoom(httpReq.Context(), &req, &res); err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("rsAPI.QueryServerJoinedToRoom failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	createLeaveTemplate := func(proto *gomatrixserverlib.ProtoEvent) (gomatrixserverlib.PDU, []gomatrixserverlib.PDU, error) {
		destination := request.Destination()
		identity, signErr := cfg.Global.SigningIdentityFor(destination)
		if signErr != nil {
			util.Log(httpReq.Context()).WithError(signErr).WithField("destination", destination).Error("Failed to obtain signing identity for destination")
			return nil, nil, spec.NotFound(fmt.Sprintf("Server name %q does not exist", destination))
		}

		queryRes := api.QueryLatestEventsAndStateResponse{}
		event, buildErr := eventutil.QueryAndBuildEvent(httpReq.Context(), proto, identity, time.Now(), rsAPI, &queryRes)
		switch e := buildErr.(type) {
		case nil:
		case eventutil.ErrRoomNoExists:
			util.Log(httpReq.Context()).WithError(buildErr).Error("eventutil.BuildEvent failed")
			return nil, nil, spec.NotFound("Room does not exist")
		case gomatrixserverlib.BadJSONError:
			util.Log(httpReq.Context()).WithError(buildErr).Error("eventutil.BuildEvent failed")
			return nil, nil, spec.BadJSON(e.Error())
		default:
			util.Log(httpReq.Context()).WithError(buildErr).Error("eventutil.BuildEvent failed")
			return nil, nil, spec.InternalServerError{}
		}

		stateEvents := make([]gomatrixserverlib.PDU, len(queryRes.StateEvents))
		for i, stateEvent := range queryRes.StateEvents {
			stateEvents[i] = stateEvent.PDU
		}
		return event, stateEvents, nil
	}

	senderID, err := rsAPI.QuerySenderIDForUser(httpReq.Context(), roomID, userID)
	if err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("rsAPI.QuerySenderIDForUser failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	} else if senderID == nil {
		util.Log(httpReq.Context()).WithField("roomID", roomID).WithField("userID", userID).Error("rsAPI.QuerySenderIDForUser returned nil sender ID")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	input := gomatrixserverlib.HandleMakeLeaveInput{
		UserID:             userID,
		SenderID:           *senderID,
		RoomID:             roomID,
		RoomVersion:        roomVersion,
		RequestOrigin:      request.Origin(),
		LocalServerName:    cfg.Global.ServerName,
		LocalServerInRoom:  res.RoomExists && res.IsInRoom,
		BuildEventTemplate: createLeaveTemplate,
		UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
		},
	}

	response, internalErr := gomatrixserverlib.HandleMakeLeave(input)
	switch e := internalErr.(type) {
	case nil:
	case spec.InternalServerError:
		util.Log(httpReq.Context()).WithError(internalErr).Error("failed to handle make_leave request")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	case spec.MatrixError:
		util.Log(httpReq.Context()).WithError(internalErr).Error("failed to handle make_leave request")
		code := http.StatusInternalServerError
		switch e.ErrCode {
		case spec.ErrorForbidden:
			code = http.StatusForbidden
		case spec.ErrorNotFound:
			code = http.StatusNotFound
		case spec.ErrorBadJSON:
			code = http.StatusBadRequest
		}

		return util.JSONResponse{
			Code: code,
			JSON: e,
		}
	default:
		util.Log(httpReq.Context()).WithError(internalErr).Error("failed to handle make_leave request")
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("unknown error"),
		}
	}

	if response == nil {
		util.Log(httpReq.Context()).Error("gmsl.HandleMakeLeave returned invalid response")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: map[string]interface{}{
			"event":        response.LeaveTemplateEvent,
			"room_version": response.RoomVersion,
		},
	}
}

// validateLeaveEvent validates the leave event from the request.
// Returns the event and any error encountered during validation.
func validateLeaveEvent(
	_ *http.Request,
	request *fclient.FederationRequest,
	verImpl gomatrixserverlib.IRoomVersion,
	roomID string,
	eventID string,
) (event gomatrixserverlib.PDU, respErr util.JSONResponse) {
	// Decode the event JSON from the request.
	event, err := verImpl.NewEventFromUntrustedJSON(request.Content())
	switch err.(type) {
	case gomatrixserverlib.BadJSONError:
		return nil, util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	case nil:
	default:
		return nil, util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into valid JSON. " + err.Error()),
		}
	}

	// Check that the room ID is correct.
	if event.RoomID().String() != roomID {
		return nil, util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The room ID in the request path must match the room ID in the leave event JSON"),
		}
	}

	// Check that the event ID is correct.
	if event.EventID() != eventID {
		return nil, util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event ID in the request path must match the event ID in the leave event JSON"),
		}
	}

	if event.StateKey() == nil || event.StateKeyEquals("") {
		return nil, util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("No state key was provided in the leave event."),
		}
	}
	if !event.StateKeyEquals(string(event.SenderID())) {
		return nil, util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Event state key must match the event sender."),
		}
	}

	return event, util.JSONResponse{}
}

// validateLeaveSender validates that the sender of the leave event belongs to the server that sent the request.
func validateLeaveSender(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	rsAPI api.FederationRoomserverAPI,
	event gomatrixserverlib.PDU,
) (*spec.UserID, util.JSONResponse) {
	// Check that the sender belongs to the server that is sending us
	// the request. By this point we've already asserted that the sender
	// and the state key are equal so we don't need to check both.
	sender, err := rsAPI.QueryUserIDForSender(httpReq.Context(), event.RoomID(), event.SenderID())
	if err != nil {
		return nil, util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("The sender of the join is invalid"),
		}
	} else if sender.Domain() != request.Origin() {
		return nil, util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("The sender does not match the server that originated the request"),
		}
	}

	return sender, util.JSONResponse{}
}

// checkUserHasLeft checks if the user has already left the room.
// Returns true if the user has already left, false otherwise.
func checkUserHasLeft(
	httpReq *http.Request,
	rsAPI api.FederationRoomserverAPI,
	roomID string,
	event gomatrixserverlib.PDU,
) (bool, util.JSONResponse) {
	// Check if the user has already left. If so, no-op!
	queryReq := &api.QueryLatestEventsAndStateRequest{
		RoomID: roomID,
		StateToFetch: []gomatrixserverlib.StateKeyTuple{
			{
				EventType: spec.MRoomMember,
				StateKey:  *event.StateKey(),
			},
		},
	}
	queryRes := &api.QueryLatestEventsAndStateResponse{}
	err := rsAPI.QueryLatestEventsAndState(httpReq.Context(), queryReq, queryRes)
	if err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("rsAPI.QueryLatestEventsAndState failed")
		return false, util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// The room doesn't exist or we weren't ever joined to it.
	if !queryRes.RoomExists || len(queryRes.StateEvents) == 0 {
		return true, util.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}

	// Check if we're recycling a previous leave event.
	if event.EventID() == queryRes.StateEvents[0].EventID() {
		return true, util.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}

	// We are/were joined/invited/banned or something. Check if we can no-op here.
	if len(queryRes.StateEvents) == 1 {
		if mem, merr := queryRes.StateEvents[0].Membership(); merr == nil && mem == spec.Leave {
			return true, util.JSONResponse{
				Code: http.StatusOK,
				JSON: struct{}{},
			}
		}
	}

	return false, util.JSONResponse{}
}

// verifyLeaveEventSignature verifies that the leave event is signed by the server sending the request.
func verifyLeaveEventSignature(
	httpReq *http.Request,
	verImpl gomatrixserverlib.IRoomVersion,
	keys gomatrixserverlib.JSONVerifier,
	event gomatrixserverlib.PDU,
	sender *spec.UserID,
) util.JSONResponse {
	// Check that the event is signed by the server sending the request.
	redactedJSON, err := verImpl.RedactEventJSON(event.JSON())
	if err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("XXX: leave.go")
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event JSON could not be redacted"),
		}
	}
	verifyRequests := []gomatrixserverlib.VerifyJSONRequest{{
		ServerName:           sender.Domain(),
		Message:              redactedJSON,
		AtTS:                 event.OriginServerTS(),
		ValidityCheckingFunc: gomatrixserverlib.StrictValiditySignatureCheck,
	}}
	verifyResults, err := keys.VerifyJSONs(httpReq.Context(), verifyRequests)
	if err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("keys.VerifyJSONs failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if verifyResults[0].Error != nil {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("The leave must be signed by the server it originated on"),
		}
	}

	return util.JSONResponse{}
}

// checkMembershipIsLeave checks that the membership in the event is set to leave.
func checkMembershipIsLeave(
	httpReq *http.Request,
	event gomatrixserverlib.PDU,
) util.JSONResponse {
	// check membership is set to leave
	mem, err := event.Membership()
	if err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("event.Membership failed")
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("missing content.membership key"),
		}
	}
	if mem != spec.Leave {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The membership in the event content must be set to leave"),
		}
	}

	return util.JSONResponse{}
}

// SendLeave implements the /send_leave API
func SendLeave(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	keys gomatrixserverlib.JSONVerifier,
	roomID string,
	eventID string,
) util.JSONResponse {
	roomVersion, err := rsAPI.QueryRoomVersionForRoom(httpReq.Context(), roomID)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedRoomVersion(err.Error()),
		}
	}

	verImpl, err := gomatrixserverlib.GetRoomVersion(roomVersion)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.UnsupportedRoomVersion(
				fmt.Sprintf("QueryRoomVersionForRoom returned unknown version: %s", roomVersion),
			),
		}
	}

	// Validate the leave event
	event, jsonResp := validateLeaveEvent(httpReq, request, verImpl, roomID, eventID)
	if jsonResp.Code != 0 {
		return jsonResp
	}

	// Validate the leave sender
	sender, jsonResp := validateLeaveSender(httpReq, request, rsAPI, event)
	if jsonResp.Code != 0 {
		return jsonResp
	}

	// Check if the user has already left
	hasLeft, jsonResp := checkUserHasLeft(httpReq, rsAPI, roomID, event)
	if jsonResp.Code != 0 {
		return jsonResp
	}
	if hasLeft {
		return jsonResp
	}

	// Verify the leave event signature
	jsonResp = verifyLeaveEventSignature(httpReq, verImpl, keys, event, sender)
	if jsonResp.Code != 0 {
		return jsonResp
	}

	// Check membership is set to leave
	jsonResp = checkMembershipIsLeave(httpReq, event)
	if jsonResp.Code != 0 {
		return jsonResp
	}

	// Send the events to the room server.
	// We are responsible for notifying other servers that the user has left
	// the room, so set SendAsServer to cfg.Global.ServerName
	var response api.InputRoomEventsResponse
	rsAPI.InputRoomEvents(httpReq.Context(), &api.InputRoomEventsRequest{
		InputRoomEvents: []api.InputRoomEvent{
			{
				Kind:          api.KindNew,
				Event:         &types.HeaderedEvent{PDU: event},
				SendAsServer:  string(cfg.Global.ServerName),
				TransactionID: nil,
			},
		},
	}, &response)

	if response.ErrMsg != "" {
		util.Log(httpReq.Context()).WithField("error", response.ErrMsg).WithField("not_allowed", response.NotAllowed).Error("producer.SendEvents failed")
		if response.NotAllowed {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Forbidden(response.ErrMsg),
			}
		}
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}
