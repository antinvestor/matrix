// Copyright 2017 Vector Creations Ltd
//
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
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
)

// InviteV3 implements /_matrix/federation/v2/invite/{roomID}/{userID}
func InviteV3(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	roomID spec.RoomID,
	invitedUser spec.UserID,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	keys gomatrixserverlib.JSONVerifier,
) util.JSONResponse {
	inviteReq := fclient.InviteV3Request{}
	err := json.Unmarshal(request.Content(), &inviteReq)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	}
	if !cfg.Global.IsLocalServerName(invitedUser.Domain()) {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The invited user domain does not belong to this server"),
		}
	}

	input := gomatrixserverlib.HandleInviteV3Input{
		HandleInviteInput: gomatrixserverlib.HandleInviteInput{
			RoomVersion:       inviteReq.RoomVersion(),
			RoomID:            roomID,
			InvitedUser:       invitedUser,
			KeyID:             cfg.Global.KeyID,
			PrivateKey:        cfg.Global.PrivateKey,
			Verifier:          keys,
			RoomQuerier:       rsAPI,
			MembershipQuerier: &api.MembershipQuerier{Roomserver: rsAPI},
			StateQuerier:      rsAPI.StateQuerier(),
			InviteEvent:       nil,
			StrippedState:     inviteReq.InviteRoomState(),
			UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
			},
		},
		InviteProtoEvent: inviteReq.Event(),
		GetOrCreateSenderID: func(ctx context.Context, userID spec.UserID, roomID spec.RoomID, roomVersion string) (spec.SenderID, ed25519.PrivateKey, error) {
			// assign a roomNID, otherwise we can't create a private key for the user
			_, nidErr := rsAPI.AssignRoomNID(ctx, roomID, gomatrixserverlib.RoomVersion(roomVersion))
			if nidErr != nil {
				return "", nil, nidErr
			}
			key, keyErr := rsAPI.GetOrCreateUserRoomPrivateKey(ctx, userID, roomID)
			if keyErr != nil {
				return "", nil, keyErr
			}

			return spec.SenderIDFromPseudoIDKey(key), key, nil
		},
	}
	event, jsonErr := handleInviteV3(httpReq.Context(), input, rsAPI)
	if jsonErr != nil {
		return *jsonErr
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespInviteV2{Event: event.JSON()},
	}
}

// InviteV2 implements /_matrix/federation/v2/invite/{roomID}/{eventID}
func InviteV2(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	roomID spec.RoomID,
	eventID string,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	keys gomatrixserverlib.JSONVerifier,
) util.JSONResponse {
	inviteReq := fclient.InviteV2Request{}
	err := json.Unmarshal(request.Content(), &inviteReq)
	switch e := err.(type) {
	case gomatrixserverlib.UnsupportedRoomVersionError:
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedRoomVersion(
				fmt.Sprintf("Room version %q is not supported by this server.", e.Version),
			),
		}
	case gomatrixserverlib.BadJSONError:
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	case nil:
		if inviteReq.Event().StateKey() == nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("The invite event has no state key"),
			}
		}

		invitedUser, userErr := spec.NewUserID(*inviteReq.Event().StateKey(), true)
		if userErr != nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("The user ID is invalid"),
			}
		}
		if !cfg.Global.IsLocalServerName(invitedUser.Domain()) {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("The invited user domain does not belong to this server"),
			}
		}

		if inviteReq.Event().EventID() != eventID {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("The event ID in the request path must match the event ID in the invite event JSON"),
			}
		}

		input := gomatrixserverlib.HandleInviteInput{
			RoomVersion:       inviteReq.RoomVersion(),
			RoomID:            roomID,
			InvitedUser:       *invitedUser,
			KeyID:             cfg.Global.KeyID,
			PrivateKey:        cfg.Global.PrivateKey,
			Verifier:          keys,
			RoomQuerier:       rsAPI,
			MembershipQuerier: &api.MembershipQuerier{Roomserver: rsAPI},
			StateQuerier:      rsAPI.StateQuerier(),
			InviteEvent:       inviteReq.Event(),
			StrippedState:     inviteReq.InviteRoomState(),
			UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
			},
		}
		event, jsonErr := handleInvite(httpReq.Context(), input, rsAPI)
		if jsonErr != nil {
			return *jsonErr
		}
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: fclient.RespInviteV2{Event: event.JSON()},
		}
	default:
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into an invite request. " + err.Error()),
		}
	}
}

// InviteV1 implements /_matrix/federation/v1/invite/{roomID}/{eventID}
func InviteV1(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	roomID spec.RoomID,
	eventID string,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	keys gomatrixserverlib.JSONVerifier,
) util.JSONResponse {
	roomVer := gomatrixserverlib.RoomVersionV1
	body := request.Content()
	// roomVer is hardcoded to v1 so we know we won't panic on Must
	event, err := gomatrixserverlib.MustGetRoomVersion(roomVer).NewEventFromTrustedJSON(body, false)
	switch err.(type) {
	case gomatrixserverlib.BadJSONError:
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	case nil:
	default:
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into an invite v1 request. " + err.Error()),
		}
	}
	var strippedState []gomatrixserverlib.InviteStrippedState
	if jsonErr := json.Unmarshal(event.Unsigned(), &strippedState); jsonErr != nil {
		// just warn, they may not have added any.
		util.Log(httpReq.Context()).Warn("failed to extract stripped state from invite event")
	}

	if event.StateKey() == nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The invite event has no state key"),
		}
	}

	invitedUser, err := spec.NewUserID(*event.StateKey(), true)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The user ID is invalid"),
		}
	}
	if !cfg.Global.IsLocalServerName(invitedUser.Domain()) {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The invited user domain does not belong to this server"),
		}
	}

	if event.EventID() != eventID {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event ID in the request path must match the event ID in the invite event JSON"),
		}
	}

	input := gomatrixserverlib.HandleInviteInput{
		RoomVersion:       roomVer,
		RoomID:            roomID,
		InvitedUser:       *invitedUser,
		KeyID:             cfg.Global.KeyID,
		PrivateKey:        cfg.Global.PrivateKey,
		Verifier:          keys,
		RoomQuerier:       rsAPI,
		MembershipQuerier: &api.MembershipQuerier{Roomserver: rsAPI},
		StateQuerier:      rsAPI.StateQuerier(),
		InviteEvent:       event,
		StrippedState:     strippedState,
		UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
		},
	}
	event, jsonErr := handleInvite(httpReq.Context(), input, rsAPI)
	if jsonErr != nil {
		return *jsonErr
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespInvite{Event: event.JSON()},
	}
}

func handleInvite(ctx context.Context, input gomatrixserverlib.HandleInviteInput, rsAPI api.FederationRoomserverAPI) (gomatrixserverlib.PDU, *util.JSONResponse) {
	inviteEvent, err := gomatrixserverlib.HandleInvite(ctx, input)
	return handleInviteResult(ctx, inviteEvent, err, rsAPI)
}

func handleInviteV3(ctx context.Context, input gomatrixserverlib.HandleInviteV3Input, rsAPI api.FederationRoomserverAPI) (gomatrixserverlib.PDU, *util.JSONResponse) {
	inviteEvent, err := gomatrixserverlib.HandleInviteV3(ctx, input)
	return handleInviteResult(ctx, inviteEvent, err, rsAPI)
}

func handleInviteResult(ctx context.Context, inviteEvent gomatrixserverlib.PDU, err error, rsAPI api.FederationRoomserverAPI) (gomatrixserverlib.PDU, *util.JSONResponse) {
	switch e := err.(type) {
	case nil:
	case spec.InternalServerError:
		util.Log(ctx).WithError(err)
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	case spec.MatrixError:
		util.Log(ctx).WithError(err)
		code := http.StatusInternalServerError
		switch e.ErrCode {
		case spec.ErrorForbidden:
			code = http.StatusForbidden
		case spec.ErrorUnsupportedRoomVersion:
			fallthrough // http.StatusBadRequest
		case spec.ErrorBadJSON:
			code = http.StatusBadRequest
		}

		return nil, &util.JSONResponse{
			Code: code,
			JSON: e,
		}
	default:
		util.Log(ctx).WithError(err)
		return nil, &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("unknown error"),
		}
	}

	headeredInvite := &types.HeaderedEvent{PDU: inviteEvent}
	if err = rsAPI.HandleInvite(ctx, headeredInvite); err != nil {
		util.Log(ctx).WithError(err).Error("HandleInvite failed")
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return inviteEvent, nil

}
