// Copyright 2020 New Vector Ltd
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
	"encoding/json"
	"net/http"

	"github.com/antinvestor/gomatrix"
	"github.com/antinvestor/gomatrixserverlib/spec"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

func PeekRoomByIDOrAlias(
	req *http.Request,
	device *api.Device,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	roomIDOrAlias string,
) util.JSONResponse {
	// if this is a remote roomIDOrAlias, we have to ask the roomserver (or federation sender?) to
	// to call /peek and /state on the remote server.
	// TODO: in future we could skip this if we know we're already participating in the room,
	// but this is fiddly in case we stop participating in the room.

	// then we create a local peek.
	peekReq := roomserverAPI.PerformPeekRequest{
		RoomIDOrAlias: roomIDOrAlias,
		UserID:        device.UserID,
		DeviceID:      device.ID,
	}
	// Check to see if any ?server_name= query parameters were
	// given in the request.
	if serverNames, ok := req.URL.Query()["server_name"]; ok {
		for _, serverName := range serverNames {
			peekReq.ServerNames = append(
				peekReq.ServerNames,
				spec.ServerName(serverName),
			)
		}
	}

	// Ask the roomserver to perform the peek.
	roomID, err := rsAPI.PerformPeek(req.Context(), &peekReq)
	switch e := err.(type) {
	case roomserverAPI.ErrInvalidID:
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown(e.Error()),
		}
	case roomserverAPI.ErrNotAllowed:
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(e.Error()),
		}
	case *gomatrix.HTTPError:
		return util.JSONResponse{
			Code: e.Code,
			JSON: json.RawMessage(e.Message),
		}
	case nil:
	default:
		util.Log(req.Context()).WithError(err).WithField("roomID", roomIDOrAlias).Error("Failed to peek room")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// if this user is already joined to the room, we let them peek anyway
	// (given they might be about to part the room, and it makes things less fiddly)

	// Peeking stops if none of the devices who started peeking have been
	// /syncing for a while, or if everyone who was peeking calls /leave
	// (or /unpeek with a server_name param? or DELETE /peek?)
	// on the peeked room.

	return util.JSONResponse{
		Code: http.StatusOK,
		// TODO: Put the response struct somewhere internal.
		JSON: struct {
			RoomID string `json:"room_id"`
		}{roomID},
	}
}

func UnpeekRoomByID(
	req *http.Request,
	device *api.Device,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	roomID string,
) util.JSONResponse {
	err := rsAPI.PerformUnpeek(req.Context(), roomID, device.UserID, device.ID)
	switch e := err.(type) {
	case roomserverAPI.ErrInvalidID:
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown(e.Error()),
		}
	case nil:
	default:
		util.Log(req.Context()).WithError(err).WithField("roomID", roomID).Error("Failed to un-peek room")
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
