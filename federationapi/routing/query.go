// Copyright 2017 New Vector Ltd
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
	"errors"
	"fmt"
	"net/http"

	"github.com/antinvestor/gomatrix"
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	federationAPI "github.com/antinvestor/matrix/federationapi/api"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
)

// RoomAliasToID converts the queried alias into a room ID and returns it
func RoomAliasToID(
	httpReq *http.Request,
	federation fclient.FederationClient,
	cfg *config.FederationAPI,
	rsAPI roomserverAPI.FederationRoomserverAPI,
	senderAPI federationAPI.FederationInternalAPI,
) util.JSONResponse {
	roomAlias := httpReq.FormValue("room_alias")
	if roomAlias == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Must supply room alias parameter."),
		}
	}
	_, domain, err := gomatrixserverlib.SplitID('#', roomAlias)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Room alias must be in the form '#localpart:domain'"),
		}
	}

	var resp fclient.RespDirectory

	if domain == cfg.Global.ServerName {
		queryReq := &roomserverAPI.GetRoomIDForAliasRequest{
			Alias:              roomAlias,
			IncludeAppservices: true,
		}
		queryRes := &roomserverAPI.GetRoomIDForAliasResponse{}
		if err = rsAPI.GetRoomIDForAlias(httpReq.Context(), queryReq, queryRes); err != nil {
			util.Log(httpReq.Context()).WithError(err).Error("aliasAPI.GetRoomIDForAlias failed")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		if queryRes.RoomID != "" {
			serverQueryReq := federationAPI.QueryJoinedHostServerNamesInRoomRequest{RoomID: queryRes.RoomID}
			var serverQueryRes federationAPI.QueryJoinedHostServerNamesInRoomResponse
			if err = senderAPI.QueryJoinedHostServerNamesInRoom(httpReq.Context(), &serverQueryReq, &serverQueryRes); err != nil {
				util.Log(httpReq.Context()).WithError(err).Error("senderAPI.QueryJoinedHostServerNamesInRoom failed")
				return util.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}

			resp = fclient.RespDirectory{
				RoomID:  queryRes.RoomID,
				Servers: serverQueryRes.ServerNames,
			}
		} else {
			// If no alias was found, return an error
			return util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound(fmt.Sprintf("Room alias %s not found", roomAlias)),
			}
		}
	} else {
		resp, err = federation.LookupRoomAlias(httpReq.Context(), domain, cfg.Global.ServerName, roomAlias)
		if err != nil {
			var x gomatrix.HTTPError
			switch {
			case errors.As(err, &x):
				if x.Code == http.StatusNotFound {
					return util.JSONResponse{
						Code: http.StatusNotFound,
						JSON: spec.NotFound("Room alias not found"),
					}
				}
			}
			// TODO: Return 502 if the remote server errored.
			// TODO: Return 504 if the remote server timed out.
			util.Log(httpReq.Context()).WithError(err).Error("federation.LookupRoomAlias failed")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: resp,
	}
}

// Query the immediate children of a room/space
//
// Implements /_matrix/federation/v1/hierarchy/{roomID}
func QueryRoomHierarchy(httpReq *http.Request, request *fclient.FederationRequest, roomIDStr string, rsAPI roomserverAPI.FederationRoomserverAPI) util.JSONResponse {

	ctx := httpReq.Context()

	parsedRoomID, err := spec.NewRoomID(roomIDStr)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.InvalidParam("room is unknown/forbidden"),
		}
	}
	roomID := *parsedRoomID

	suggestedOnly := false // Defaults to false (spec-defined)
	switch httpReq.URL.Query().Get("suggested_only") {
	case "true":
		suggestedOnly = true
	case "false":
	case "": // Empty string is returned when query param is not set
	default:
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("query parameter 'suggested_only', if set, must be 'true' or 'false'"),
		}
	}

	walker := roomserverAPI.NewRoomHierarchyWalker(types.NewServerNameNotDevice(request.Origin()), roomID, suggestedOnly, 1)
	discoveredRooms, inaccessibleRooms, _, err := rsAPI.QueryNextRoomHierarchyPage(httpReq.Context(), walker, -1)

	if err != nil {
		var errRoomUnknownOrNotAllowed roomserverAPI.ErrRoomUnknownOrNotAllowed
		switch {
		case errors.As(err, &errRoomUnknownOrNotAllowed):
			util.Log(ctx).WithError(err).Debug("room unknown/forbidden when handling SS room hierarchy request")
			return util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("room is unknown/forbidden"),
			}
		default:
			util.Log(ctx).WithError(err).Error("failed to fetch next page of room hierarchy (SS API)")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.Unknown("internal server error"),
			}
		}
	}

	if len(discoveredRooms) == 0 {
		util.Log(httpReq.Context()).Debug("no rooms found when handling SS room hierarchy request")
		return util.JSONResponse{
			Code: 404,
			JSON: spec.NotFound("room is unknown/forbidden"),
		}
	}
	return util.JSONResponse{
		Code: 200,
		JSON: fclient.RoomHierarchyResponse{
			Room:                 discoveredRooms[0],
			Children:             discoveredRooms[1:],
			InaccessibleChildren: inaccessibleRooms,
		},
	}
}
