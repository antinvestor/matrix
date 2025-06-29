// Copyright 2018 New Vector Ltd
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
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
)

// Backfill implements the /backfill federation endpoint.
// https://matrix.org/docs/spec/server_server/unstable.html#get-matrix-federation-v1-backfill-roomid
func Backfill(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	rsAPI api.FederationRoomserverAPI,
	roomID string,
	cfg *config.FederationAPI,
) util.JSONResponse {
	var res api.PerformBackfillResponse
	var eIDs []string
	var limit string
	var exists bool
	var err error

	// Check the room ID's format.
	if _, _, err = gomatrixserverlib.SplitID('!', roomID); err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Bad room ID: " + err.Error()),
		}
	}

	// If we don't think we belong to this room then don't waste the effort
	// responding to expensive requests for it.
	if err := ErrorIfLocalServerNotInRoom(httpReq.Context(), rsAPI, roomID); err != nil {
		return *err
	}

	// Check if all of the required parameters are there.
	eIDs, exists = httpReq.URL.Query()["v"]
	if !exists {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("v is missing"),
		}
	}
	limit = httpReq.URL.Query().Get("limit")
	if len(limit) == 0 {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("limit is missing"),
		}
	}

	// Populate the request.
	req := api.PerformBackfillRequest{
		RoomID: roomID,
		// we don't know who the successors are for these events, which won't
		// be a problem because we don't use that information when servicing /backfill requests,
		// only when making them. TODO: Think of a better API shape
		BackwardsExtremities: map[string][]string{
			"": eIDs,
		},
		ServerName:  request.Origin(),
		VirtualHost: request.Destination(),
	}
	if req.Limit, err = strconv.Atoi(limit); err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("strconv.Atoi failed")
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(fmt.Sprintf("limit %q is invalid format", limit)),
		}
	}

	// Enforce a limit of 100 events, as not to hit the Cm to hard.
	// Synapse has a hard limit of 100 events as well.
	if req.Limit > 100 {
		req.Limit = 100
	}

	// Query the Roomserver.
	if err = rsAPI.PerformBackfill(httpReq.Context(), &req, &res); err != nil {
		util.Log(httpReq.Context()).WithError(err).Error("query.PerformBackfill failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Filter any event that's not from the requested room out.
	evs := make([]gomatrixserverlib.PDU, 0)

	var ev *types.HeaderedEvent
	for _, ev = range res.Events {
		if ev.RoomID().String() == roomID {
			evs = append(evs, ev.PDU)
		}
	}

	var eventJSONs []json.RawMessage
	for _, e := range gomatrixserverlib.ReverseTopologicalOrdering(
		evs,
		gomatrixserverlib.TopologicalOrderByPrevEvents,
	) {
		eventJSONs = append(eventJSONs, e.JSON())
	}

	// sytest wants these in reversed order, similar to /messages, so reverse them now.
	for i := len(eventJSONs)/2 - 1; i >= 0; i-- {
		opp := len(eventJSONs) - 1 - i
		eventJSONs[i], eventJSONs[opp] = eventJSONs[opp], eventJSONs[i]
	}

	txn := gomatrixserverlib.Transaction{
		Origin:         request.Destination(),
		PDUs:           eventJSONs,
		OriginServerTS: spec.AsTimestamp(time.Now()),
	}

	// Send the events to the client.
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: txn,
	}
}
