// Copyright 2022 The Global.org Foundation C.I.C.
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
	"fmt"
	"net/http"

	"buf.build/gen/go/antinvestor/presence/connectrpc/go/presencev1connect"
	presenceV1 "buf.build/gen/go/antinvestor/presence/protocolbuffers/go"
	"connectrpc.com/connect"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/httputil"
	"github.com/antinvestor/matrix/clientapi/producers"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

type presenceReq struct {
	Presence  string  `json:"presence"`
	StatusMsg *string `json:"status_msg,omitempty"`
}

func SetPresence(
	req *http.Request,
	cfg *config.ClientAPI,
	device *api.Device,
	producer *producers.SyncAPIProducer,
	userID string,
) util.JSONResponse {
	if !cfg.Global.Presence.EnableOutbound {
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}
	if device.UserID != userID {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Unable to set presence for other user."),
		}
	}
	var presence presenceReq
	parseErr := httputil.UnmarshalJSONRequest(req, &presence)
	if parseErr != nil {
		return *parseErr
	}

	presenceStatus, ok := types.PresenceFromString(presence.Presence)
	if !ok {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown(fmt.Sprintf("Unknown presence '%s'.", presence.Presence)),
		}
	}
	err := producer.SendPresence(req.Context(), userID, presenceStatus, presence.StatusMsg)
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("failed to update presence")
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

func GetPresence(
	req *http.Request,
	device *api.Device,
	client presencev1connect.PresenceServiceClient,
	userID string,
) util.JSONResponse {

	ctx := req.Context()

	resp, err := client.GetPresence(ctx, connect.NewRequest(&presenceV1.GetPresenceRequest{
		UserId: userID,
	}))

	if err != nil {
		util.Log(ctx).WithError(err).Error("unable to get presence")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	presence := resp.Msg
	statusMsg := presence.GetStatusMsg()
	lastActive := presence.GetLastActiveTs()

	p := types.PresenceInternal{LastActiveTS: spec.Timestamp(lastActive)}
	currentlyActive := p.CurrentlyActive()
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: types.PresenceClientResponse{
			CurrentlyActive: &currentlyActive,
			LastActiveAgo:   p.LastActiveAgo(),
			Presence:        presence.GetPresence(),
			StatusMsg:       &statusMsg,
		},
	}
}
