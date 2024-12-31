// Copyright 2022 The Matrix.org Foundation C.I.C.
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
	"strconv"
	"time"

	"github.com/antinvestor/matrix/clientapi/httputil"
	"github.com/antinvestor/matrix/clientapi/producers"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/pitabwire/util"
	log "github.com/sirupsen/logrus"
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
	if !cfg.Matrix.Presence.EnableOutbound {
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
		log.With(slog.Any("error", err)).Error("failed to update presence")
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
	natsClient *nats.Conn,
	presenceTopic string,
	userID string,
) util.JSONResponse {
	msg := nats.NewMsg(presenceTopic)
	msg.Header.Set(jetstream.UserID, userID)

	presence, err := natsClient.RequestMsg(msg, time.Second*10)
	if err != nil {
		log.With(slog.Any("error", err)).Error("unable to get presence")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	statusMsg := presence.Header.Get("status_msg")
	e := presence.Header.Get("error")
	if e != "" {
		log.Error("received error msg from nats: %s", e)
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: types.PresenceClientResponse{
				Presence: types.PresenceUnavailable.String(),
			},
		}
	}
	lastActive, err := strconv.Atoi(presence.Header.Get("last_active_ts"))
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	p := types.PresenceInternal{LastActiveTS: spec.Timestamp(lastActive)}
	currentlyActive := p.CurrentlyActive()
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: types.PresenceClientResponse{
			CurrentlyActive: &currentlyActive,
			LastActiveAgo:   p.LastActiveAgo(),
			Presence:        presence.Header.Get("presence"),
			StatusMsg:       &statusMsg,
		},
	}
}
