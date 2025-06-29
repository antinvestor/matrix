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
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/httputil"
	"github.com/antinvestor/matrix/clientapi/producers"
	"github.com/antinvestor/matrix/internal/eventutil"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

// GetAccountData implements GET /user/{userId}/[rooms/{roomid}/]account_data/{type}
func GetAccountData(
	req *http.Request, userAPI api.ClientUserAPI, device *api.Device,
	userID string, roomID string, dataType string,
) util.JSONResponse {
	if userID != device.UserID {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID does not match the current user"),
		}
	}

	dataReq := api.QueryAccountDataRequest{
		UserID:   userID,
		DataType: dataType,
		RoomID:   roomID,
	}
	dataRes := api.QueryAccountDataResponse{}
	if err := userAPI.QueryAccountData(req.Context(), &dataReq, &dataRes); err != nil {
		util.Log(req.Context()).WithError(err).Error("userAPI.QueryAccountData failed")
		return util.ErrorResponse(fmt.Errorf("userAPI.QueryAccountData: %w", err))
	}

	var data json.RawMessage
	var ok bool
	if roomID != "" {
		data, ok = dataRes.RoomAccountData[roomID][dataType]
	} else {
		data, ok = dataRes.GlobalAccountData[dataType]
	}
	if ok {
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: data,
		}
	}

	return util.JSONResponse{
		Code: http.StatusNotFound,
		JSON: spec.NotFound("data not found"),
	}
}

// SaveAccountData implements PUT /user/{userId}/[rooms/{roomId}/]account_data/{type}
func SaveAccountData(
	req *http.Request, userAPI api.ClientUserAPI, device *api.Device,
	userID string, roomID string, dataType string, syncProducer *producers.SyncAPIProducer,
) util.JSONResponse {
	if userID != device.UserID {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID does not match the current user"),
		}
	}

	defer req.Body.Close() // nolint: errcheck

	if req.Body == http.NoBody {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("Content not JSON"),
		}
	}

	if dataType == "m.fully_read" || dataType == "m.push_rules" {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(fmt.Sprintf("Unable to modify %q using this API", dataType)),
		}
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("io.ReadAll failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if !json.Valid(body) {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Bad JSON content"),
		}
	}

	dataReq := api.InputAccountDataRequest{
		UserID:      userID,
		DataType:    dataType,
		RoomID:      roomID,
		AccountData: json.RawMessage(body),
	}
	dataRes := api.InputAccountDataResponse{}
	if err := userAPI.InputAccountData(req.Context(), &dataReq, &dataRes); err != nil {
		util.Log(req.Context()).WithError(err).Error("userAPI.InputAccountData failed")
		return util.ErrorResponse(err)
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

type fullyReadEvent struct {
	EventID string `json:"event_id"`
}

// SaveReadMarker implements POST /rooms/{roomId}/read_markers
func SaveReadMarker(
	req *http.Request,
	userAPI api.ClientUserAPI, rsAPI roomserverAPI.ClientRoomserverAPI,
	syncProducer *producers.SyncAPIProducer, device *api.Device, roomID string,
) util.JSONResponse {
	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("userID for this device is invalid"),
		}
	}

	// Verify that the user is a member of this room
	resErr := checkMemberInRoom(req.Context(), rsAPI, *deviceUserID, roomID)
	if resErr != nil {
		return *resErr
	}

	var r eventutil.ReadMarkerJSON
	resErr = httputil.UnmarshalJSONRequest(req, &r)
	if resErr != nil {
		return *resErr
	}

	if r.FullyRead != "" {
		data, err := json.Marshal(fullyReadEvent{EventID: r.FullyRead})
		if err != nil {
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		dataReq := api.InputAccountDataRequest{
			UserID:      device.UserID,
			DataType:    "m.fully_read",
			RoomID:      roomID,
			AccountData: data,
		}
		dataRes := api.InputAccountDataResponse{}
		if err := userAPI.InputAccountData(req.Context(), &dataReq, &dataRes); err != nil {
			util.Log(req.Context()).WithError(err).Error("userAPI.InputAccountData failed")
			return util.ErrorResponse(err)
		}
	}

	// Handle the read receipts that may be included in the read marker.
	if r.Read != "" {
		return SetReceipt(req, userAPI, syncProducer, device, roomID, "m.read", r.Read)
	}
	if r.ReadPrivate != "" {
		return SetReceipt(req, userAPI, syncProducer, device, roomID, "m.read.private", r.ReadPrivate)
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}
