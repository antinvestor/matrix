// Copyright 2021 Dan Peleg <dan@globekeeper.com>
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
	"net/http"
	"net/url"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/httputil"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

// GetPushers handles /_matrix/client/r0/pushers
func GetPushers(
	req *http.Request, device *userapi.Device,
	userAPI userapi.ClientUserAPI,
) util.JSONResponse {
	var queryRes userapi.QueryPushersResponse
	localpart, domain, err := gomatrixserverlib.SplitID('@', device.UserID)
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("SplitID failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	err = userAPI.QueryPushers(req.Context(), &userapi.QueryPushersRequest{
		Localpart:  localpart,
		ServerName: domain,
	}, &queryRes)
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("QueryPushers failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	for i := range queryRes.Pushers {
		queryRes.Pushers[i].SessionID = ""
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: queryRes,
	}
}

// SetPusher handles /_matrix/client/r0/pushers/set
// This endpoint allows the creation, modification and deletion of pushers for this user ID.
// The behaviour of this endpoint varies depending on the values in the JSON body.
func SetPusher(
	req *http.Request, device *userapi.Device,
	userAPI userapi.ClientUserAPI,
) util.JSONResponse {
	localpart, domain, err := gomatrixserverlib.SplitID('@', device.UserID)
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("SplitID failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	body := userapi.PerformPusherSetRequest{}
	if resErr := httputil.UnmarshalJSONRequest(req, &body); resErr != nil {
		return *resErr
	}
	if len(body.AppID) > 64 {
		return invalidParam("length of app_id must be no more than 64 characters")
	}
	if len(body.PushKey) > 512 {
		return invalidParam("length of pushkey must be no more than 512 bytes")
	}
	uInt := body.Data["url"]
	if uInt != nil {
		u, ok := uInt.(string)
		if !ok {
			return invalidParam("url must be string")
		}
		if u != "" {
			var pushUrl *url.URL
			pushUrl, err = url.Parse(u)
			if err != nil {
				return invalidParam("malformed url passed")
			}
			if pushUrl.Scheme != "https" {
				return invalidParam("only https scheme is allowed")
			}
		}

	}
	body.Localpart = localpart
	body.ServerName = domain
	body.SessionID = device.SessionID
	err = userAPI.PerformPusherSet(req.Context(), &body, &struct{}{})
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("PerformPusherSet failed")
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

func invalidParam(msg string) util.JSONResponse {
	return util.JSONResponse{
		Code: http.StatusBadRequest,
		JSON: spec.InvalidParam(msg),
	}
}
