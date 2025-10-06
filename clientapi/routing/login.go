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
	"net/http"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/clientapi/userutil"
	"github.com/antinvestor/matrix/setup/config"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

type loginResponse struct {
	UserID       string `json:"user_id"`
	AccessToken  string `json:"access_token"`
	DeviceID     string `json:"device_id"`
	RefreshToken string `json:"refresh_token"`
}

type flows struct {
	Flows []flow `json:"flows"`
}

type flow struct {
	Type string `json:"type"`
}

// Login implements GET and POST /login
func Login(
	req *http.Request, userAPI userapi.ClientUserAPI,
	cfg *config.ClientAPI,
) util.JSONResponse {
	switch req.Method {
	case http.MethodGet:
		loginFlows := []flow{{Type: authtypes.LoginTypePassword}, {Type: authtypes.LoginTypeSSO}}
		if len(cfg.Derived.ApplicationServices) > 0 {
			loginFlows = append(loginFlows, flow{Type: authtypes.LoginTypeApplicationService})
		}
		// TODO: support other forms of login, depending on config options
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: flows{
				Flows: loginFlows,
			},
		}
	case http.MethodPost:
		login, cleanup, authErr := auth.LoginFromJSONReader(req, userAPI, userAPI, cfg)
		if authErr != nil {
			return *authErr
		}
		// make a device/access token
		authResponse := completeAuth(req.Context(), cfg.Global, userAPI, login, req.RemoteAddr, req.UserAgent())
		cleanup(req.Context(), &authResponse)
		return authResponse
	}
	return util.JSONResponse{
		Code: http.StatusMethodNotAllowed,
		JSON: spec.NotFound("Bad method"),
	}
}

func completeAuth(
	ctx context.Context, cfg *config.Global, userAPI userapi.ClientUserAPI, login *auth.Login,
	ipAddr, userAgent string,
) util.JSONResponse {

	accessToken, err := auth.GenerateAccessToken()
	if err != nil {
		util.Log(ctx).WithError(err).Error("auth.GenerateAccessToken failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	refreshToken := ""
	if login.ExtraData != nil {
		accessToken = login.ExtraData.AccessToken
		refreshToken = login.ExtraData.RefreshToken
		util.Log(ctx).WithField("username", login.Username()).WithField("has_access_token", accessToken != "").WithField("has_refresh_token", refreshToken != "").Debug("completeAuth: using SSO tokens from login.ExtraData")
	} else {
		util.Log(ctx).WithField("username", login.Username()).Debug("completeAuth: no SSO tokens, using generated access token")
	}

	localpart, serverName, err := userutil.ParseUsernameParam(login.Username(), cfg)
	if err != nil {
		util.Log(ctx).WithError(err).Error("auth.ParseUsernameParam failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var performRes userapi.PerformDeviceCreationResponse
	err = userAPI.PerformDeviceCreation(ctx, &userapi.PerformDeviceCreationRequest{
		DeviceDisplayName: login.InitialDisplayName,
		DeviceID:          login.DeviceID,
		AccessToken:       accessToken,
		ExtraData:         login.ExtraData,
		Localpart:         localpart,
		ServerName:        serverName,
		IPAddr:            ipAddr,
		UserAgent:         userAgent,
	}, &performRes)
	if err != nil {
		util.Log(ctx).WithError(err).Error("userAPI.PerformDeviceCreation failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("failed to create device: " + err.Error()),
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: loginResponse{
			UserID:       performRes.Device.UserID,
			AccessToken:  accessToken,
			RefreshToken: refreshToken,
			DeviceID:     performRes.Device.ID,
		},
	}
}
