// Copyright 2021 The Global.org Foundation C.I.C.
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

package auth

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/setup/config"
	uapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

// LoginFromJSONReader performs authentication given a login request body reader and
// some context. It returns the basic login information and a cleanup function to be
// called after authorization has completed, with the result of the authorization.
// If the final return value is non-nil, an error occurred and the cleanup function
// is nil.
func LoginFromJSONReader(
	req *http.Request,
	useraccountAPI uapi.UserLoginAPI,
	userAPI UserInternalAPIForLogin,
	cfg *config.ClientAPI,
) (*Login, LoginCleanupFunc, *util.JSONResponse) {
	reqBytes, err := io.ReadAll(req.Body)
	if err != nil {
		err := &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Reading request body failed: " + err.Error()),
		}
		return nil, nil, err
	}

	var header struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(reqBytes, &header); err != nil {
		err := &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Reading request body failed: " + err.Error()),
		}
		return nil, nil, err
	}

	var typ Type
	switch header.Type {
	case authtypes.LoginTypePassword:
		typ = &LoginTypePassword{
			GetAccountByPassword: useraccountAPI.QueryAccountByPassword,
			Config:               cfg,
		}
	case authtypes.LoginTypeToken:
		typ = &LoginTypeToken{
			UserAPI: userAPI,
			Config:  cfg,
		}
	case authtypes.LoginTypeApplicationService:
		token, err := ExtractAccessToken(req)
		if err != nil {
			err := &util.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.MissingToken(err.Error()),
			}
			return nil, nil, err
		}

		typ = &LoginTypeApplicationService{
			Config: cfg,
			Token:  token,
		}
	default:
		err := util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("unhandled login type: " + header.Type),
		}
		return nil, nil, &err
	}

	return typ.LoginFromJSON(req.Context(), reqBytes)
}

// UserInternalAPIForLogin contains the aspects of UserAPI required for logging in.
type UserInternalAPIForLogin interface {
	uapi.LoginTokenInternalAPI
}
