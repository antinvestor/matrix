// Copyright 2021 The Matrix.org Foundation C.I.C.
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
	"context"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/clientapi/httputil"
	"github.com/antinvestor/matrix/setup/config"
	uapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"net/http"
)

// LoginTypeJWT describes how to authenticate with a jwt token.
type LoginTypeJWT struct {
	UserAPI uapi.LoginJWTInternalAPI
	Config  *config.ClientAPI
}

// Name implements Type.
func (t *LoginTypeJWT) Name() string {
	return authtypes.LoginTypeJwt
}

// LoginFromJSON implements Type. The cleanup function has no use other than just logging success.
func (t *LoginTypeJWT) LoginFromJSON(ctx context.Context, reqBytes []byte) (*Login, LoginCleanupFunc, *util.JSONResponse) {
	var r loginJwtRequest
	if err := httputil.UnmarshalJSON(reqBytes, &r); err != nil {
		return nil, nil, err
	}

	var res uapi.QueryLoginJWTResponse
	if err := t.UserAPI.QueryLoginJWT(ctx, &uapi.QueryLoginJWTRequest{Token: r.Token}, &res); err != nil {
		util.GetLogger(ctx).WithError(err).Error("UserAPI.QueryLoginToken failed")
		return nil, nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if res.Data == nil {
		return nil, nil, &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("invalid login token"),
		}
	}

	r.Login.Identifier.Type = "m.id.user"
	r.Login.Identifier.User = res.Data.UserID

	cleanup := func(ctx context.Context, authRes *util.JSONResponse) {
		if authRes == nil {
			util.GetLogger(ctx).Error("No JSONResponse provided to LoginJWTType cleanup function")
			return
		}
	}
	return &r.Login, cleanup, nil
}

// loginJwtRequest struct to hold the possible parameters from an HTTP request.
type loginJwtRequest struct {
	Login
	Token string `json:"token"`
}
