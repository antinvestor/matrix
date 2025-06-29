// Copyright 2025 Ant Investor Ltd.
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
	"net/http"
	"strings"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/clientapi/httputil"
	"github.com/antinvestor/matrix/clientapi/userutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

type GetAccountByPassword func(ctx context.Context, req *api.QueryAccountByPasswordRequest, res *api.QueryAccountByPasswordResponse) error

type PasswordRequest struct {
	Login
	Password string `json:"password"`
}

// LoginTypePassword implements https://matrix.org/docs/spec/client_server/r0.6.1#password-based
type LoginTypePassword struct {
	GetAccountByPassword GetAccountByPassword
	Config               *config.ClientAPI
}

func (t *LoginTypePassword) Name() string {
	return authtypes.LoginTypePassword
}

func (t *LoginTypePassword) LoginFromJSON(ctx context.Context, reqBytes []byte) (*Login, LoginCleanupFunc, *util.JSONResponse) {
	var r PasswordRequest
	if err := httputil.UnmarshalJSON(reqBytes, &r); err != nil {
		return nil, nil, err
	}

	login, err := t.Login(ctx, &r)
	if err != nil {
		return nil, nil, err
	}

	return login, func(context.Context, *util.JSONResponse) {}, nil
}

func (t *LoginTypePassword) Login(ctx context.Context, req interface{}) (*Login, *util.JSONResponse) {
	r := req.(*PasswordRequest)
	username := r.Username()
	if username == "" {
		return nil, &util.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: spec.BadJSON("A username must be supplied."),
		}
	}
	if len(r.Password) == 0 {
		return nil, &util.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: spec.BadJSON("A password must be supplied."),
		}
	}
	localpart, domain, err := userutil.ParseUsernameParam(username, t.Config.Global)
	if err != nil {
		return nil, &util.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: spec.InvalidUsername(err.Error()),
		}
	}
	if !t.Config.Global.IsLocalServerName(domain) {
		return nil, &util.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: spec.InvalidUsername("The server name is not known."),
		}
	}
	// Squash username to all lowercase letters
	res := &api.QueryAccountByPasswordResponse{}
	err = t.GetAccountByPassword(ctx, &api.QueryAccountByPasswordRequest{
		Localpart:         strings.ToLower(localpart),
		ServerName:        domain,
		PlaintextPassword: r.Password,
	}, res)
	if err != nil {
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("Unable to fetch account by password."),
		}
	}

	// If we couldn't find the user by the lower cased localpart, try the provided
	// localpart as is.
	if !res.Exists {
		err = t.GetAccountByPassword(ctx, &api.QueryAccountByPasswordRequest{
			Localpart:         localpart,
			ServerName:        domain,
			PlaintextPassword: r.Password,
		}, res)
		if err != nil {
			return nil, &util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.Unknown("Unable to fetch account by password."),
			}
		}
		// Technically we could tell them if the user does not exist by checking if sqlutil.ErrorIsNoRows(err)
		// but that would leak the existence of the user.
		if !res.Exists {
			return nil, &util.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.Forbidden("The username or password was incorrect or the account does not exist."),
			}
		}
	}
	// Set the user, so login.Username() can do the right thing
	r.Identifier.User = res.Account.UserID
	r.User = res.Account.UserID
	return &r.Login, nil
}
