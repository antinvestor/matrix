// Copyright 2023 The Global.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
)

// LoginTypeApplicationService describes how to authenticate as an
// application service
type LoginTypeApplicationService struct {
	Config *config.ClientAPI
	Token  string
}

// Name implements Type
func (t *LoginTypeApplicationService) Name() string {
	return authtypes.LoginTypeApplicationService
}

// LoginFromJSON implements Type
func (t *LoginTypeApplicationService) LoginFromJSON(
	ctx context.Context, reqBytes []byte,
) (*Login, LoginCleanupFunc, *util.JSONResponse) {
	var r Login
	if err := httputil.UnmarshalJSON(reqBytes, &r); err != nil {
		return nil, nil, err
	}

	_, err := internal.ValidateApplicationServiceRequest(t.Config, r.Identifier.User, t.Token)
	if err != nil {
		return nil, nil, err
	}

	cleanup := func(ctx context.Context, j *util.JSONResponse) {}
	return &r, cleanup, nil
}
