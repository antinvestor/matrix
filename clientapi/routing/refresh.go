// Copyright 2025 Ant Investor Ltd
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

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/httputil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
)

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type refreshResponse struct {
	AccessToken  string `json:"access_token"`
	ExpiresInMs  int64  `json:"expires_in_ms,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

// Refresh implements POST /_matrix/client/r0/refresh
// MSC2918: https://github.com/matrix-org/matrix-spec-proposals/pull/2918
func Refresh(
	req *http.Request,
	auth ssoAuthenticator,
	cfg *config.LoginSSO,
) util.JSONResponse {

	ctx := req.Context()
	log := util.Log(ctx)

	if req.Method != http.MethodPost {
		return util.JSONResponse{
			Code: http.StatusMethodNotAllowed,
			JSON: spec.NotFound("Bad method"),
		}
	}

	var r refreshRequest
	if resErr := httputil.UnmarshalJSONRequest(req, &r); resErr != nil {
		return *resErr
	}

	if r.RefreshToken == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Missing refresh_token"),
		}
	}

	query := req.URL.Query()
	idpID := query.Get("partition_id")

	if idpID == "" {
		idpID = cfg.DefaultProviderID
		if idpID == "" && len(cfg.Providers) > 0 {
			idpID = cfg.Providers[0].ID
		}
	}

	token, err := auth.RefreshToken(ctx, idpID, r.RefreshToken)
	if err != nil {
		log.WithError(err).Warn(" TokenRefresh failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.MissingToken(err.Error()),
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: refreshResponse{
			AccessToken:  token.AccessToken,
			ExpiresInMs:  token.ExpiresIn,
			RefreshToken: token.RefreshToken,
		},
	}
}
