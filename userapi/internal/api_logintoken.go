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

package internal

import (
	"context"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

// PerformLoginTokenCreation creates a new login token and associates it with the provided data.
func (a *UserInternalAPI) PerformLoginTokenCreation(ctx context.Context, req *api.PerformLoginTokenCreationRequest, res *api.PerformLoginTokenCreationResponse) error {
	util.Log(ctx).WithField("user_id", req.Data.UserID).Info("PerformLoginTokenCreation")
	_, domain, err := gomatrixserverlib.SplitID('@', req.Data.UserID)
	if err != nil {
		return err
	}
	if !a.Config.Global.IsLocalServerName(domain) {
		return fmt.Errorf("cannot create a login token for a remote user (server name %s)", domain)
	}
	tokenMeta, err := a.DB.CreateLoginToken(ctx, &req.Data)
	if err != nil {
		return err
	}
	res.Metadata = *tokenMeta
	return nil
}

// PerformLoginTokenDeletion ensures the token doesn't exist.
func (a *UserInternalAPI) PerformLoginTokenDeletion(ctx context.Context, req *api.PerformLoginTokenDeletionRequest, res *api.PerformLoginTokenDeletionResponse) error {
	util.Log(ctx).WithField("token", req.Token).Info("PerformLoginTokenDeletion")
	return a.DB.RemoveLoginToken(ctx, req.Token)
}

// QueryLoginToken returns the data associated with a login token. If
// the token is not valid, success is returned, but res.Data == nil.
func (a *UserInternalAPI) QueryLoginToken(ctx context.Context, req *api.QueryLoginTokenRequest, res *api.QueryLoginTokenResponse) error {
	tokenData, err := a.DB.GetLoginTokenDataByToken(ctx, req.Token)
	if err != nil {
		res.Data = nil
		if sqlutil.ErrorIsNoRows(err) {
			return nil
		}
		return err
	}
	localpart, domain, err := gomatrixserverlib.SplitID('@', tokenData.UserID)
	if err != nil {
		return err
	}
	if !a.Config.Global.IsLocalServerName(domain) {
		return fmt.Errorf("cannot return a login token for a remote user (server name %s)", domain)
	}
	if _, err = a.DB.GetAccountByLocalpart(ctx, localpart, domain); err != nil {
		res.Data = nil
		if sqlutil.ErrorIsNoRows(err) {
			return nil
		}
		return err
	}
	res.Data = tokenData
	return nil
}
