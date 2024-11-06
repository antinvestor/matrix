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

package internal

import (
	"context"
	"database/sql"
	"errors"

	"github.com/antinvestor/matrix/userapi/api"
	"github.com/matrix-org/util"
)

func (a *UserInternalAPI) PerformEnsureSSOAccountExists(ctx context.Context, req *api.QuerySSOAccountRequest, res *api.QuerySSOAccountResponse) error {

	var qAccRes api.QueryAccountByLocalpartResponse
	err := a.QueryAccountByLocalpart(ctx, &api.QueryAccountByLocalpartRequest{
		Localpart:  req.Subject,
		ServerName: req.ServerName,
	}, &qAccRes)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {

			return err
		}

		util.GetLogger(ctx).WithField("request", req).Info("No account exists with the profile id")

		var accRes api.PerformAccountCreationResponse
		err = a.PerformAccountCreation(ctx, &api.PerformAccountCreationRequest{
			Localpart:   req.Subject,
			ServerName:  req.ServerName,
			AccountType: api.AccountTypeUser,
			OnConflict:  api.ConflictAbort,
		}, &accRes)

		if err != nil {
			return err
		}

		res.Account = accRes.Account
		res.AccountCreated = accRes.AccountCreated

		return nil
	}

	res.AccountCreated = false
	res.Account = qAccRes.Account
	return nil

}
