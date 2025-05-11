// Copyright 2020 The Matrix.org Foundation C.I.C.
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
	"errors"
	"fmt"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"net/http"
	"strings"

	"github.com/antinvestor/gomatrix"
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/roomserver/api"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

type UserDirectoryResponse struct {
	Results []authtypes.FullyQualifiedProfile `json:"results"`
	Limited bool                              `json:"limited"`
}

func SearchUserDirectory(
	ctx context.Context,
	device *userapi.Device,
	rsAPI api.ClientRoomserverAPI,
	provider userapi.QuerySearchProfilesAPI,
	searchString string,
	limit int,
	federation fclient.FederationClient,
	localServerName spec.ServerName,
) util.JSONResponse {
	if limit < 10 {
		limit = 10
	}

	results := map[string]authtypes.FullyQualifiedProfile{}
	response := &UserDirectoryResponse{
		Results: []authtypes.FullyQualifiedProfile{},
		Limited: false,
	}

	// Get users we share a room with
	knownUsersReq := &api.QueryKnownUsersRequest{
		UserID: device.UserID,
		Limit:  limit,
	}
	knownUsersRes := &api.QueryKnownUsersResponse{}
	if err := rsAPI.QueryKnownUsers(ctx, knownUsersReq, knownUsersRes); err != nil && !sqlutil.ErrorIsNoRows(err) {
		return util.ErrorResponse(fmt.Errorf("rsAPI.QueryKnownUsers: %w", err))
	}

	activeUserLocalpart, _, _ := gomatrixserverlib.SplitID('@', device.UserID)
knownUsersLoop:

	for _, profile := range knownUsersRes.Users {
		if len(results) == limit {
			response.Limited = true
			break
		}
		userID := profile.UserID
		// get the full profile of the local user
		localpart, serverName, _ := gomatrixserverlib.SplitID('@', userID)
		if serverName == localServerName {
			userReq := &userapi.QuerySearchProfilesRequest{
				Localpart:    activeUserLocalpart,
				SearchString: localpart,
				Limit:        limit,
			}
			userRes := &userapi.QuerySearchProfilesResponse{}
			if err := provider.QuerySearchProfiles(ctx, userReq, userRes); err != nil {
				return util.ErrorResponse(fmt.Errorf("userAPI.QuerySearchProfiles: %w", err))
			}
			for _, p := range userRes.Profiles {
				if strings.Contains(p.DisplayName, searchString) ||
					strings.Contains(p.Localpart, searchString) {
					profile.DisplayName = p.DisplayName
					profile.AvatarURL = p.AvatarURL
					results[userID] = profile
					if len(results) == limit {
						response.Limited = true
						break knownUsersLoop
					}
				}
			}
		} else {
			// If the username already contains the search string, don't bother hitting federation.
			// This will result in missing avatars and displaynames, but saves the federation roundtrip.
			if strings.Contains(localpart, searchString) {
				results[userID] = profile
				if len(results) == limit {
					response.Limited = true
					break knownUsersLoop
				}
				continue
			}
			// TODO: We should probably cache/store this
			fedProfile, fedErr := federation.LookupProfile(ctx, localServerName, serverName, userID, "")
			if fedErr != nil {
				var x gomatrix.HTTPError
				if errors.As(fedErr, &x) {
					if x.Code == http.StatusNotFound {
						continue
					}
				}
			}
			if strings.Contains(fedProfile.DisplayName, searchString) {
				profile.DisplayName = fedProfile.DisplayName
				profile.AvatarURL = fedProfile.AvatarURL
				results[userID] = profile
				if len(results) == limit {
					response.Limited = true
					break knownUsersLoop
				}
			}
		}
	}

	for _, result := range results {
		response.Results = append(response.Results, result)
	}

	return util.JSONResponse{
		Code: 200,
		JSON: response,
	}
}
