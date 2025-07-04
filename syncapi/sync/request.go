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

package sync

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

const defaultSyncTimeout = time.Duration(0)
const DefaultTimelineLimit = 20

func newSyncRequest(req *http.Request, device userapi.Device, syncDB storage.Database) (*types.SyncRequest, error) {
	timeout := getTimeout(req.URL.Query().Get("timeout"))
	fullState := req.URL.Query().Get("full_state")
	wantFullState := fullState != "" && fullState != "false"
	since, sinceStr := types.StreamingToken{}, req.URL.Query().Get("since")
	if sinceStr != "" {
		var err error
		since, err = types.NewStreamTokenFromString(sinceStr)
		if err != nil {
			return nil, err
		}
	}

	// Create a default filter and apply a stored filter on top of it (if specified)
	filter := synctypes.DefaultFilter()
	filterQuery := req.URL.Query().Get("filter")
	if filterQuery != "" {
		if filterQuery[0] == '{' {
			// Parse the filter from the query string
			if err := json.Unmarshal([]byte(filterQuery), &filter); err != nil {
				return nil, fmt.Errorf("json.Unmarshal: %w", err)
			}
		} else {
			// Try to load the filter from the database
			localpart, _, err := gomatrixserverlib.SplitID('@', device.UserID)
			if err != nil {
				util.Log(req.Context()).WithError(err).Error("gomatrixserverlib.SplitID failed")
				return nil, fmt.Errorf("gomatrixserverlib.SplitID: %w", err)
			}
			err = syncDB.GetFilter(req.Context(), &filter, localpart, filterQuery)
			if err != nil && !sqlutil.ErrorIsNoRows(err) {
				util.Log(req.Context()).WithError(err).Error("syncDB.GetFilter failed")
				return nil, fmt.Errorf("syncDB.GetFilter: %w", err)
			}
		}
	}

	// A loaded filter might have overwritten these values,
	// so set them after loading the filter.
	if since.IsEmpty() {
		// Send as much account data down for complete syncs as possible
		// by default, otherwise clients do weird things while waiting
		// for the rest of the data to trickle down.
		filter.AccountData.Limit = math.MaxInt32
		filter.Room.AccountData.Limit = math.MaxInt32
	}

	// logger := util.Log(req.Context()).
	//	WithField("user_id", device.UserID).
	//	WithField("device_id", device.ID).
	//	WithField("since", since).
	//	WithField("timeout", timeout).
	//	WithField("limit", filter.Room.Timeline.Limit)

	return &types.SyncRequest{
		Context:           req.Context(),             //
		Device:            &device,                   //
		Response:          types.NewResponse(),       // Populated by all streams
		Filter:            filter,                    //
		Since:             since,                     //
		Timeout:           timeout,                   //
		Rooms:             make(map[string]string),   // Populated by the PDU stream
		WantFullState:     wantFullState,             //
		MembershipChanges: make(map[string]struct{}), // Populated by the PDU stream
	}, nil
}

func getTimeout(timeoutMS string) time.Duration {
	if timeoutMS == "" {
		return defaultSyncTimeout
	}
	i, err := strconv.Atoi(timeoutMS)
	if err != nil {
		return defaultSyncTimeout
	}
	return time.Duration(i) * time.Millisecond
}
