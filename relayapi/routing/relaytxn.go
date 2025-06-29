// Copyright 2022 The Global.org Foundation C.I.C.
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
	"encoding/json"
	"net/http"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/relayapi/api"
	"github.com/pitabwire/util"
)

// GetTransactionFromRelay implements GET /_matrix/federation/v1/relay_txn/{userID}
// This endpoint can be extracted into a separate relay server service.
func GetTransactionFromRelay(
	httpReq *http.Request,
	fedReq *fclient.FederationRequest,
	relayAPI api.RelayInternalAPI,
	userID spec.UserID,
) util.JSONResponse {
	log := util.Log(httpReq.Context())
	log.Info("Processing relay_txn for %s", userID.String())

	var previousEntry fclient.RelayEntry
	if err := json.Unmarshal(fedReq.Content(), &previousEntry); err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.BadJSON("invalid json provided"),
		}
	}
	if previousEntry.EntryID < 0 {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.BadJSON("Invalid entry id provided. Must be >= 0."),
		}
	}
	log.Info("Previous entry provided: %v", previousEntry.EntryID)

	response, err := relayAPI.QueryTransactions(httpReq.Context(), userID, previousEntry)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespGetRelayTransaction{
			Transaction:   response.Transaction,
			EntryID:       response.EntryID,
			EntriesQueued: response.EntriesQueued,
		},
	}
}
