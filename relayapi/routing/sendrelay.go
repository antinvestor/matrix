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
	"github.com/pitabwire/frame"
	"net/http"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/relayapi/api"
	"github.com/pitabwire/util"
)

// SendTransactionToRelay implements PUT /_matrix/federation/v1/send_relay/{txnID}/{userID}
// This endpoint can be extracted into a separate relay server service.
func SendTransactionToRelay(
	httpReq *http.Request,
	fedReq *fclient.FederationRequest,
	relayAPI api.RelayInternalAPI,
	txnID gomatrixserverlib.TransactionID,
	userID spec.UserID,
) util.JSONResponse {
	log := frame.Log(httpReq.Context())
	log.Info("Processing send_relay for %s", userID.String())

	var txnEvents fclient.RelayEvents
	if err := json.Unmarshal(fedReq.Content(), &txnEvents); err != nil {
		log.Info("The request body could not be decoded into valid JSON." + err.Error())
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into valid JSON." + err.Error()),
		}
	}

	// Transactions are limited in size; they can have at most 50 PDUs and 100 EDUs.
	// https://matrix.org/docs/spec/server_server/latest#transactions
	if len(txnEvents.PDUs) > 50 || len(txnEvents.EDUs) > 100 {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("max 50 pdus / 100 edus"),
		}
	}

	t := gomatrixserverlib.Transaction{}
	t.PDUs = txnEvents.PDUs
	t.EDUs = txnEvents.EDUs
	t.Origin = fedReq.Origin()
	t.TransactionID = txnID
	t.Destination = userID.Domain()

	util.GetLogger(httpReq.Context()).Warn("Received transaction %q from %q containing %d PDUs, %d EDUs", txnID, fedReq.Origin(), len(t.PDUs), len(t.EDUs))

	err := relayAPI.PerformStoreTransaction(httpReq.Context(), t, userID)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.BadJSON("could not store the transaction for forwarding"),
		}
	}

	return util.JSONResponse{Code: 200}
}
