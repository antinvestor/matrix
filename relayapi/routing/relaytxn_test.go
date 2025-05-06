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

package routing_test

import (
	"net/http"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/relayapi/internal"
	"github.com/antinvestor/matrix/relayapi/routing"
	"github.com/antinvestor/matrix/relayapi/storage/shared"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

func createQuery(
	userID spec.UserID,
	prevEntry fclient.RelayEntry,
) fclient.FederationRequest {
	var federationPathPrefixV1 = "/_matrix/federation/v1"
	path := federationPathPrefixV1 + "/relay_txn/" + userID.String()
	request := fclient.NewFederationRequest("GET", userID.Domain(), "relay", path)
	request.SetContent(prevEntry)

	return request
}

func TestGetEmptyDatabaseReturnsNothing(t *testing.T) {
	ctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)
	testDB := test.NewInMemoryRelayDatabase()
	db := shared.Database{
		Writer:         sqlutil.NewDummyWriter(),
		RelayQueue:     testDB,
		RelayQueueJSON: testDB,
	}
	httpReq := &http.Request{}
	userID, err := spec.NewUserID("@local:domain", false)
	assert.NoError(t, err, "Invalid userID")

	transaction := createTransaction()

	_, err = db.StoreTransaction(ctx, transaction)
	assert.NoError(t, err, "Failed to store transaction")

	relayAPI := internal.NewRelayInternalAPI(
		&db, nil, nil, nil, nil, false, "", true,
	)

	request := createQuery(*userID, fclient.RelayEntry{})
	response := routing.GetTransactionFromRelay(httpReq, &request, relayAPI, *userID)
	assert.Equal(t, http.StatusOK, response.Code)

	jsonResponse := response.JSON.(fclient.RespGetRelayTransaction)
	assert.Equal(t, false, jsonResponse.EntriesQueued)
	assert.Equal(t, gomatrixserverlib.Transaction{}, jsonResponse.Transaction)

	count, err := db.GetTransactionCount(ctx, *userID)
	assert.NoError(t, err)
	assert.Zero(t, count)
}

func TestGetInvalidPrevEntryFails(t *testing.T) {
	ctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)
	testDB := test.NewInMemoryRelayDatabase()
	db := shared.Database{
		Writer:         sqlutil.NewDummyWriter(),
		RelayQueue:     testDB,
		RelayQueueJSON: testDB,
	}
	httpReq := &http.Request{}
	userID, err := spec.NewUserID("@local:domain", false)
	assert.NoError(t, err, "Invalid userID")

	transaction := createTransaction()

	_, err = db.StoreTransaction(ctx, transaction)
	assert.NoError(t, err, "Failed to store transaction")

	relayAPI := internal.NewRelayInternalAPI(
		&db, nil, nil, nil, nil, false, "", true,
	)

	request := createQuery(*userID, fclient.RelayEntry{EntryID: -1})
	response := routing.GetTransactionFromRelay(httpReq, &request, relayAPI, *userID)
	assert.Equal(t, http.StatusInternalServerError, response.Code)
}

func TestGetReturnsSavedTransaction(t *testing.T) {
	ctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)
	testDB := test.NewInMemoryRelayDatabase()
	db := shared.Database{
		Writer:         sqlutil.NewDummyWriter(),
		RelayQueue:     testDB,
		RelayQueueJSON: testDB,
	}
	httpReq := &http.Request{}
	userID, err := spec.NewUserID("@local:domain", false)
	assert.NoError(t, err, "Invalid userID")

	transaction := createTransaction()
	receipt, err := db.StoreTransaction(ctx, transaction)
	assert.NoError(t, err, "Failed to store transaction")

	err = db.AssociateTransactionWithDestinations(
		ctx,
		map[spec.UserID]struct{}{
			*userID: {},
		},
		transaction.TransactionID,
		receipt)
	assert.NoError(t, err, "Failed to associate transaction with user")

	relayAPI := internal.NewRelayInternalAPI(
		&db, nil, nil, nil, nil, false, "", true,
	)

	request := createQuery(*userID, fclient.RelayEntry{})
	response := routing.GetTransactionFromRelay(httpReq, &request, relayAPI, *userID)
	assert.Equal(t, http.StatusOK, response.Code)

	jsonResponse := response.JSON.(fclient.RespGetRelayTransaction)
	assert.True(t, jsonResponse.EntriesQueued)
	assert.Equal(t, transaction, jsonResponse.Transaction)

	// And once more to clear the queue
	request = createQuery(*userID, fclient.RelayEntry{EntryID: jsonResponse.EntryID})
	response = routing.GetTransactionFromRelay(httpReq, &request, relayAPI, *userID)
	assert.Equal(t, http.StatusOK, response.Code)

	jsonResponse = response.JSON.(fclient.RespGetRelayTransaction)
	assert.False(t, jsonResponse.EntriesQueued)
	assert.Equal(t, gomatrixserverlib.Transaction{}, jsonResponse.Transaction)

	count, err := db.GetTransactionCount(ctx, *userID)
	assert.NoError(t, err)
	assert.Zero(t, count)
}

func TestGetReturnsMultipleSavedTransactions(t *testing.T) {
	ctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)
	testDB := test.NewInMemoryRelayDatabase()
	db := shared.Database{
		Writer:         sqlutil.NewDummyWriter(),
		RelayQueue:     testDB,
		RelayQueueJSON: testDB,
	}
	httpReq := &http.Request{}
	userID, err := spec.NewUserID("@local:domain", false)
	assert.NoError(t, err, "Invalid userID")

	transaction := createTransaction()
	receipt, err := db.StoreTransaction(ctx, transaction)
	assert.NoError(t, err, "Failed to store transaction")

	err = db.AssociateTransactionWithDestinations(
		ctx,
		map[spec.UserID]struct{}{
			*userID: {},
		},
		transaction.TransactionID,
		receipt)
	assert.NoError(t, err, "Failed to associate transaction with user")

	transaction2 := createTransaction()
	receipt2, err := db.StoreTransaction(ctx, transaction2)
	assert.NoError(t, err, "Failed to store transaction")

	err = db.AssociateTransactionWithDestinations(
		ctx,
		map[spec.UserID]struct{}{
			*userID: {},
		},
		transaction2.TransactionID,
		receipt2)
	assert.NoError(t, err, "Failed to associate transaction with user")

	relayAPI := internal.NewRelayInternalAPI(
		&db, nil, nil, nil, nil, false, "", true,
	)

	request := createQuery(*userID, fclient.RelayEntry{})
	response := routing.GetTransactionFromRelay(httpReq, &request, relayAPI, *userID)
	assert.Equal(t, http.StatusOK, response.Code)

	jsonResponse := response.JSON.(fclient.RespGetRelayTransaction)
	assert.True(t, jsonResponse.EntriesQueued)
	assert.Equal(t, transaction, jsonResponse.Transaction)

	request = createQuery(*userID, fclient.RelayEntry{EntryID: jsonResponse.EntryID})
	response = routing.GetTransactionFromRelay(httpReq, &request, relayAPI, *userID)
	assert.Equal(t, http.StatusOK, response.Code)

	jsonResponse = response.JSON.(fclient.RespGetRelayTransaction)
	assert.True(t, jsonResponse.EntriesQueued)
	assert.Equal(t, transaction2, jsonResponse.Transaction)

	// And once more to clear the queue
	request = createQuery(*userID, fclient.RelayEntry{EntryID: jsonResponse.EntryID})
	response = routing.GetTransactionFromRelay(httpReq, &request, relayAPI, *userID)
	assert.Equal(t, http.StatusOK, response.Code)

	jsonResponse = response.JSON.(fclient.RespGetRelayTransaction)
	assert.False(t, jsonResponse.EntriesQueued)
	assert.Equal(t, gomatrixserverlib.Transaction{}, jsonResponse.Transaction)

	count, err := db.GetTransactionCount(ctx, *userID)
	assert.NoError(t, err)
	assert.Zero(t, count)
}
