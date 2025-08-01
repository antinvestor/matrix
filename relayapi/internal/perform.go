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

package internal

import (
	"context"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/shared/receipt"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/relayapi/api"
	"github.com/pitabwire/util"
)

// SetRelayingEnabled implements api.RelayInternalAPI
func (r *RelayInternalAPI) SetRelayingEnabled(enabled bool) {
	r.relayingEnabledMutex.Lock()
	defer r.relayingEnabledMutex.Unlock()
	r.relayingEnabled = enabled
}

// RelayingEnabled implements api.RelayInternalAPI
func (r *RelayInternalAPI) RelayingEnabled() bool {
	r.relayingEnabledMutex.Lock()
	defer r.relayingEnabledMutex.Unlock()
	return r.relayingEnabled
}

// PerformRelayServerSync implements api.RelayInternalAPI
func (r *RelayInternalAPI) PerformRelayServerSync(
	ctx context.Context,
	userID spec.UserID,
	relayServer spec.ServerName,
) error {
	// Providing a default RelayEntry (EntryID = 0) is done to ask the relay if there are any
	// transactions available for this node.
	prevEntry := fclient.RelayEntry{}
	asyncResponse, err := r.fedClient.P2PGetTransactionFromRelay(ctx, userID, prevEntry, relayServer)
	if err != nil {
		util.Log(ctx).Error("P2PGetTransactionFromRelay: %s", err.Error())
		return err
	}
	r.processTransaction(ctx, &asyncResponse.Transaction)

	prevEntry = fclient.RelayEntry{EntryID: asyncResponse.EntryID}
	for asyncResponse.EntriesQueued {
		// There are still more entries available for this node from the relay.
		util.Log(ctx).Info("Retrieving next entry from relay, previous: %v", prevEntry)
		asyncResponse, err = r.fedClient.P2PGetTransactionFromRelay(ctx, userID, prevEntry, relayServer)
		prevEntry = fclient.RelayEntry{EntryID: asyncResponse.EntryID}
		if err != nil {
			util.Log(ctx).Error("P2PGetTransactionFromRelay: %s", err.Error())
			return err
		}
		r.processTransaction(ctx, &asyncResponse.Transaction)
	}

	return nil
}

// PerformStoreTransaction implements api.RelayInternalAPI
func (r *RelayInternalAPI) PerformStoreTransaction(
	ctx context.Context,
	transaction gomatrixserverlib.Transaction,
	userID spec.UserID,
) error {
	util.Log(ctx).Warn("Storing transaction for %v", userID)
	receiptTx, err := r.db.StoreTransaction(ctx, transaction)
	if err != nil {
		util.Log(ctx).Error("db.StoreTransaction: %s", err.Error())
		return err
	}
	err = r.db.AssociateTransactionWithDestinations(
		ctx,
		map[spec.UserID]struct{}{
			userID: {},
		},
		transaction.TransactionID,
		receiptTx)

	return err
}

// QueryTransactions implements api.RelayInternalAPI
func (r *RelayInternalAPI) QueryTransactions(
	ctx context.Context,
	userID spec.UserID,
	previousEntry fclient.RelayEntry,
) (api.QueryRelayTransactionsResponse, error) {
	log := util.Log(ctx).WithField("user_id", userID.String())
	log.Info("Querying transactions")
	if previousEntry.EntryID > 0 {
		log = log.WithField("previous_entry_id", previousEntry.EntryID)
		log.Info("Cleaning previous entry from database")
		prevReceipt := receipt.NewReceipt(previousEntry.EntryID)
		err := r.db.CleanTransactions(ctx, userID, []*receipt.Receipt{&prevReceipt})
		if err != nil {
			log.WithError(err).Error("Failed to clean transactions from database")
			return api.QueryRelayTransactionsResponse{}, err
		}
	}

	transaction, receiptTx, err := r.db.GetTransaction(ctx, userID)
	if err != nil {
		util.Log(ctx).Error("db.GetTransaction: %s", err.Error())
		return api.QueryRelayTransactionsResponse{}, err
	}

	response := api.QueryRelayTransactionsResponse{}
	if transaction != nil && receiptTx != nil {
		log = log.WithField("transaction_id", transaction.TransactionID)
		log.Info("Obtained transaction")
		response.Transaction = *transaction
		response.EntryID = receiptTx.GetNID()
		response.EntriesQueued = true
	} else {
		log.Info("No more entries in the queue")
		response.EntryID = 0
		response.EntriesQueued = false
	}

	return response, nil
}

func (r *RelayInternalAPI) processTransaction(ctx context.Context, txn *gomatrixserverlib.Transaction) {
	util.Log(ctx).Warn("Processing transaction from relay server")
	mu := internal.NewMutexByRoom()
	t := internal.NewTxnReq(
		r.rsAPI,
		nil,
		r.serverName,
		r.keyRing,
		mu,
		r.producer,
		r.presenceEnabledInbound,
		txn.PDUs,
		txn.EDUs,
		txn.Origin,
		txn.TransactionID,
		txn.Destination)

	t.ProcessTransaction(context.TODO())
}
