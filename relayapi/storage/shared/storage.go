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

package shared

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/shared/receipt"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/relayapi/storage/tables"
)

type Database struct {
	Cm                sqlutil.ConnectionManager
	IsLocalServerName func(spec.ServerName) bool
	Cache             cacheutil.FederationCache
	RelayQueue        tables.RelayQueue
	RelayQueueJSON    tables.RelayQueueJSON
}

func (d *Database) StoreTransaction(
	ctx context.Context,
	transaction gomatrixserverlib.Transaction,
) (*receipt.Receipt, error) {
	var err error
	jsonTransaction, err := json.Marshal(transaction)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal: %w", err)
	}

	var nid int64
	_ = d.Cm.Do(ctx, func(ctx context.Context) error {
		nid, err = d.RelayQueueJSON.InsertQueueJSON(ctx, string(jsonTransaction))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("d.insertQueueJSON: %w", err)
	}

	newReceipt := receipt.NewReceipt(nid)
	return &newReceipt, nil
}

func (d *Database) AssociateTransactionWithDestinations(
	ctx context.Context,
	destinations map[spec.UserID]struct{},
	transactionID gomatrixserverlib.TransactionID,
	dbReceipt *receipt.Receipt,
) error {
	err := d.Cm.Do(ctx, func(ctx context.Context) error {
		var lastErr error
		for destination := range destinations {
			destination := destination
			err := d.RelayQueue.InsertQueueEntry(
				ctx,
				transactionID,
				destination.Domain(),
				dbReceipt.GetNID(),
			)
			if err != nil {
				lastErr = fmt.Errorf("d.insertQueueEntry: %w", err)
			}
		}
		return lastErr
	})

	return err
}

func (d *Database) CleanTransactions(
	ctx context.Context,
	userID spec.UserID,
	receipts []*receipt.Receipt,
) error {
	nids := make([]int64, len(receipts))
	for i, dbReceipt := range receipts {
		nids[i] = dbReceipt.GetNID()
	}

	err := d.Cm.Do(ctx, func(ctx context.Context) error {
		deleteEntryErr := d.RelayQueue.DeleteQueueEntries(ctx, userID.Domain(), nids)
		// TODO : If there are still queue entries for any of these nids for other destinations
		// then we shouldn't delete the json entries.
		// But this can't happen with the current api design.
		// There will only ever be one server entry for each nid since each call to send_relay
		// only accepts a single server name and inside there we create a new json entry.
		// So for multiple destinations we would call send_relay multiple times and have multiple
		// json entries of the same transaction.
		//
		// TLDR; this works as expected right now but can easily be optimised in the future.
		deleteJSONErr := d.RelayQueueJSON.DeleteQueueJSON(ctx, nids)

		if deleteEntryErr != nil {
			return fmt.Errorf("d.deleteQueueEntries: %w", deleteEntryErr)
		}
		if deleteJSONErr != nil {
			return fmt.Errorf("d.deleteQueueJSON: %w", deleteJSONErr)
		}
		return nil
	})

	return err
}

func (d *Database) GetTransaction(
	ctx context.Context,
	userID spec.UserID,
) (*gomatrixserverlib.Transaction, *receipt.Receipt, error) {
	entriesRequested := 1
	nids, err := d.RelayQueue.SelectQueueEntries(ctx, userID.Domain(), entriesRequested)
	if err != nil {
		return nil, nil, fmt.Errorf("d.SelectQueueEntries: %w", err)
	}
	if len(nids) == 0 {
		return nil, nil, nil
	}
	firstNID := nids[0]

	txns := map[int64][]byte{}
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		txns, err = d.RelayQueueJSON.SelectQueueJSON(ctx, nids)
		return err
	})
	if err != nil {
		return nil, nil, fmt.Errorf("d.SelectQueueJSON: %w", err)
	}

	transaction := &gomatrixserverlib.Transaction{}
	if _, ok := txns[firstNID]; !ok {
		return nil, nil, fmt.Errorf("failed retrieving json blob for transaction: %d", firstNID)
	}

	err = json.Unmarshal(txns[firstNID], transaction)
	if err != nil {
		return nil, nil, fmt.Errorf("unmarshal transaction: %w", err)
	}

	newReceipt := receipt.NewReceipt(firstNID)
	return transaction, &newReceipt, nil
}

func (d *Database) GetTransactionCount(
	ctx context.Context,
	userID spec.UserID,
) (int64, error) {
	count, err := d.RelayQueue.SelectQueueEntryCount(ctx, userID.Domain())
	if err != nil {
		return 0, fmt.Errorf("d.SelectQueueEntryCount: %w", err)
	}
	return count, nil
}
