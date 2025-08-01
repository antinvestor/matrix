// Copyright 2025 Ant Investor Ltd.
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
	"errors"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/shared/receipt"
	"github.com/antinvestor/matrix/roomserver/types"
)

// AssociatePDUWithDestination creates an association that the
// destination queues will use to determine which JSON blobs to send
// to which servers.
func (d *Database) AssociatePDUWithDestinations(
	ctx context.Context,
	destinations map[spec.ServerName]struct{},
	dbReceipt *receipt.Receipt,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		var err error
		for destination := range destinations {
			err = d.FederationQueuePDUs.InsertQueuePDU(
				ctx,                // context
				"",                 // transaction ID
				destination,        // destination server name
				dbReceipt.GetNID(), // NID from the federationapi_queue_json table
			)
		}
		return err
	})
}

// GetNextTransactionPDUs retrieves events from the database for
// the next pending transaction, up to the limit specified.
func (d *Database) GetPendingPDUs(
	ctx context.Context,
	serverName spec.ServerName,
	limit int,
) (
	events map[*receipt.Receipt]*types.HeaderedEvent,
	err error,
) {
	// Strictly speaking this doesn't need to be using the writer
	// since we are only performing selects, but since we don't have
	// a guarantee of transactional isolation, it's actually useful
	// to know in SQLite mode that nothing else is trying to modify
	// the database.
	events = make(map[*receipt.Receipt]*types.HeaderedEvent)
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		nids, err := d.FederationQueuePDUs.SelectQueuePDUs(ctx, serverName, limit)
		if err != nil {
			return fmt.Errorf("SelectQueuePDUs: %w", err)
		}

		retrieve := make([]int64, 0, len(nids))
		for _, nid := range nids {
			if event, ok := d.Cache.GetFederationQueuedPDU(ctx, nid); ok {
				newReceipt := receipt.NewReceipt(nid)
				events[&newReceipt] = event
			} else {
				retrieve = append(retrieve, nid)
			}
		}

		blobs, err := d.FederationQueueJSON.SelectQueueJSON(ctx, retrieve)
		if err != nil {
			return fmt.Errorf("SelectQueueJSON: %w", err)
		}

		for nid, blob := range blobs {
			var event types.HeaderedEvent
			if err := json.Unmarshal(blob, &event); err != nil {
				return fmt.Errorf("json.Unmarshal: %w", err)
			}
			newReceipt := receipt.NewReceipt(nid)
			events[&newReceipt] = &event
			_ = d.Cache.StoreFederationQueuedPDU(ctx, nid, &event)
		}

		return nil
	})
	return
}

// CleanTransactionPDUs cleans up all associated events for a
// given transaction. This is done when the transaction was sent
// successfully.
func (d *Database) CleanPDUs(
	ctx context.Context,
	serverName spec.ServerName,
	receipts []*receipt.Receipt,
) error {
	if len(receipts) == 0 {
		return errors.New("expected receipt")
	}

	nids := make([]int64, len(receipts))
	for i := range receipts {
		nids[i] = receipts[i].GetNID()
	}

	return d.Cm.Do(ctx, func(ctx context.Context) error {
		if err := d.FederationQueuePDUs.DeleteQueuePDUs(ctx, serverName, nids); err != nil {
			return err
		}

		var deleteNIDs []int64
		for _, nid := range nids {
			count, err := d.FederationQueuePDUs.SelectQueuePDUReferenceJSONCount(ctx, nid)
			if err != nil {
				return fmt.Errorf("SelectQueuePDUReferenceJSONCount: %w", err)
			}
			if count == 0 {
				deleteNIDs = append(deleteNIDs, nid)
				_ = d.Cache.EvictFederationQueuedPDU(ctx, nid)
			}
		}

		if len(deleteNIDs) > 0 {
			if err := d.FederationQueueJSON.DeleteQueueJSON(ctx, deleteNIDs); err != nil {
				return fmt.Errorf("DeleteQueueJSON: %w", err)
			}
		}

		return nil
	})
}

// GetPendingServerNames returns the server names that have PDUs
// waiting to be sent.
func (d *Database) GetPendingPDUServerNames(
	ctx context.Context,
) ([]spec.ServerName, error) {
	return d.FederationQueuePDUs.SelectQueuePDUServerNames(ctx)
}
