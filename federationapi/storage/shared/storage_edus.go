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
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/shared/receipt"
)

// defaultExpiry for EDUs if not listed below
var defaultExpiry = time.Hour * 24

// defaultExpireEDUTypes contains EDUs which can/should be expired after a given time
// if the target server isn't reachable for some reason.
var defaultExpireEDUTypes = map[string]time.Duration{
	spec.MTyping:   time.Minute,
	spec.MPresence: time.Minute * 10,
}

// AssociateEDUWithDestination creates an association that the
// destination queues will use to determine which JSON blobs to send
// to which servers.
func (d *Database) AssociateEDUWithDestinations(
	ctx context.Context,
	destinations map[spec.ServerName]struct{},
	dbReceipt *receipt.Receipt,
	eduType string,
	expireEDUTypes map[string]time.Duration,
) error {
	if expireEDUTypes == nil {
		expireEDUTypes = defaultExpireEDUTypes
	}
	expiresAt := spec.AsTimestamp(time.Now().Add(defaultExpiry))
	if duration, ok := expireEDUTypes[eduType]; ok {
		// Keep EDUs for at least x minutes before deleting them
		expiresAt = spec.AsTimestamp(time.Now().Add(duration))
	}
	// We forcibly set m.direct_to_device and m.device_list_update events
	// to 0, as we always want them to be delivered. (required for E2EE)
	if eduType == spec.MDirectToDevice || eduType == spec.MDeviceListUpdate {
		expiresAt = 0
	}
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		var err error
		for destination := range destinations {
			err = d.FederationQueueEDUs.InsertQueueEDU(
				ctx,                // context
				eduType,            // EDU type for coalescing
				destination,        // destination server name
				dbReceipt.GetNID(), // NID from the federationapi_queue_json table
				expiresAt,          // The timestamp this EDU will expire
			)
		}
		return err
	})
}

// GetNextTransactionEDUs retrieves events from the database for
// the next pending transaction, up to the limit specified.
func (d *Database) GetPendingEDUs(
	ctx context.Context,
	serverName spec.ServerName,
	limit int,
) (
	edus map[*receipt.Receipt]*gomatrixserverlib.EDU,
	err error,
) {
	edus = make(map[*receipt.Receipt]*gomatrixserverlib.EDU)
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		nids, err := d.FederationQueueEDUs.SelectQueueEDUs(ctx, serverName, limit)
		if err != nil {
			return fmt.Errorf("SelectQueueEDUs: %w", err)
		}

		retrieve := make([]int64, 0, len(nids))
		for _, nid := range nids {
			if edu, ok := d.Cache.GetFederationQueuedEDU(ctx, nid); ok {
				newReceipt := receipt.NewReceipt(nid)
				edus[&newReceipt] = edu
			} else {
				retrieve = append(retrieve, nid)
			}
		}

		blobs, err := d.FederationQueueJSON.SelectQueueJSON(ctx, retrieve)
		if err != nil {
			return fmt.Errorf("SelectQueueJSON: %w", err)
		}

		for nid, blob := range blobs {
			var event gomatrixserverlib.EDU
			if err := json.Unmarshal(blob, &event); err != nil {
				return fmt.Errorf("json.Unmarshal: %w", err)
			}
			newReceipt := receipt.NewReceipt(nid)
			edus[&newReceipt] = &event
			_ = d.Cache.StoreFederationQueuedEDU(ctx, nid, &event)
		}

		return nil
	})
	return
}

// CleanEDUs cleans up all specified EDUs. This is done when a
// transaction was sent successfully.
func (d *Database) CleanEDUs(
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
		if err := d.FederationQueueEDUs.DeleteQueueEDUs(ctx, serverName, nids); err != nil {
			return err
		}

		var deleteNIDs []int64
		for _, nid := range nids {
			count, err := d.FederationQueueEDUs.SelectQueueEDUReferenceJSONCount(ctx, nid)
			if err != nil {
				return fmt.Errorf("SelectQueueEDUReferenceJSONCount: %w", err)
			}
			if count == 0 {
				deleteNIDs = append(deleteNIDs, nid)
				_ = d.Cache.EvictFederationQueuedEDU(ctx, nid)
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

// GetPendingEDUServerNames returns the server names that have EDUs
// waiting to be sent.
func (d *Database) GetPendingEDUServerNames(
	ctx context.Context,
) ([]spec.ServerName, error) {
	return d.FederationQueueEDUs.SelectQueueEDUServerNames(ctx)
}

// DeleteExpiredEDUs deletes expired EDUs and evicts them from the cache.
func (d *Database) DeleteExpiredEDUs(ctx context.Context) error {
	var jsonNIDs []int64
	err := d.Cm.Do(ctx, func(ctx context.Context) (err error) {
		expiredBefore := spec.AsTimestamp(time.Now())
		jsonNIDs, err = d.FederationQueueEDUs.SelectExpiredEDUs(ctx, expiredBefore)
		if err != nil {
			return err
		}
		if len(jsonNIDs) == 0 {
			return nil
		}

		if err = d.FederationQueueJSON.DeleteQueueJSON(ctx, jsonNIDs); err != nil {
			return err
		}

		return d.FederationQueueEDUs.DeleteExpiredEDUs(ctx, expiredBefore)
	})

	if err != nil {
		return err
	}

	for i := range jsonNIDs {
		_ = d.Cache.EvictFederationQueuedEDU(ctx, jsonNIDs[i])
	}

	return nil
}
