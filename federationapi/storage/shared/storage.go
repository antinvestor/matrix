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
	"fmt"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/shared/receipt"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/federationapi/types"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
)

type Database struct {
	Cm                       sqlutil.ConnectionManager
	IsLocalServerName        func(spec.ServerName) bool
	Cache                    cacheutil.FederationCache
	FederationQueuePDUs      tables.FederationQueuePDUs
	FederationQueueEDUs      tables.FederationQueueEDUs
	FederationQueueJSON      tables.FederationQueueJSON
	FederationJoinedHosts    tables.FederationJoinedHosts
	FederationBlacklist      tables.FederationBlacklist
	FederationAssumedOffline tables.FederationAssumedOffline
	FederationRelayServers   tables.FederationRelayServers
	FederationOutboundPeeks  tables.FederationOutboundPeeks
	FederationInboundPeeks   tables.FederationInboundPeeks
	NotaryServerKeysJSON     tables.FederationNotaryServerKeysJSON
	NotaryServerKeysMetadata tables.FederationNotaryServerKeysMetadata
	ServerSigningKeys        tables.FederationServerSigningKeys
}

// UpdateRoom updates the joined hosts for a room and returns what the joined
// hosts were before the update, or nil if this was a duplicate message.
// This is called when we receive a message from kafka, so we pass in
// oldEventID and newEventID to check that we haven't missed any messages or
// this isn't a duplicate message.
func (d *Database) UpdateRoom(
	ctx context.Context,
	roomID string,
	addHosts []types.JoinedHost,
	removeHosts []string,
	purgeRoomFirst bool,
) (joinedHosts []types.JoinedHost, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		if purgeRoomFirst {
			if err = d.FederationJoinedHosts.DeleteJoinedHostsForRoom(ctx, roomID); err != nil {
				return fmt.Errorf("d.FederationJoinedHosts.DeleteJoinedHosts: %w", err)
			}
			for _, add := range addHosts {
				if err = d.FederationJoinedHosts.InsertJoinedHosts(ctx, roomID, add.MemberEventID, add.ServerName); err != nil {
					return err
				}
				joinedHosts = append(joinedHosts, add)
			}
		} else {
			if joinedHosts, err = d.FederationJoinedHosts.SelectJoinedHosts(ctx, roomID); err != nil {
				return err
			}
			for _, add := range addHosts {
				if err = d.FederationJoinedHosts.InsertJoinedHosts(ctx, roomID, add.MemberEventID, add.ServerName); err != nil {
					return err
				}
			}
			if err = d.FederationJoinedHosts.DeleteJoinedHosts(ctx, removeHosts); err != nil {
				return err
			}
		}
		return nil
	})
	return
}

// GetJoinedHosts returns the currently joined hosts for room,
// as known to federationserver.
// Returns an error if something goes wrong.
func (d *Database) GetJoinedHosts(
	ctx context.Context, roomID string,
) ([]types.JoinedHost, error) {
	return d.FederationJoinedHosts.SelectJoinedHosts(ctx, roomID)
}

// GetAllJoinedHosts returns the currently joined hosts for
// all rooms known to the federation sender.
// Returns an error if something goes wrong.
func (d *Database) GetAllJoinedHosts(
	ctx context.Context,
) ([]spec.ServerName, error) {
	return d.FederationJoinedHosts.SelectAllJoinedHosts(ctx)
}

func (d *Database) GetJoinedHostsForRooms(
	ctx context.Context,
	roomIDs []string,
	excludeSelf,
	excludeBlacklisted bool,
) ([]spec.ServerName, error) {
	servers, err := d.FederationJoinedHosts.SelectJoinedHostsForRooms(ctx, roomIDs, excludeBlacklisted)
	if err != nil {
		return nil, err
	}
	if excludeSelf {
		for i, server := range servers {
			if d.IsLocalServerName(server) {
				copy(servers[i:], servers[i+1:])
				servers = servers[:len(servers)-1]
				break
			}
		}
	}
	return servers, nil
}

// StoreJSON adds a JSON blob into the queue JSON table and returns
// a NID. The NID will then be used when inserting the per-destination
// metadata entries.
func (d *Database) StoreJSON(
	ctx context.Context, js string,
) (*receipt.Receipt, error) {
	var nid int64
	var err error
	_ = d.Cm.Do(ctx, func(ctx context.Context) error {
		nid, err = d.FederationQueueJSON.InsertQueueJSON(ctx, js)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("d.insertQueueJSON: %w", err)
	}
	newReceipt := receipt.NewReceipt(nid)
	return &newReceipt, nil
}

func (d *Database) AddServerToBlacklist(
	ctx context.Context, serverName spec.ServerName,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationBlacklist.InsertBlacklist(ctx, serverName)
	})
}

func (d *Database) RemoveServerFromBlacklist(
	ctx context.Context, serverName spec.ServerName,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationBlacklist.DeleteBlacklist(ctx, serverName)
	})
}

func (d *Database) RemoveAllServersFromBlacklist(ctx context.Context) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationBlacklist.DeleteAllBlacklist(ctx)
	})
}

func (d *Database) IsServerBlacklisted(
	ctx context.Context, serverName spec.ServerName,
) (bool, error) {
	return d.FederationBlacklist.SelectBlacklist(ctx, serverName)
}

func (d *Database) SetServerAssumedOffline(
	ctx context.Context,
	serverName spec.ServerName,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationAssumedOffline.InsertAssumedOffline(ctx, serverName)
	})
}

func (d *Database) RemoveServerAssumedOffline(
	ctx context.Context,
	serverName spec.ServerName,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationAssumedOffline.DeleteAssumedOffline(ctx, serverName)
	})
}

func (d *Database) RemoveAllServersAssumedOffline(
	ctx context.Context,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationAssumedOffline.DeleteAllAssumedOffline(ctx)
	})
}

func (d *Database) IsServerAssumedOffline(
	ctx context.Context,
	serverName spec.ServerName,
) (bool, error) {
	return d.FederationAssumedOffline.SelectAssumedOffline(ctx, serverName)
}

func (d *Database) P2PAddRelayServersForServer(
	ctx context.Context,
	serverName spec.ServerName,
	relayServers []spec.ServerName,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationRelayServers.InsertRelayServers(ctx, serverName, relayServers)
	})
}

func (d *Database) P2PGetRelayServersForServer(
	ctx context.Context,
	serverName spec.ServerName,
) ([]spec.ServerName, error) {
	return d.FederationRelayServers.SelectRelayServers(ctx, serverName)
}

func (d *Database) P2PRemoveRelayServersForServer(
	ctx context.Context,
	serverName spec.ServerName,
	relayServers []spec.ServerName,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationRelayServers.DeleteRelayServers(ctx, serverName, relayServers)
	})
}

func (d *Database) P2PRemoveAllRelayServersForServer(
	ctx context.Context,
	serverName spec.ServerName,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationRelayServers.DeleteAllRelayServers(ctx, serverName)
	})
}

func (d *Database) AddOutboundPeek(
	ctx context.Context,
	serverName spec.ServerName,
	roomID string,
	peekID string,
	renewalInterval int64,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationOutboundPeeks.InsertOutboundPeek(ctx, serverName, roomID, peekID, renewalInterval)
	})
}

func (d *Database) RenewOutboundPeek(
	ctx context.Context,
	serverName spec.ServerName,
	roomID string,
	peekID string,
	renewalInterval int64,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationOutboundPeeks.RenewOutboundPeek(ctx, serverName, roomID, peekID, renewalInterval)
	})
}

func (d *Database) GetOutboundPeek(
	ctx context.Context,
	serverName spec.ServerName,
	roomID,
	peekID string,
) (*types.OutboundPeek, error) {
	return d.FederationOutboundPeeks.SelectOutboundPeek(ctx, serverName, roomID, peekID)
}

func (d *Database) GetOutboundPeeks(
	ctx context.Context,
	roomID string,
) ([]types.OutboundPeek, error) {
	return d.FederationOutboundPeeks.SelectOutboundPeeks(ctx, roomID)
}

func (d *Database) AddInboundPeek(
	ctx context.Context,
	serverName spec.ServerName,
	roomID string,
	peekID string,
	renewalInterval int64,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationInboundPeeks.InsertInboundPeek(ctx, serverName, roomID, peekID, renewalInterval)
	})
}

func (d *Database) RenewInboundPeek(
	ctx context.Context,
	serverName spec.ServerName,
	roomID string,
	peekID string,
	renewalInterval int64,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.FederationInboundPeeks.RenewInboundPeek(ctx, serverName, roomID, peekID, renewalInterval)
	})
}

func (d *Database) GetInboundPeek(
	ctx context.Context,
	serverName spec.ServerName,
	roomID string,
	peekID string,
) (*types.InboundPeek, error) {
	return d.FederationInboundPeeks.SelectInboundPeek(ctx, serverName, roomID, peekID)
}

func (d *Database) GetInboundPeeks(
	ctx context.Context,
	roomID string,
) ([]types.InboundPeek, error) {
	return d.FederationInboundPeeks.SelectInboundPeeks(ctx, roomID)
}

func (d *Database) UpdateNotaryKeys(
	ctx context.Context,
	serverName spec.ServerName,
	serverKeys gomatrixserverlib.ServerKeys,
) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		validUntil := serverKeys.ValidUntilTS
		// Servers MUST use the lesser of this field and 7 days into the future when determining if a key is valid.
		// This is to avoid a situation where an attacker publishes a key which is valid for a significant amount of
		// time without a way for the homeserver owner to revoke it.
		// https://spec.matrix.org/unstable/server-server-api/#querying-keys-through-another-server
		weekIntoFuture := time.Now().Add(7 * 24 * time.Hour)
		if weekIntoFuture.Before(validUntil.Time()) {
			validUntil = spec.AsTimestamp(weekIntoFuture)
		}
		notaryID, err := d.NotaryServerKeysJSON.InsertJSONResponse(ctx, serverKeys, serverName, validUntil)
		if err != nil {
			return err
		}
		// update the metadata for the keys
		for keyID := range serverKeys.OldVerifyKeys {
			_, err = d.NotaryServerKeysMetadata.UpsertKey(ctx, serverName, keyID, notaryID, validUntil)
			if err != nil {
				return err
			}
		}
		for keyID := range serverKeys.VerifyKeys {
			_, err = d.NotaryServerKeysMetadata.UpsertKey(ctx, serverName, keyID, notaryID, validUntil)
			if err != nil {
				return err
			}
		}

		// clean up old responses
		return d.NotaryServerKeysMetadata.DeleteOldJSONResponses(ctx)
	})
}

func (d *Database) GetNotaryKeys(
	ctx context.Context,
	serverName spec.ServerName,
	optKeyIDs []gomatrixserverlib.KeyID,
) (sks []gomatrixserverlib.ServerKeys, err error) {
	err = d.Cm.Do(ctx, func(ctx context.Context) error {
		sks, err = d.NotaryServerKeysMetadata.SelectKeys(ctx, serverName, optKeyIDs)
		return err
	})
	return sks, err
}

func (d *Database) PurgeRoom(ctx context.Context, roomID string) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		if err := d.FederationJoinedHosts.DeleteJoinedHostsForRoom(ctx, roomID); err != nil {
			return fmt.Errorf("failed to purge joined hosts: %w", err)
		}
		if err := d.FederationInboundPeeks.DeleteInboundPeeks(ctx, roomID); err != nil {
			return fmt.Errorf("failed to purge inbound peeks: %w", err)
		}
		if err := d.FederationOutboundPeeks.DeleteOutboundPeeks(ctx, roomID); err != nil {
			return fmt.Errorf("failed to purge outbound peeks: %w", err)
		}
		return nil
	})
}
