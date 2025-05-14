// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Global.org Foundation C.I.C.
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

package postgres

import (
	"context"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/shared"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
)

// Database stores information needed by the federation sender
type Database struct {
	shared.Database
}

// NewDatabase opens a new database
func NewDatabase(ctx context.Context, cm sqlutil.ConnectionManager, cache caching.FederationCache, isLocalServerName func(spec.ServerName) bool) (*Database, error) {
	var d Database

	blacklist, err := NewPostgresBlacklistTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	joinedHosts, err := NewPostgresJoinedHostsTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	queuePDUs, err := NewPostgresQueuePDUsTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	queueEDUs, err := NewPostgresQueueEDUsTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	queueJSON, err := NewPostgresQueueJSONTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	assumedOffline, err := NewPostgresAssumedOfflineTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	relayServers, err := NewPostgresRelayServersTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	inboundPeeks, err := NewPostgresInboundPeeksTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	outboundPeeks, err := NewPostgresOutboundPeeksTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	notaryJSON, err := NewPostgresNotaryServerKeysTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	notaryMetadata, err := NewPostgresNotaryServerKeysMetadataTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	serverSigningKeys, err := NewPostgresServerSigningKeysTable(ctx, cm)
	if err != nil {
		return nil, err
	}

	err = cm.Migrate(ctx)
	if err != nil {
		return nil, err
	}

	d.Database = shared.Database{
		Cm:                       cm,
		IsLocalServerName:        isLocalServerName,
		Cache:                    cache,
		FederationJoinedHosts:    joinedHosts,
		FederationQueuePDUs:      queuePDUs,
		FederationQueueEDUs:      queueEDUs,
		FederationQueueJSON:      queueJSON,
		FederationBlacklist:      blacklist,
		FederationAssumedOffline: assumedOffline,
		FederationRelayServers:   relayServers,
		FederationInboundPeeks:   inboundPeeks,
		FederationOutboundPeeks:  outboundPeeks,
		NotaryServerKeysJSON:     notaryJSON,
		NotaryServerKeysMetadata: notaryMetadata,
		ServerSigningKeys:        serverSigningKeys,
	}
	return &d, nil
}
