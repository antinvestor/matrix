// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Matrix.org Foundation C.I.C.
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
	"database/sql"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/postgres/deltas"
	"github.com/antinvestor/matrix/federationapi/storage/shared"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
)

// Database stores information needed by the federation sender
type Database struct {
	shared.Database
	db     *sql.DB
	writer sqlutil.Writer
}

// NewDatabase opens a new database
func NewDatabase(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions, cache caching.FederationCache, isLocalServerName func(spec.ServerName) bool) (*Database, error) {
	var d Database
	var err error
	if d.db, d.writer, err = conMan.Connection(ctx, dbProperties); err != nil {
		return nil, err
	}
	blacklist, err := NewPostgresBlacklistTable(ctx, d.db)
	if err != nil {
		return nil, err
	}
	joinedHosts, err := NewPostgresJoinedHostsTable(ctx, d.db)
	if err != nil {
		return nil, err
	}
	queuePDUs, err := NewPostgresQueuePDUsTable(ctx, d.db)
	if err != nil {
		return nil, err
	}
	queueEDUs, err := NewPostgresQueueEDUsTable(ctx, d.db)
	if err != nil {
		return nil, err
	}
	queueJSON, err := NewPostgresQueueJSONTable(ctx, d.db)
	if err != nil {
		return nil, err
	}
	assumedOffline, err := NewPostgresAssumedOfflineTable(ctx, d.db)
	if err != nil {
		return nil, err
	}
	relayServers, err := NewPostgresRelayServersTable(ctx, d.db)
	if err != nil {
		return nil, err
	}
	inboundPeeks, err := NewPostgresInboundPeeksTable(ctx, d.db)
	if err != nil {
		return nil, err
	}
	outboundPeeks, err := NewPostgresOutboundPeeksTable(ctx, d.db)
	if err != nil {
		return nil, err
	}
	notaryJSON, err := NewPostgresNotaryServerKeysTable(ctx, d.db)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresNotaryServerKeysTable: %s", err)
	}
	notaryMetadata, err := NewPostgresNotaryServerKeysMetadataTable(ctx, d.db)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresNotaryServerKeysMetadataTable: %s", err)
	}
	serverSigningKeys, err := NewPostgresServerSigningKeysTable(ctx, d.db)
	if err != nil {
		return nil, err
	}
	m := sqlutil.NewMigrator(d.db)
	m.AddMigrations(sqlutil.Migration{
		Version: "federationsender: drop federationsender_rooms",
		Up:      deltas.UpRemoveRoomsTable,
	})
	err = m.Up(ctx)
	if err != nil {
		return nil, err
	}
	if err = queueEDUs.Prepare(); err != nil {
		return nil, err
	}
	d.Database = shared.Database{
		DB:                       d.db,
		IsLocalServerName:        isLocalServerName,
		Cache:                    cache,
		Writer:                   d.writer,
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
