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
	"github.com/antinvestor/matrix/federationapi/storage/shared"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
)

// Migrations All federationapi migrations for the postgres module
var Migrations = []sqlutil.Migration{
	{
		Version:   "federationapi_001_create_queue_json_table",
		QueryUp:   queueJSONSchema,
		QueryDown: queueJSONSchemaRevert,
	},
	{
		Version:   "federationapi_002_create_queue_pdus_table",
		QueryUp:   queuePDUsSchema,
		QueryDown: queuePDUsSchemaRevert,
	},
	{
		Version:   "federationapi_003_create_queue_edus_table",
		QueryUp:   queueEDUsSchema,
		QueryDown: queueEDUsSchemaRevert,
	},
	{
		Version:   "federationapi_004_create_server_keys_table",
		QueryUp:   serverSigningKeysSchema,
		QueryDown: serverSigningKeysSchemaRevert,
	},
	{
		Version:   "federationapi_005_create_blacklist_table",
		QueryUp:   blacklistSchema,
		QueryDown: blacklistSchemaRevert,
	},
	{
		Version:   "federationapi_006_create_notary_server_keys_json_table",
		QueryUp:   notaryServerKeysJSONSchema,
		QueryDown: notaryServerKeysJSONSchemaRevert,
	},
	{
		Version:   "federationapi_007_create_joined_hosts_table",
		QueryUp:   joinedHostsSchema,
		QueryDown: joinedHostsSchemaRevert,
	},
	{
		Version:   "federationapi_008_create_assumed_offline_table",
		QueryUp:   assumedOfflineSchema,
		QueryDown: assumedOfflineSchemaRevert,
	},
	{
		Version:   "federationapi_009_create_inbound_peeks_table",
		QueryUp:   inboundPeeksSchema,
		QueryDown: inboundPeeksSchemaRevert,
	},
	{
		Version:   "federationapi_010_create_outbound_peeks_table",
		QueryUp:   outboundPeeksSchema,
		QueryDown: outboundPeeksSchemaRevert,
	},
	{
		Version:   "federationapi_011_create_relay_servers_table",
		QueryUp:   relayServersSchema,
		QueryDown: relayServersSchemaRevert,
	},
	{
		Version:   "federationapi_012_create_notary_server_keys_metadata_table",
		QueryUp:   notaryServerKeysMetadataSchema,
		QueryDown: notaryServerKeysMetadataSchemaRevert,
	},
}

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

	m := sqlutil.NewMigrator(d.db)
	m.AddMigrations(Migrations...)
	err = m.Up(ctx)
	if err != nil {
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
