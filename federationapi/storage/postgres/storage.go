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
	"database/sql"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/shared"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/pitabwire/frame"
)

// Migrations All federationapi migrations for the postgres module
var Migrations = []frame.MigrationPatch{
	{
		Name:        "federationapi_001_create_queue_json_table",
		Patch:       queueJSONSchema,
		RevertPatch: queueJSONSchemaRevert,
	},
	{
		Name:        "federationapi_002_create_queue_pdus_table",
		Patch:       queuePDUsSchema,
		RevertPatch: queuePDUsSchemaRevert,
	},
	{
		Name:        "federationapi_003_create_queue_edus_table",
		Patch:       queueEDUsSchema,
		RevertPatch: queueEDUsSchemaRevert,
	},
	{
		Name:        "federationapi_004_create_server_keys_table",
		Patch:       serverSigningKeysSchema,
		RevertPatch: serverSigningKeysSchemaRevert,
	},
	{
		Name:        "federationapi_005_create_blacklist_table",
		Patch:       blacklistSchema,
		RevertPatch: blacklistSchemaRevert,
	},
	{
		Name:        "federationapi_006_create_notary_server_keys_json_table",
		Patch:       notaryServerKeysJSONSchema,
		RevertPatch: notaryServerKeysJSONSchemaRevert,
	},
	{
		Name:        "federationapi_007_create_joined_hosts_table",
		Patch:       joinedHostsSchema,
		RevertPatch: joinedHostsSchemaRevert,
	},
	{
		Name:        "federationapi_008_create_assumed_offline_table",
		Patch:       assumedOfflineSchema,
		RevertPatch: assumedOfflineSchemaRevert,
	},
	{
		Name:        "federationapi_009_create_inbound_peeks_table",
		Patch:       inboundPeeksSchema,
		RevertPatch: inboundPeeksSchemaRevert,
	},
	{
		Name:        "federationapi_010_create_outbound_peeks_table",
		Patch:       outboundPeeksSchema,
		RevertPatch: outboundPeeksSchemaRevert,
	},
	{
		Name:        "federationapi_011_create_relay_servers_table",
		Patch:       relayServersSchema,
		RevertPatch: relayServersSchemaRevert,
	},
	{
		Name:        "federationapi_012_create_notary_server_keys_metadata_table",
		Patch:       notaryServerKeysMetadataSchema,
		RevertPatch: notaryServerKeysMetadataSchemaRevert,
	},
}

// Database stores information needed by the federation sender
type Database struct {
	shared.Database
	db     *sql.DB
	writer sqlutil.Writer
}

// NewDatabase opens a new database
func NewDatabase(ctx context.Context, cm *sqlutil.Connections, cache caching.FederationCache, isLocalServerName func(spec.ServerName) bool) (*Database, error) {
	var d Database

	err := cm.MigrateStrings(ctx, Migrations...)
	if err != nil {
		return nil, err
	}

	assumedOffline := NewPostgresAssumedOfflineTable(cm)
	blacklist := NewPostgresBlacklistTable(cm)
	inboundPeeks := NewPostgresInboundPeeksTable(cm)
	joinedHosts := NewPostgresJoinedHostsTable(cm)
	queuePDUs := NewPostgresQueuePDUsTable(cm)
	queueEDUs := NewPostgresQueueEDUsTable(cm)
	queueJSON := NewPostgresQueueJSONTable(cm)
	relayServers := NewPostgresRelayServersTable(cm)
	outboundPeeks := NewPostgresOutboundPeeksTable(cm)
	notaryJSON := NewPostgresNotaryServerKeysJSONTable(cm)
	notaryMetadata := NewPostgresNotaryServerKeysMetadataTable(cm)
	serverSigningKeys := NewPostgresServerSigningKeysTable(cm)

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
