// Copyright 2022 The Matrix.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/federationapi/storage/tables"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
)

// Schema for the relay servers table
const relayServersSchema = `
CREATE TABLE IF NOT EXISTS federationsender_relay_servers (
	-- The destination server name
	server_name TEXT NOT NULL,
	-- The relay server name for a given destination
	relay_server_name TEXT NOT NULL,
	UNIQUE (server_name, relay_server_name)
);

CREATE INDEX IF NOT EXISTS federationsender_relay_servers_server_name_idx
	ON federationsender_relay_servers (server_name);
`

// Schema revert for the relay servers table
const relayServersSchemaRevert = `
DROP TABLE IF EXISTS federationsender_relay_servers;
`

// SQL for inserting relay servers
const insertRelayServersSQL = "" +
	"INSERT INTO federationsender_relay_servers (server_name, relay_server_name) VALUES ($1, $2)" +
	" ON CONFLICT DO NOTHING"

// SQL for selecting relay servers
const selectRelayServersSQL = "" +
	"SELECT relay_server_name FROM federationsender_relay_servers WHERE server_name = $1"

// SQL for deleting relay servers
const deleteRelayServersSQL = "" +
	"DELETE FROM federationsender_relay_servers WHERE server_name = $1 AND relay_server_name = ANY($2)"

// SQL for deleting all relay servers
const deleteAllRelayServersSQL = "" +
	"DELETE FROM federationsender_relay_servers WHERE server_name = $1"

// relayServersTable stores information about relay servers
type relayServersTable struct {
	cm *sqlutil.Connections
	// SQL query string fields, initialized at construction
	insertRelayServersSQL    string
	selectRelayServersSQL    string
	deleteRelayServersSQL    string
	deleteAllRelayServersSQL string
}

// NewPostgresRelayServersTable creates a new postgres relay servers table
func NewPostgresRelayServersTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationRelayServers, error) {
	s := &relayServersTable{
		cm:                       cm,
		insertRelayServersSQL:    insertRelayServersSQL,
		selectRelayServersSQL:    selectRelayServersSQL,
		deleteRelayServersSQL:    deleteRelayServersSQL,
		deleteAllRelayServersSQL: deleteAllRelayServersSQL,
	}

	// Perform schema migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "federationapi_relay_servers_table_schema_001",
		Patch:       relayServersSchema,
		RevertPatch: relayServersSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertRelayServers adds relay servers to the table
func (s *relayServersTable) InsertRelayServers(
	ctx context.Context,

	serverName spec.ServerName,
	relayServers []spec.ServerName,
) error {
	db := s.cm.Connection(ctx, false)
	for _, relayServer := range relayServers {
		if err := db.Exec(s.insertRelayServersSQL, serverName, relayServer).Error; err != nil {
			return err
		}
	}
	return nil
}

// SelectRelayServers gets all relay servers for a server
func (s *relayServersTable) SelectRelayServers(
	ctx context.Context,

	serverName spec.ServerName,
) ([]spec.ServerName, error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectRelayServersSQL, serverName).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectRelayServers: rows.close() failed")

	var result []spec.ServerName
	for rows.Next() {
		var relayServer string
		if err = rows.Scan(&relayServer); err != nil {
			return nil, err
		}
		result = append(result, spec.ServerName(relayServer))
	}
	return result, rows.Err()
}

// DeleteRelayServers removes specific relay servers
func (s *relayServersTable) DeleteRelayServers(
	ctx context.Context,

	serverName spec.ServerName,
	relayServers []spec.ServerName,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteRelayServersSQL, serverName, pq.Array(relayServers)).Error
}

// DeleteAllRelayServers removes all relay servers for a server
func (s *relayServersTable) DeleteAllRelayServers(
	ctx context.Context,

	serverName spec.ServerName,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteAllRelayServersSQL, serverName).Error
}
