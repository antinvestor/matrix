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

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
)

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

// SQL query string constants
const (
	// insertRelayServersSQL inserts a relay server for a destination with conflict handling
	insertRelayServersSQL = "" +
		"INSERT INTO federationsender_relay_servers (server_name, relay_server_name) VALUES ($1, $2)" +
		" ON CONFLICT DO NOTHING"

	// selectRelayServersSQL retrieves all relay servers for a given destination
	selectRelayServersSQL = "" +
		"SELECT relay_server_name FROM federationsender_relay_servers WHERE server_name = $1"

	// deleteRelayServersSQL removes specific relay servers for a destination
	deleteRelayServersSQL = "" +
		"DELETE FROM federationsender_relay_servers WHERE server_name = $1 AND relay_server_name = ANY($2)"

	// deleteAllRelayServersSQL removes all relay servers for a given destination
	deleteAllRelayServersSQL = "" +
		"DELETE FROM federationsender_relay_servers WHERE server_name = $1"
)

// relayServersTable contains the postgres-specific implementation
type relayServersTable struct {
	cm *sqlutil.Connections

	insertRelayServersStmt    string
	selectRelayServersStmt    string
	deleteRelayServersStmt    string
	deleteAllRelayServersStmt string
}

// NewPostgresRelayServersTable creates a new postgres relay servers table
func NewPostgresRelayServersTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationRelayServers, error) {
	// Initialize schema using GORM
	gormDB := cm.Connection(ctx, false)
	if err := gormDB.Exec(relayServersSchema).Error; err != nil {
		return nil, err
	}

	s := &relayServersTable{
		cm:                        cm,
		insertRelayServersStmt:    insertRelayServersSQL,
		selectRelayServersStmt:    selectRelayServersSQL,
		deleteRelayServersStmt:    deleteRelayServersSQL,
		deleteAllRelayServersStmt: deleteAllRelayServersSQL,
	}

	return s, nil
}

// InsertRelayServers adds relay servers for a destination
func (s *relayServersTable) InsertRelayServers(
	ctx context.Context,
	serverName spec.ServerName,
	relayServers []spec.ServerName,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	for _, relayServer := range relayServers {
		if err := db.Exec(
			s.insertRelayServersStmt,
			serverName, relayServer,
		).Error; err != nil {
			return err
		}
	}
	return nil
}

// SelectRelayServers retrieves all relay servers for a destination
func (s *relayServersTable) SelectRelayServers(
	ctx context.Context,
	serverName spec.ServerName,
) ([]spec.ServerName, error) {
	// Get read-only database connection
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(s.selectRelayServersStmt, serverName).Rows()
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

// DeleteRelayServers removes specific relay servers for a destination
func (s *relayServersTable) DeleteRelayServers(
	ctx context.Context,
	serverName spec.ServerName,
	relayServers []spec.ServerName,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	// Convert []spec.ServerName to []string for pq.Array
	relayServerStrings := make([]string, len(relayServers))
	for i, server := range relayServers {
		relayServerStrings[i] = string(server)
	}

	return db.Exec(
		s.deleteRelayServersStmt,
		serverName, pq.Array(relayServerStrings),
	).Error
}

// DeleteAllRelayServers removes all relay servers for a destination
func (s *relayServersTable) DeleteAllRelayServers(
	ctx context.Context,
	serverName spec.ServerName,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(s.deleteAllRelayServersStmt, serverName).Error
}
