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

package postgres

import (
	"context"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
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

const relayServersSchemaRevert = `DROP TABLE IF EXISTS federationsender_relay_servers; DROP INDEX IF EXISTS federationsender_relay_servers_server_name_idx;`

const insertRelayServersSQL = "" +
	"INSERT INTO federationsender_relay_servers (server_name, relay_server_name) VALUES ($1, $2)" +
	" ON CONFLICT DO NOTHING"

const selectRelayServersSQL = "" +
	"SELECT relay_server_name FROM federationsender_relay_servers WHERE server_name = $1"

const deleteRelayServersSQL = "" +
	"DELETE FROM federationsender_relay_servers WHERE server_name = $1 AND relay_server_name = ANY($2)"

const deleteAllRelayServersSQL = "" +
	"DELETE FROM federationsender_relay_servers WHERE server_name = $1"

// relayServersTable provides methods for relay servers operations using GORM.
type relayServersTable struct {
	cm                 *sqlutil.Connections
	InsertSQL          string
	SelectSQL          string
	DeleteSQL          string
	DeleteAllSQL       string
}

// NewPostgresRelayServersTable initializes a relayServersTable with SQL constants and a connection manager
func NewPostgresRelayServersTable(cm *sqlutil.Connections) tables.FederationRelayServers {
	return &relayServersTable{
		cm:           cm,
		InsertSQL:    insertRelayServersSQL,
		SelectSQL:    selectRelayServersSQL,
		DeleteSQL:    deleteRelayServersSQL,
		DeleteAllSQL: deleteAllRelayServersSQL,
	}
}

// InsertRelayServers inserts relay servers for a given server name
func (t *relayServersTable) InsertRelayServers(ctx context.Context, serverName spec.ServerName, relayServers []spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	for _, relayServer := range relayServers {
		if err := db.Exec(t.InsertSQL, serverName, relayServer).Error; err != nil {
			return err
		}
	}
	return nil
}

// SelectRelayServers selects relay servers for a given server name
func (t *relayServersTable) SelectRelayServers(ctx context.Context, serverName spec.ServerName) ([]spec.ServerName, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.SelectSQL, serverName).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

// DeleteRelayServers deletes specific relay servers for a given server name
func (t *relayServersTable) DeleteRelayServers(ctx context.Context, serverName spec.ServerName, relayServers []spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteSQL, serverName, pq.Array(relayServers)).Error
}

// DeleteAllRelayServers deletes all relay servers for a given server name
func (t *relayServersTable) DeleteAllRelayServers(ctx context.Context, serverName spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteAllSQL, serverName).Error
}
