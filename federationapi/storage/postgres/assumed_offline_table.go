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
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/pitabwire/frame"
)

// Schema for the assumed offline table
const assumedOfflineSchema = `
CREATE TABLE IF NOT EXISTS federationsender_assumed_offline(
    -- The assumed offline server name
	server_name TEXT PRIMARY KEY NOT NULL
);
`

// Schema revert for the assumed offline table
const assumedOfflineSchemaRevert = `
DROP TABLE IF EXISTS federationsender_assumed_offline;
`

// SQL for inserting a server into the assumed offline list
const insertAssumedOfflineSQL = "" +
	"INSERT INTO federationsender_assumed_offline (server_name) VALUES ($1)" +
	" ON CONFLICT DO NOTHING"

// SQL for checking if a server is in the assumed offline list
const selectAssumedOfflineSQL = "" +
	"SELECT server_name FROM federationsender_assumed_offline WHERE server_name = $1"

// SQL for removing a server from the assumed offline list
const deleteAssumedOfflineSQL = "" +
	"DELETE FROM federationsender_assumed_offline WHERE server_name = $1"

// SQL for removing all servers from the assumed offline list
const deleteAllAssumedOfflineSQL = "" +
	"TRUNCATE federationsender_assumed_offline"

// assumedOfflineTable stores the list of assumed offline servers
type assumedOfflineTable struct {
	cm *sqlutil.Connections
	// SQL query string fields, initialized at construction
	insertAssumedOfflineSQL    string
	selectAssumedOfflineSQL    string
	deleteAssumedOfflineSQL    string
	deleteAllAssumedOfflineSQL string
}

// NewPostgresAssumedOfflineTable creates a new postgres assumed offline table
func NewPostgresAssumedOfflineTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationAssumedOffline, error) {
	s := &assumedOfflineTable{
		cm:                         cm,
		insertAssumedOfflineSQL:    insertAssumedOfflineSQL,
		selectAssumedOfflineSQL:    selectAssumedOfflineSQL,
		deleteAssumedOfflineSQL:    deleteAssumedOfflineSQL,
		deleteAllAssumedOfflineSQL: deleteAllAssumedOfflineSQL,
	}

	// Perform schema migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "federationapi_assumed_offline_table_schema_001",
		Patch:       assumedOfflineSchema,
		RevertPatch: assumedOfflineSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertAssumedOffline adds a server to the assumed offline list
func (s *assumedOfflineTable) InsertAssumedOffline(
	ctx context.Context, serverName spec.ServerName,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.insertAssumedOfflineSQL, serverName).Error
}

// SelectAssumedOffline checks if a server is in the assumed offline list
func (s *assumedOfflineTable) SelectAssumedOffline(
	ctx context.Context, serverName spec.ServerName,
) (bool, error) {
	db := s.cm.Connection(ctx, true)
	var result spec.ServerName
	row := db.Raw(s.selectAssumedOfflineSQL, serverName).Row()
	err := row.Scan(&result)
	if err != nil {
		// If the error is sql.ErrNoRows, that means the server is not in the assumed offline list
		if frame.DBErrorIsRecordNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// DeleteAssumedOffline removes a server from the assumed offline list
func (s *assumedOfflineTable) DeleteAssumedOffline(
	ctx context.Context, serverName spec.ServerName,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteAssumedOfflineSQL, serverName).Error
}

// DeleteAllAssumedOffline removes all servers from the assumed offline list
func (s *assumedOfflineTable) DeleteAllAssumedOffline(
	ctx context.Context,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteAllAssumedOfflineSQL).Error
}
