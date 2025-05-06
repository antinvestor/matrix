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
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal/sqlutil"

	"github.com/antinvestor/gomatrixserverlib/spec"
)

const assumedOfflineSchema = `
CREATE TABLE IF NOT EXISTS federationsender_assumed_offline(
    -- The assumed offline server name
	server_name TEXT PRIMARY KEY NOT NULL
);
`

const assumedOfflineSchemaRevert = `DROP TABLE IF EXISTS federationsender_assumed_offline;`

// SQL queries for assumed offline operations
const (
	// Insert a server name into the assumed offline table
	insertAssumedOfflineSQL = "INSERT INTO federationsender_assumed_offline (server_name) VALUES ($1) ON CONFLICT DO NOTHING"

	// Select a server name from the assumed offline table
	selectAssumedOfflineSQL = "SELECT server_name FROM federationsender_assumed_offline WHERE server_name = $1"

	// Delete a server name from the assumed offline table
	deleteAssumedOfflineSQL = "DELETE FROM federationsender_assumed_offline WHERE server_name = $1"

	// Truncate the assumed offline table
	deleteAllAssumedOfflineSQL = "TRUNCATE federationsender_assumed_offline"
)

// assumedOfflineTable provides methods for assumed offline operations using GORM.
type assumedOfflineTable struct {
	cm           *sqlutil.Connections // Connection manager for database access
	InsertSQL    string
	SelectSQL    string
	DeleteSQL    string
	DeleteAllSQL string
}

// NewPostgresAssumedOfflineTable initializes an assumedOfflineTable with SQL constants and a connection manager
func NewPostgresAssumedOfflineTable(cm *sqlutil.Connections) tables.FederationAssumedOffline {
	return &assumedOfflineTable{
		cm:           cm,
		InsertSQL:    insertAssumedOfflineSQL,
		SelectSQL:    selectAssumedOfflineSQL,
		DeleteSQL:    deleteAssumedOfflineSQL,
		DeleteAllSQL: deleteAllAssumedOfflineSQL,
	}
}

// InsertAssumedOffline inserts a server name into the assumed offline table
func (t *assumedOfflineTable) InsertAssumedOffline(ctx context.Context, serverName spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.InsertSQL, serverName).Error
}

// SelectAssumedOffline checks if a server name is in the assumed offline table
func (t *assumedOfflineTable) SelectAssumedOffline(ctx context.Context, serverName spec.ServerName) (bool, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.SelectSQL, serverName).Row()
	var name string
	err := row.Scan(&name)
	if err != nil {
		return false, nil // Not found
	}
	return true, nil
}

// DeleteAssumedOffline deletes a server name from the assumed offline table
func (t *assumedOfflineTable) DeleteAssumedOffline(ctx context.Context, serverName spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteSQL, serverName).Error
}

// DeleteAllAssumedOffline truncates the assumed offline table
func (t *assumedOfflineTable) DeleteAllAssumedOffline(ctx context.Context) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteAllSQL).Error
}
