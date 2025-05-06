// Copyright 2020 The Global.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/internal/sqlutil"
)

const blacklistSchema = `
CREATE TABLE IF NOT EXISTS federationsender_blacklist (
    -- The blacklisted server name
	server_name TEXT NOT NULL,
	UNIQUE (server_name)
);
`

const blacklistSchemaRevert = `DROP TABLE IF EXISTS federationsender_blacklist;`

// SQL queries for blacklist operations
const (
	// Insert a server name into the blacklist table
	insertBlacklistSQL = "INSERT INTO federationsender_blacklist (server_name) VALUES ($1) ON CONFLICT DO NOTHING"

	// Select a server name from the blacklist table
	selectBlacklistSQL = "SELECT server_name FROM federationsender_blacklist WHERE server_name = $1"

	// Delete a server name from the blacklist table
	deleteBlacklistSQL = "DELETE FROM federationsender_blacklist WHERE server_name = $1"

	// Truncate the blacklist table
	deleteAllBlacklistSQL = "TRUNCATE federationsender_blacklist"
)

// blacklistTable provides methods for blacklist operations using GORM.
type blacklistTable struct {
	cm           *sqlutil.Connections // Connection manager for database access
	InsertSQL    string
	SelectSQL    string
	DeleteSQL    string
	DeleteAllSQL string
}

// NewPostgresBlacklistTable initializes a blacklistTable with SQL constants and a connection manager
func NewPostgresBlacklistTable(cm *sqlutil.Connections) tables.FederationBlacklist {
	return &blacklistTable{
		cm:           cm,
		InsertSQL:    insertBlacklistSQL,
		DeleteSQL:    deleteBlacklistSQL,
		SelectSQL:    selectBlacklistSQL,
		DeleteAllSQL: deleteAllBlacklistSQL,
	}
}

// InsertBlacklist inserts a server name into the blacklist table
func (t *blacklistTable) InsertBlacklist(ctx context.Context, serverName spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.InsertSQL, serverName).Error
}

// SelectBlacklist checks if a server name is in the blacklist table
func (t *blacklistTable) SelectBlacklist(ctx context.Context, serverName spec.ServerName) (bool, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.SelectSQL, serverName).Row()
	var name string
	err := row.Scan(&name)
	if err != nil {
		return false, nil // Not found
	}
	return true, nil
}

// DeleteBlacklist deletes a server name from the blacklist table
func (t *blacklistTable) DeleteBlacklist(ctx context.Context, serverName spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteSQL, serverName).Error
}

// DeleteAllBlacklist truncates the blacklist table
func (t *blacklistTable) DeleteAllBlacklist(ctx context.Context) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteAllSQL).Error
}
