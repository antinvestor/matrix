// Copyright 2020 The Matrix.org Foundation C.I.C.
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

// Schema for the blacklist table
const blacklistSchema = `
CREATE TABLE IF NOT EXISTS federationsender_blacklist (
    -- The blacklisted server name
	server_name TEXT NOT NULL,
	UNIQUE (server_name)
);
`

// Schema revert for the blacklist table
const blacklistSchemaRevert = `
DROP TABLE IF EXISTS federationsender_blacklist;
`

// SQL for inserting a server into the blacklist
const insertBlacklistSQL = "" +
	"INSERT INTO federationsender_blacklist (server_name) VALUES ($1)" +
	" ON CONFLICT DO NOTHING"

// SQL for checking if a server is in the blacklist
const selectBlacklistSQL = "" +
	"SELECT server_name FROM federationsender_blacklist WHERE server_name = $1"

// SQL for removing a server from the blacklist
const deleteBlacklistSQL = "" +
	"DELETE FROM federationsender_blacklist WHERE server_name = $1"

// SQL for removing all servers from the blacklist
const deleteAllBlacklistSQL = "" +
	"TRUNCATE federationsender_blacklist"

// blacklistTable stores the list of blacklisted servers
type blacklistTable struct {
	cm *sqlutil.Connections
	// SQL query string fields, initialized at construction
	insertBlacklistSQL    string
	selectBlacklistSQL    string
	deleteBlacklistSQL    string
	deleteAllBlacklistSQL string
}

// NewPostgresBlacklistTable creates a new postgres blacklist table
func NewPostgresBlacklistTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationBlacklist, error) {
	s := &blacklistTable{
		cm:                    cm,
		insertBlacklistSQL:    insertBlacklistSQL,
		selectBlacklistSQL:    selectBlacklistSQL,
		deleteBlacklistSQL:    deleteBlacklistSQL,
		deleteAllBlacklistSQL: deleteAllBlacklistSQL,
	}

	// Perform schema migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "federationapi_blacklist_table_schema_001",
		Patch:       blacklistSchema,
		RevertPatch: blacklistSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertBlacklist adds a server to the blacklist
func (s *blacklistTable) InsertBlacklist(
	ctx context.Context, serverName spec.ServerName,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.insertBlacklistSQL, serverName).Error
}

// SelectBlacklist checks if a server is in the blacklist
func (s *blacklistTable) SelectBlacklist(
	ctx context.Context, serverName spec.ServerName,
) (bool, error) {
	db := s.cm.Connection(ctx, true)
	var result spec.ServerName
	row := db.Raw(s.selectBlacklistSQL, serverName).Row()
	err := row.Scan(&result)
	if err != nil {
		// If the error is sql.ErrNoRows, that means the server is not blacklisted
		if frame.DBErrorIsRecordNotFound(err) {
			return false, nil
		}
		return false, err
	}
	// The query will return the server name if the server is blacklisted, and
	// will return no rows if not. By calling Next, we find out if a row was
	// returned or not - we don't care about the value itself.
	return true, nil
}

// DeleteBlacklist removes a server from the blacklist
func (s *blacklistTable) DeleteBlacklist(
	ctx context.Context, serverName spec.ServerName,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteBlacklistSQL, serverName).Error
}

// DeleteAllBlacklist removes all servers from the blacklist
func (s *blacklistTable) DeleteAllBlacklist(
	ctx context.Context,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteAllBlacklistSQL).Error
}
