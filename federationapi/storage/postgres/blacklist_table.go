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
	"gorm.io/gorm"
)

const blacklistSchema = `
CREATE TABLE IF NOT EXISTS federationsender_blacklist (
    -- The blacklisted server name
	server_name TEXT NOT NULL,
	UNIQUE (server_name)
);
`

// blacklistTable contains the postgres-specific implementation of the blacklist table
type blacklistTable struct {
	cm *sqlutil.Connections
	
	// SQL queries stored as fields for better maintainability
	// insertBlacklistSQL adds a server name to the blacklist
	insertBlacklistSQL string
	
	// selectBlacklistSQL checks if a server name is blacklisted
	selectBlacklistSQL string
	
	// deleteBlacklistSQL removes a server name from the blacklist
	deleteBlacklistSQL string
	
	// deleteAllBlacklistSQL removes all server names from the blacklist
	deleteAllBlacklistSQL string
}

// NewPostgresBlacklistTable creates a new postgres blacklist table and prepares all statements
func NewPostgresBlacklistTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationBlacklist, error) {
	// Initialize schema using GORM
	gormDB := cm.Connection(ctx, false)
	if err := gormDB.Exec(blacklistSchema).Error; err != nil {
		return nil, err
	}
	
	s := &blacklistTable{
		cm: cm,
		insertBlacklistSQL: "INSERT INTO federationsender_blacklist (server_name) VALUES ($1) ON CONFLICT DO NOTHING",
		selectBlacklistSQL: "SELECT server_name FROM federationsender_blacklist WHERE server_name = $1",
		deleteBlacklistSQL: "DELETE FROM federationsender_blacklist WHERE server_name = $1",
		deleteAllBlacklistSQL: "TRUNCATE federationsender_blacklist",
	}

	return s, nil
}

// InsertBlacklist adds a server name to the blacklist
func (s *blacklistTable) InsertBlacklist(
	ctx context.Context, serverName spec.ServerName,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.insertBlacklistSQL, serverName).Error
}

// SelectBlacklist checks if a server name is on the blacklist
func (s *blacklistTable) SelectBlacklist(
	ctx context.Context, serverName spec.ServerName,
) (bool, error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectBlacklistSQL, serverName).Rows()
	if err != nil {
		return false, err
	}
	defer rows.Close() // nolint:errcheck
	
	// The query will return the server name if the server is blacklisted, and
	// will return no rows if not. By calling Next, we find out if a row was
	// returned or not - we don't care about the value itself.
	return rows.Next(), nil
}

// DeleteBlacklist removes a server name from the blacklist
func (s *blacklistTable) DeleteBlacklist(
	ctx context.Context, serverName spec.ServerName,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteBlacklistSQL, serverName).Error
}

// DeleteAllBlacklist removes all server names from the blacklist
func (s *blacklistTable) DeleteAllBlacklist(
	ctx context.Context,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteAllBlacklistSQL).Error
}
