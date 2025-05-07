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
	"errors"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"gorm.io/gorm"
)

const assumedOfflineSchema = `
CREATE TABLE IF NOT EXISTS federationsender_assumed_offline(
    -- The assumed offline server name
	server_name TEXT PRIMARY KEY NOT NULL
);
`

// SQL query string constants
const (
	// insertAssumedOfflineSQL inserts a server name into the assumed offline list with conflict handling
	insertAssumedOfflineSQL = "" +
		"INSERT INTO federationsender_assumed_offline (server_name) VALUES ($1)" +
		" ON CONFLICT DO NOTHING"

	// selectAssumedOfflineSQL checks if a server is in the assumed offline list
	selectAssumedOfflineSQL = "" +
		"SELECT server_name FROM federationsender_assumed_offline WHERE server_name = $1"

	// deleteAssumedOfflineSQL removes a server from the assumed offline list
	deleteAssumedOfflineSQL = "" +
		"DELETE FROM federationsender_assumed_offline WHERE server_name = $1"

	// deleteAllAssumedOfflineSQL removes all servers from the assumed offline list
	deleteAllAssumedOfflineSQL = "" +
		"TRUNCATE federationsender_assumed_offline"
)

// assumedOfflineTable contains the postgres-specific implementation
type assumedOfflineTable struct {
	cm *sqlutil.Connections

	insertAssumedOfflineStmt    string
	selectAssumedOfflineStmt    string
	deleteAssumedOfflineStmt    string
	deleteAllAssumedOfflineStmt string
}

// NewPostgresAssumedOfflineTable creates a new postgres assumed offline table
func NewPostgresAssumedOfflineTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationAssumedOffline, error) {
	// Initialize schema using GORM
	gormDB := cm.Connection(ctx, false)
	if err := gormDB.Exec(assumedOfflineSchema).Error; err != nil {
		return nil, err
	}

	s := &assumedOfflineTable{
		cm: cm,
		insertAssumedOfflineStmt:    insertAssumedOfflineSQL,
		selectAssumedOfflineStmt:    selectAssumedOfflineSQL,
		deleteAssumedOfflineStmt:    deleteAssumedOfflineSQL,
		deleteAllAssumedOfflineStmt: deleteAllAssumedOfflineSQL,
	}

	return s, nil
}

// InsertAssumedOffline adds a server to the assumed offline list
func (s *assumedOfflineTable) InsertAssumedOffline(
	ctx context.Context, serverName spec.ServerName,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(s.insertAssumedOfflineStmt, serverName).Error
}

// SelectAssumedOffline checks if a server is in the assumed offline list
func (s *assumedOfflineTable) SelectAssumedOffline(
	ctx context.Context, serverName spec.ServerName,
) (bool, error) {
	// Get read-only database connection
	db := s.cm.Connection(ctx, true)

	var result string
	err := db.Raw(s.selectAssumedOfflineStmt, serverName).Scan(&result).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// If we found a record, the server is assumed offline
	return result != "", nil
}

// DeleteAssumedOffline removes a server from the assumed offline list
func (s *assumedOfflineTable) DeleteAssumedOffline(
	ctx context.Context, serverName spec.ServerName,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(s.deleteAssumedOfflineStmt, serverName).Error
}

// DeleteAllAssumedOffline removes all servers from the assumed offline list
func (s *assumedOfflineTable) DeleteAllAssumedOffline(
	ctx context.Context,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(s.deleteAllAssumedOfflineStmt).Error
}
