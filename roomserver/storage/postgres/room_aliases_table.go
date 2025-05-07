// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Matrix.org Foundation C.I.C.
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
	"errors"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
)

const roomAliasesSchema = `
-- Stores room aliases and room IDs they refer to
CREATE TABLE IF NOT EXISTS roomserver_room_aliases (
    -- Alias of the room
    alias TEXT NOT NULL PRIMARY KEY,
    -- Room ID the alias refers to
    room_id TEXT NOT NULL,
    -- User ID of the creator of this alias
    creator_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS roomserver_room_id_idx ON roomserver_room_aliases(room_id);
`

// SQL query constants for room aliases operations
const (
	// insertRoomAliasSQL inserts a new room alias
	insertRoomAliasSQL = "INSERT INTO roomserver_room_aliases (alias, room_id, creator_id) VALUES ($1, $2, $3)"
	
	// selectRoomIDFromAliasSQL gets the room ID for a given alias
	selectRoomIDFromAliasSQL = "SELECT room_id FROM roomserver_room_aliases WHERE alias = $1"
	
	// selectAliasesFromRoomIDSQL gets all aliases for a given room ID
	selectAliasesFromRoomIDSQL = "SELECT alias FROM roomserver_room_aliases WHERE room_id = $1"
	
	// selectCreatorIDFromAliasSQL gets the creator ID for a given alias
	selectCreatorIDFromAliasSQL = "SELECT creator_id FROM roomserver_room_aliases WHERE alias = $1"
	
	// deleteRoomAliasSQL removes a room alias
	deleteRoomAliasSQL = "DELETE FROM roomserver_room_aliases WHERE alias = $1"
)

// roomAliasesStatements implements tables.RoomAliases interface
type roomAliasesStatements struct {
	cm *sqlutil.Connections
	
	// SQL statements stored as struct fields
	insertRoomAliasStmt         string
	selectRoomIDFromAliasStmt   string
	selectAliasesFromRoomIDStmt string
	selectCreatorIDFromAliasStmt string
	deleteRoomAliasStmt         string
}

// NewPostgresRoomAliasesTable creates a new PostgreSQL room aliases table and prepares all statements
func NewPostgresRoomAliasesTable(ctx context.Context, cm *sqlutil.Connections) (tables.RoomAliases, error) {
	// Create the table first
	if err := cm.Writer.ExecSQL(ctx, roomAliasesSchema); err != nil {
		return nil, err
	}
	
	// Initialize the table
	s := &roomAliasesStatements{
		cm: cm,
		
		// Initialize SQL statement fields with the constants
		insertRoomAliasStmt:         insertRoomAliasSQL,
		selectRoomIDFromAliasStmt:   selectRoomIDFromAliasSQL,
		selectAliasesFromRoomIDStmt: selectAliasesFromRoomIDSQL,
		selectCreatorIDFromAliasStmt: selectCreatorIDFromAliasSQL,
		deleteRoomAliasStmt:         deleteRoomAliasSQL,
	}
	
	return s, nil
}

// InsertRoomAlias inserts a new room alias into the database
func (s *roomAliasesStatements) InsertRoomAlias(
	ctx context.Context, txn *sql.Tx,
	alias string, roomID string, creatorUserID string,
) (err error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	if txn != nil {
		db = db.WithContext(ctx)
	}
	
	return db.Exec(s.insertRoomAliasStmt, alias, roomID, creatorUserID).Error
}

// SelectRoomIDFromAlias looks up the room ID associated with a particular alias
func (s *roomAliasesStatements) SelectRoomIDFromAlias(
	ctx context.Context, txn *sql.Tx,
	alias string,
) (roomID string, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	if txn != nil {
		db = db.WithContext(ctx)
	}
	
	row := db.Raw(s.selectRoomIDFromAliasStmt, alias).Row()
	err = row.Scan(&roomID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return
}

// SelectAliasesFromRoomID looks up the aliases associated with a particular room ID
func (s *roomAliasesStatements) SelectAliasesFromRoomID(
	ctx context.Context, txn *sql.Tx,
	roomID string,
) ([]string, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	if txn != nil {
		db = db.WithContext(ctx)
	}
	
	rows, err := db.Raw(s.selectAliasesFromRoomIDStmt, roomID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectAliasesFromRoomID: rows.close() failed")
	
	var aliases []string
	for rows.Next() {
		var alias string
		if err = rows.Scan(&alias); err != nil {
			return nil, err
		}
		aliases = append(aliases, alias)
	}
	
	return aliases, rows.Err()
}

// SelectCreatorIDFromAlias looks up the creator ID associated with a particular alias
func (s *roomAliasesStatements) SelectCreatorIDFromAlias(
	ctx context.Context, txn *sql.Tx,
	alias string,
) (creatorID string, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	if txn != nil {
		db = db.WithContext(ctx)
	}
	
	row := db.Raw(s.selectCreatorIDFromAliasStmt, alias).Row()
	err = row.Scan(&creatorID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return
}

// DeleteRoomAlias removes a room alias
func (s *roomAliasesStatements) DeleteRoomAlias(
	ctx context.Context, txn *sql.Tx,
	alias string,
) (err error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	if txn != nil {
		db = db.WithContext(ctx)
	}
	
	return db.Exec(s.deleteRoomAliasStmt, alias).Error
}
