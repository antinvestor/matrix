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
	"fmt"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
)

const stateSnapshotSchema = `
-- A state snapshot contains the state of a room at a given state block.
-- For the extremity of a room the state snapshot also contains a list of
-- event NIDs that can be used to reconstruct the current state from
-- the state at the given state block.
CREATE TABLE IF NOT EXISTS roomserver_state_snapshots (
    -- Local numeric ID for the state.
    state_snapshot_nid BIGINT PRIMARY KEY,
    -- Local numeric ID of the room this state is for.
    room_nid BIGINT NOT NULL,
    -- Local numeric ID for the state block for this state.
    state_block_nid BIGINT NOT NULL DEFAULT 0
);

CREATE SEQUENCE IF NOT EXISTS roomserver_state_snapshot_nid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 100
;
`

// SQL query constants for state snapshot operations
const (
	// insertStateSQL inserts a new state snapshot and returns a state snapshot NID
	insertStateSQL = "" +
		"INSERT INTO roomserver_state_snapshots (state_snapshot_nid, room_nid, state_block_nid)" +
		" VALUES ($1, $2, $3)" +
		" ON CONFLICT DO NOTHING"

	// selectNextStateSnapshotNIDSQL selects the next state snapshot NID from the sequence
	selectNextStateSnapshotNIDSQL = "SELECT nextval('roomserver_state_snapshot_nid_seq')"

	// selectStateSnapshotSQL selects a state snapshot by NID
	selectStateSnapshotSQL = "" +
		"SELECT room_nid, state_block_nid FROM roomserver_state_snapshots" +
		" WHERE state_snapshot_nid = $1"

	// bulkSelectStateBlockNIDsSQL selects the state block NIDs for a list of state snapshot NIDs
	bulkSelectStateBlockNIDsSQL = "" +
		"SELECT state_snapshot_nid, state_block_nid FROM roomserver_state_snapshots" +
		" WHERE state_snapshot_nid = ANY($1)"

	// bulkSelectStateBlockNIDsForRoomSQL selects all state snapshots for a room
	bulkSelectStateBlockNIDsForRoomSQL = "" +
		"SELECT state_snapshot_nid, state_block_nid FROM roomserver_state_snapshots" +
		" WHERE room_nid = $1"
)

type stateSnapshotStatements struct {
	cm *sqlutil.Connections
	
	// SQL statements stored as struct fields
	insertStateStmt           string
	selectNextStateSnapshotNIDStmt  string
	selectStateSnapshotStmt   string
	bulkSelectStateBlockNIDsStmt string
	bulkSelectStateBlockNIDsForRoomStmt string
}

// NewPostgresStateSnapshotsTable creates a new PostgreSQL state snapshots table
func NewPostgresStateSnapshotsTable(ctx context.Context, cm *sqlutil.Connections) (tables.StateSnapshot, error) {
	// Create the table first
	if err := cm.Writer.ExecSQL(ctx, stateSnapshotSchema); err != nil {
		return nil, err
	}

	// Initialize the struct
	s := &stateSnapshotStatements{
		cm: cm,
		
		// Initialize SQL statement fields with the constants
		insertStateStmt:           insertStateSQL,
		selectNextStateSnapshotNIDStmt:  selectNextStateSnapshotNIDSQL,
		selectStateSnapshotStmt:   selectStateSnapshotSQL,
		bulkSelectStateBlockNIDsStmt: bulkSelectStateBlockNIDsSQL,
		bulkSelectStateBlockNIDsForRoomStmt: bulkSelectStateBlockNIDsForRoomSQL,
	}

	return s, nil
}

// InsertState implements tables.StateSnapshot
func (s *stateSnapshotStatements) InsertState(
	ctx context.Context,
	txn *sql.Tx,
	roomNID types.RoomNID,
	stateBlockNID types.StateBlockNID,
) (stateNID types.StateSnapshotNID, err error) {
	// Get the next state NID from the sequence
	var nextStateNID types.StateSnapshotNID
	if err = s.selectNextStateSnapshotNID(ctx, txn, &nextStateNID); err != nil {
		return 0, err
	}

	// Insert the new state snapshot
	if err = s.insertState(ctx, txn, nextStateNID, roomNID, stateBlockNID); err != nil {
		return 0, err
	}

	return nextStateNID, nil
}

func (s *stateSnapshotStatements) selectNextStateSnapshotNID(
	ctx context.Context, txn *sql.Tx, stateNID *types.StateSnapshotNID,
) error {
	// Get database connection
	var db *sql.Conn
	if txn != nil {
		// Use existing transaction.
		return txn.QueryRowContext(ctx, s.selectNextStateSnapshotNIDStmt).Scan(stateNID)
	} else {
		// Acquire a new connection, since we're not using a transaction.
		var err error
		if db, err = s.cm.Writer.GetSQLConn(ctx); err != nil {
			return err
		}
		defer internal.CloseAndLogIfError(ctx, db, "selectNextStateSnapshotNID: failed to close connection")
		return db.QueryRowContext(ctx, s.selectNextStateSnapshotNIDStmt).Scan(stateNID)
	}
}

func (s *stateSnapshotStatements) insertState(
	ctx context.Context,
	txn *sql.Tx,
	stateNID types.StateSnapshotNID,
	roomNID types.RoomNID,
	stateBlockNID types.StateBlockNID,
) error {
	// Get database connection
	var db *sql.Conn
	var stmt *sql.Stmt
	var err error
	
	if txn != nil {
		// Use existing transaction.
		if stmt, err = txn.PrepareContext(ctx, s.insertStateStmt); err != nil {
			return err
		}
		defer internal.CloseAndLogIfError(ctx, stmt, "insertState: failed to close statement")
		
		_, err = stmt.ExecContext(ctx, stateNID, roomNID, stateBlockNID)
		return err
	} else {
		// Acquire a new connection, since we're not using a transaction.
		if db, err = s.cm.Writer.GetSQLConn(ctx); err != nil {
			return err
		}
		defer internal.CloseAndLogIfError(ctx, db, "insertState: failed to close connection")
		
		if stmt, err = db.PrepareContext(ctx, s.insertStateStmt); err != nil {
			return err
		}
		defer internal.CloseAndLogIfError(ctx, stmt, "insertState: failed to close statement")
		
		_, err = stmt.ExecContext(ctx, stateNID, roomNID, stateBlockNID)
		return err
	}
}

// GetState implements tables.StateSnapshot
func (s *stateSnapshotStatements) GetState(
	ctx context.Context,
	stateNID types.StateSnapshotNID,
) (roomNID types.RoomNID, stateBlockNID types.StateBlockNID, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	row := db.Raw(s.selectStateSnapshotStmt, stateNID).Row()
	err = row.Scan(&roomNID, &stateBlockNID)
	if err == sql.ErrNoRows {
		return 0, 0, fmt.Errorf("state %d not found", stateNID)
	}
	return
}

// BulkGetStateBlockNIDs implements tables.StateSnapshot
func (s *stateSnapshotStatements) BulkGetStateBlockNIDs(
	ctx context.Context,
	stateNIDs []types.StateSnapshotNID,
) (map[types.StateSnapshotNID]types.StateBlockNID, error) {
	if len(stateNIDs) == 0 {
		return nil, nil
	}
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Convert state NIDs to int64 for SQL parameter
	sqlStateNIDs := make([]int64, len(stateNIDs))
	for i := range stateNIDs {
		sqlStateNIDs[i] = int64(stateNIDs[i])
	}
	
	// Execute the query
	rows, err := db.Raw(s.bulkSelectStateBlockNIDsStmt, pq.Array(sqlStateNIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "BulkGetStateBlockNIDs: failed to close rows")
	
	// Create result map
	results := make(map[types.StateSnapshotNID]types.StateBlockNID, len(stateNIDs))
	var stateNID types.StateSnapshotNID
	var stateBlockNID types.StateBlockNID
	
	// Process rows
	for rows.Next() {
		if err = rows.Scan(&stateNID, &stateBlockNID); err != nil {
			return nil, err
		}
		results[stateNID] = stateBlockNID
	}
	
	return results, rows.Err()
}

// BulkGetStateBlockNIDsForRoom implements tables.StateSnapshot
func (s *stateSnapshotStatements) BulkGetStateBlockNIDsForRoom(
	ctx context.Context,
	roomNID types.RoomNID,
) ([]types.StateSnapshotNID, []types.StateBlockNID, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute the query
	rows, err := db.Raw(s.bulkSelectStateBlockNIDsForRoomStmt, roomNID).Rows()
	if err != nil {
		return nil, nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "BulkGetStateBlockNIDsForRoom: failed to close rows")
	
	// Create result slices
	var results []types.StateSnapshotNID
	var blocks []types.StateBlockNID
	var stateNID types.StateSnapshotNID
	var stateBlockNID types.StateBlockNID
	
	// Process rows
	for rows.Next() {
		if err = rows.Scan(&stateNID, &stateBlockNID); err != nil {
			return nil, nil, err
		}
		results = append(results, stateNID)
		blocks = append(blocks, stateBlockNID)
	}
	
	return results, blocks, rows.Err()
}
