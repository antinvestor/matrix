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

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

const roomsSchema = `
CREATE SEQUENCE IF NOT EXISTS roomserver_room_nid_seq;
CREATE TABLE IF NOT EXISTS roomserver_rooms (
    -- Local numeric ID for the room.
    room_nid BIGINT PRIMARY KEY DEFAULT nextval('roomserver_room_nid_seq'),
    -- Textual ID for the room.
    room_id TEXT NOT NULL CONSTRAINT roomserver_room_id_unique UNIQUE,
    -- The most recent events in the room that aren't referenced by another event.
    -- This list may empty if the server hasn't joined the room yet.
    -- (The server will be in that state while it stores the events for the initial state of the room)
    latest_event_nids BIGINT[] NOT NULL DEFAULT '{}'::BIGINT[],
    -- The last event written to the output log for this room.
    last_event_sent_nid BIGINT NOT NULL DEFAULT 0,
    -- The state of the room after the current set of latest events.
    -- This will be 0 if there are no latest events in the room.
    state_snapshot_nid BIGINT NOT NULL DEFAULT 0,
    -- The version of the room, which will assist in determining the state resolution
    -- algorithm, event ID format, etc.
    room_version TEXT NOT NULL
);
`

// SQL query constants for rooms operations
const (
	insertRoomNIDSQL = "" +
		"INSERT INTO roomserver_rooms (room_id, room_version) VALUES ($1, $2)" +
		" ON CONFLICT ON CONSTRAINT roomserver_room_id_unique" +
		" DO NOTHING RETURNING (room_nid)"

	selectRoomNIDSQL = "" +
		"SELECT room_nid FROM roomserver_rooms WHERE room_id = $1"

	selectRoomNIDForUpdateSQL = "" +
		"SELECT room_nid FROM roomserver_rooms WHERE room_id = $1 FOR UPDATE"

	selectLatestEventNIDsSQL = "" +
		"SELECT latest_event_nids, state_snapshot_nid FROM roomserver_rooms WHERE room_nid = $1"

	selectLatestEventNIDsForUpdateSQL = "" +
		"SELECT latest_event_nids, last_event_sent_nid, state_snapshot_nid FROM roomserver_rooms WHERE room_nid = $1 FOR UPDATE"

	updateLatestEventNIDsSQL = "" +
		"UPDATE roomserver_rooms SET latest_event_nids = $2, last_event_sent_nid = $3, state_snapshot_nid = $4 WHERE room_nid = $1"

	selectRoomVersionsForRoomNIDsSQL = "" +
		"SELECT room_nid, room_version FROM roomserver_rooms WHERE room_nid = ANY($1)"

	selectRoomInfoSQL = "" +
		"SELECT room_version, room_nid, state_snapshot_nid, latest_event_nids FROM roomserver_rooms WHERE room_id = $1"

	bulkSelectRoomIDsSQL = "" +
		"SELECT room_id FROM roomserver_rooms WHERE room_nid = ANY($1)"

	bulkSelectRoomNIDsSQL = "" +
		"SELECT room_nid FROM roomserver_rooms WHERE room_id = ANY($1)"
)

// roomsStatements implements tables.Rooms interface
type roomsStatements struct {
	cm *sqlutil.Connections

	// SQL statements stored as struct fields
	insertRoomNIDStmt                  string
	selectRoomNIDStmt                  string
	selectRoomNIDForUpdateStmt         string
	selectLatestEventNIDsStmt          string
	selectLatestEventNIDsForUpdateStmt string
	updateLatestEventNIDsStmt          string
	selectRoomVersionsForRoomNIDsStmt  string
	selectRoomInfoStmt                 string
	bulkSelectRoomIDsStmt              string
	bulkSelectRoomNIDsStmt             string
}

// NewPostgresRoomsTable creates a new PostgreSQL rooms table and prepares all statements
func NewPostgresRoomsTable(ctx context.Context, cm *sqlutil.Connections) (tables.Rooms, error) {
	// Create the table first
	if err := cm.Writer.ExecSQL(ctx, roomsSchema); err != nil {
		return nil, err
	}

	// Run any migrations if needed
	// (Currently no migrations for this table)

	// Initialize the table
	r := &roomsStatements{
		cm: cm,

		// Initialize SQL statement fields with the constants
		insertRoomNIDStmt:                  insertRoomNIDSQL,
		selectRoomNIDStmt:                  selectRoomNIDSQL,
		selectRoomNIDForUpdateStmt:         selectRoomNIDForUpdateSQL,
		selectLatestEventNIDsStmt:          selectLatestEventNIDsSQL,
		selectLatestEventNIDsForUpdateStmt: selectLatestEventNIDsForUpdateSQL,
		updateLatestEventNIDsStmt:          updateLatestEventNIDsSQL,
		selectRoomVersionsForRoomNIDsStmt:  selectRoomVersionsForRoomNIDsSQL,
		selectRoomInfoStmt:                 selectRoomInfoSQL,
		bulkSelectRoomIDsStmt:              bulkSelectRoomIDsSQL,
		bulkSelectRoomNIDsStmt:             bulkSelectRoomNIDsSQL,
	}

	return r, nil
}

// InsertRoomNID inserts a new room with the given ID and version, then returns a new room NID
func (s *roomsStatements) InsertRoomNID(
	ctx context.Context,
	roomID string, roomVersion gomatrixserverlib.RoomVersion,
) (types.RoomNID, error) {
	var roomNID int64

	// Get database connection
	db := s.cm.Connection(ctx, false)

	row := db.Raw(s.insertRoomNIDStmt, roomID, roomVersion).Row()
	err := row.Scan(&roomNID)

	return types.RoomNID(roomNID), err
}

// SelectRoomInfo retrieves room information for a given room ID
func (s *roomsStatements) SelectRoomInfo(ctx context.Context, roomID string) (*types.RoomInfo, error) {
	var info types.RoomInfo
	var latestNIDs pq.Int64Array
	var stateSnapshotNID types.StateSnapshotNID

	// Get database connection
	db := s.cm.Connection(ctx, true)

	row := db.Raw(s.selectRoomInfoStmt, roomID).Row()
	err := row.Scan(&info.RoomVersion, &info.RoomNID, &stateSnapshotNID, &latestNIDs)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}

	info.SetStateSnapshotNID(stateSnapshotNID)
	info.SetIsStub(len(latestNIDs) == 0)
	return &info, err
}

// SelectRoomNID retrieves the numeric ID for a room given its string ID
func (s *roomsStatements) SelectRoomNID(
	ctx context.Context, roomID string,
) (types.RoomNID, error) {
	var roomNID int64

	// Get database connection
	db := s.cm.Connection(ctx, true)

	row := db.Raw(s.selectRoomNIDStmt, roomID).Row()
	err := row.Scan(&roomNID)
	return types.RoomNID(roomNID), err
}

// SelectRoomNIDForUpdate retrieves the numeric ID for a room with FOR UPDATE clause
func (s *roomsStatements) SelectRoomNIDForUpdate(
	ctx context.Context, roomID string,
) (types.RoomNID, error) {
	var roomNID int64

	// Get database connection
	db := s.cm.Connection(ctx, false)

	row := db.Raw(s.selectRoomNIDForUpdateStmt, roomID).Row()
	err := row.Scan(&roomNID)
	return types.RoomNID(roomNID), err
}

// SelectLatestEventNIDs retrieves the latest event NIDs and state snapshot NID for a room
func (s *roomsStatements) SelectLatestEventNIDs(
	ctx context.Context, roomNID types.RoomNID,
) ([]types.EventNID, types.StateSnapshotNID, error) {
	var eventNIDs []types.EventNID
	var stateSnapshotNID types.StateSnapshotNID
	var latestNIDs pq.Int64Array

	// Get database connection
	db := s.cm.Connection(ctx, true)

	row := db.Raw(s.selectLatestEventNIDsStmt, int64(roomNID)).Row()
	err := row.Scan(&latestNIDs, &stateSnapshotNID)
	if err != nil {
		return nil, 0, err
	}

	eventNIDs = make([]types.EventNID, len(latestNIDs))
	for i := range latestNIDs {
		eventNIDs[i] = types.EventNID(latestNIDs[i])
	}

	return eventNIDs, stateSnapshotNID, nil
}

// SelectLatestEventsNIDsForUpdate retrieves the latest event NIDs, last event sent NID, and state snapshot NID with FOR UPDATE clause
func (s *roomsStatements) SelectLatestEventsNIDsForUpdate(
	ctx context.Context, roomNID types.RoomNID,
) ([]types.EventNID, types.EventNID, types.StateSnapshotNID, error) {
	var eventNIDs []types.EventNID
	var lastEventSentNID types.EventNID
	var stateSnapshotNID types.StateSnapshotNID
	var latestNIDs pq.Int64Array

	// Get database connection
	db := s.cm.Connection(ctx, false)

	row := db.Raw(s.selectLatestEventNIDsForUpdateStmt, int64(roomNID)).Row()
	err := row.Scan(&latestNIDs, &lastEventSentNID, &stateSnapshotNID)
	if err != nil {
		return nil, 0, 0, err
	}

	eventNIDs = make([]types.EventNID, len(latestNIDs))
	for i := range latestNIDs {
		eventNIDs[i] = types.EventNID(latestNIDs[i])
	}

	return eventNIDs, lastEventSentNID, stateSnapshotNID, nil
}

// UpdateLatestEventNIDs updates the latest event NIDs, last event sent NID, and state snapshot NID for a room
func (s *roomsStatements) UpdateLatestEventNIDs(
	ctx context.Context,
	txn *sql.Tx,
	roomNID types.RoomNID,
	eventNIDs []types.EventNID,
	lastEventSentNID types.EventNID,
	stateSnapshotNID types.StateSnapshotNID,
) error {
	// Get database connection
	db := s.cm.Connection(ctx, false)

	// Convert event NIDs to int64 array
	nids := make([]int64, len(eventNIDs))
	for i := range eventNIDs {
		nids[i] = int64(eventNIDs[i])
	}

	return db.Exec(
		s.updateLatestEventNIDsStmt,
		int64(roomNID),
		pq.Array(nids),
		int64(lastEventSentNID),
		int64(stateSnapshotNID),
	).Error
}

// SelectRoomVersionsForRoomNIDs retrieves the room versions for a list of room NIDs
func (s *roomsStatements) SelectRoomVersionsForRoomNIDs(
	ctx context.Context, roomNIDs []types.RoomNID,
) (map[types.RoomNID]gomatrixserverlib.RoomVersion, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)

	// Convert room NIDs to int64 array
	nids := roomNIDsAsArray(roomNIDs)

	rows, err := db.Raw(s.selectRoomVersionsForRoomNIDsStmt, pq.Array(nids)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectRoomVersionsForRoomNIDs: rows.close() failed")

	result := make(map[types.RoomNID]gomatrixserverlib.RoomVersion)
	var roomNID types.RoomNID
	var roomVersion gomatrixserverlib.RoomVersion
	for rows.Next() {
		if err = rows.Scan(&roomNID, &roomVersion); err != nil {
			return nil, err
		}
		result[roomNID] = roomVersion
	}

	return result, rows.Err()
}

// BulkSelectRoomIDs looks up room IDs for a list of room NIDs
func (s *roomsStatements) BulkSelectRoomIDs(ctx context.Context, roomNIDs []types.RoomNID) ([]string, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)

	// Convert room NIDs to int64 array
	nids := roomNIDsAsArray(roomNIDs)

	rows, err := db.Raw(s.bulkSelectRoomIDsStmt, pq.Array(nids)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "BulkSelectRoomIDs: rows.close() failed")

	var roomIDs []string
	var roomID string
	for rows.Next() {
		if err = rows.Scan(&roomID); err != nil {
			return nil, err
		}
		roomIDs = append(roomIDs, roomID)
	}

	return roomIDs, rows.Err()
}

// BulkSelectRoomNIDs looks up room NIDs for a list of room IDs
func (s *roomsStatements) BulkSelectRoomNIDs(ctx context.Context, roomIDs []string) ([]types.RoomNID, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(s.bulkSelectRoomNIDsStmt, pq.StringArray(roomIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "BulkSelectRoomNIDs: rows.close() failed")

	var roomNIDs []types.RoomNID
	var roomNID int64
	for rows.Next() {
		if err = rows.Scan(&roomNID); err != nil {
			return nil, err
		}
		roomNIDs = append(roomNIDs, types.RoomNID(roomNID))
	}

	return roomNIDs, rows.Err()
}

// Helper function to convert slice of room NIDs to int64 array
func roomNIDsAsArray(roomNIDs []types.RoomNID) []int64 {
	nids := make([]int64, len(roomNIDs))
	for i := range roomNIDs {
		nids[i] = int64(roomNIDs[i])
	}
	return nids
}
