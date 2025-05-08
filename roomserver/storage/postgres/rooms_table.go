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
	"github.com/pitabwire/frame"
)

// Schema for the rooms table
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

// Schema revert script for migration purposes
const roomsSchemaRevert = `DROP TABLE IF EXISTS roomserver_rooms;`

// SQL to insert a new room and return its numeric ID
const insertRoomNIDSQL = "" +
	"INSERT INTO roomserver_rooms (room_id, room_version) VALUES ($1, $2)" +
	" ON CONFLICT ON CONSTRAINT roomserver_room_id_unique" +
	" DO NOTHING RETURNING (room_nid)"

// SQL to select a numeric ID for a room
const selectRoomNIDSQL = "" +
	"SELECT room_nid FROM roomserver_rooms WHERE room_id = $1"

// SQL to select a numeric ID for a room with FOR UPDATE locking
const selectRoomNIDForUpdateSQL = "" +
	"SELECT room_nid FROM roomserver_rooms WHERE room_id = $1 FOR UPDATE"

// SQL to select the latest event NIDs for a room
const selectLatestEventNIDsSQL = "" +
	"SELECT latest_event_nids, state_snapshot_nid FROM roomserver_rooms WHERE room_nid = $1"

// SQL to select the latest event NIDs for a room with FOR UPDATE locking
const selectLatestEventNIDsForUpdateSQL = "" +
	"SELECT latest_event_nids, last_event_sent_nid, state_snapshot_nid FROM roomserver_rooms WHERE room_nid = $1 FOR UPDATE"

// SQL to update the latest event NIDs for a room
const updateLatestEventNIDsSQL = "" +
	"UPDATE roomserver_rooms SET latest_event_nids = $2, last_event_sent_nid = $3, state_snapshot_nid = $4 WHERE room_nid = $1"

// SQL to select room versions for a list of room NIDs
const selectRoomVersionsForRoomNIDsSQL = "" +
	"SELECT room_nid, room_version FROM roomserver_rooms WHERE room_nid = ANY($1)"

// SQL to select room information for a room ID
const selectRoomInfoSQL = "" +
	"SELECT room_version, room_nid, state_snapshot_nid, latest_event_nids FROM roomserver_rooms WHERE room_id = $1"

// SQL to bulk select room IDs for a list of room NIDs
const bulkSelectRoomIDsSQL = "" +
	"SELECT room_id FROM roomserver_rooms WHERE room_nid = ANY($1)"

// SQL to bulk select room NIDs for a list of room IDs
const bulkSelectRoomNIDsSQL = "" +
	"SELECT room_nid FROM roomserver_rooms WHERE room_id = ANY($1)"

// roomsTable implements the tables.Rooms interface using GORM
type roomsTable struct {
	cm *sqlutil.Connections

	// SQL query strings loaded from constants
	insertRoomNIDSQL                  string
	selectRoomNIDSQL                  string
	selectRoomNIDForUpdateSQL         string
	selectLatestEventNIDsSQL          string
	selectLatestEventNIDsForUpdateSQL string
	updateLatestEventNIDsSQL          string
	selectRoomVersionsForRoomNIDsSQL  string
	selectRoomInfoSQL                 string
	bulkSelectRoomIDsSQL              string
	bulkSelectRoomNIDsSQL             string
}

// NewPostgresRoomsTable creates a new rooms table
func NewPostgresRoomsTable(ctx context.Context, cm *sqlutil.Connections) (tables.Rooms, error) {
	// Create the table if it doesn't exist using migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "roomserver_rooms_table_schema_001",
		Patch:       roomsSchema,
		RevertPatch: roomsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Initialize the table struct with just the connection manager
	t := &roomsTable{
		cm: cm,

		// Initialize SQL query strings from constants
		insertRoomNIDSQL:                  insertRoomNIDSQL,
		selectRoomNIDSQL:                  selectRoomNIDSQL,
		selectRoomNIDForUpdateSQL:         selectRoomNIDForUpdateSQL,
		selectLatestEventNIDsSQL:          selectLatestEventNIDsSQL,
		selectLatestEventNIDsForUpdateSQL: selectLatestEventNIDsForUpdateSQL,
		updateLatestEventNIDsSQL:          updateLatestEventNIDsSQL,
		selectRoomVersionsForRoomNIDsSQL:  selectRoomVersionsForRoomNIDsSQL,
		selectRoomInfoSQL:                 selectRoomInfoSQL,
		bulkSelectRoomIDsSQL:              bulkSelectRoomIDsSQL,
		bulkSelectRoomNIDsSQL:             bulkSelectRoomNIDsSQL,
	}

	return t, nil
}

func (t *roomsTable) InsertRoomNID(
	ctx context.Context,
	roomID string, roomVersion gomatrixserverlib.RoomVersion,
) (types.RoomNID, error) {
	db := t.cm.Connection(ctx, false)

	var roomNID int64
	row := db.Raw(t.insertRoomNIDSQL, roomID, roomVersion).Row()
	err := row.Scan(&roomNID)
	return types.RoomNID(roomNID), err
}

func (t *roomsTable) SelectRoomInfo(ctx context.Context, roomID string) (*types.RoomInfo, error) {
	db := t.cm.Connection(ctx, true)

	var info types.RoomInfo
	var latestNIDs pq.Int64Array
	var stateSnapshotNID types.StateSnapshotNID
	row := db.Raw(t.selectRoomInfoSQL, roomID).Row()
	err := row.Scan(
		&info.RoomVersion, &info.RoomNID, &stateSnapshotNID, &latestNIDs,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	info.SetStateSnapshotNID(stateSnapshotNID)
	info.SetIsStub(len(latestNIDs) == 0)
	return &info, err
}

func (t *roomsTable) SelectRoomNID(
	ctx context.Context, roomID string,
) (types.RoomNID, error) {
	db := t.cm.Connection(ctx, true)

	var roomNID int64
	row := db.Raw(t.selectRoomNIDSQL, roomID).Row()
	err := row.Scan(&roomNID)
	return types.RoomNID(roomNID), err
}

func (t *roomsTable) SelectRoomNIDForUpdate(
	ctx context.Context, roomID string,
) (types.RoomNID, error) {
	db := t.cm.Connection(ctx, false)

	var roomNID int64
	row := db.Raw(t.selectRoomNIDForUpdateSQL, roomID).Row()
	err := row.Scan(&roomNID)
	return types.RoomNID(roomNID), err
}

func (t *roomsTable) SelectLatestEventNIDs(
	ctx context.Context, roomNID types.RoomNID,
) ([]types.EventNID, types.StateSnapshotNID, error) {
	db := t.cm.Connection(ctx, true)

	var nids pq.Int64Array
	var stateSnapshotNID int64
	row := db.Raw(t.selectLatestEventNIDsSQL, int64(roomNID)).Row()
	err := row.Scan(&nids, &stateSnapshotNID)
	if err != nil {
		return nil, 0, err
	}
	eventNIDs := make([]types.EventNID, len(nids))
	for i := range nids {
		eventNIDs[i] = types.EventNID(nids[i])
	}
	return eventNIDs, types.StateSnapshotNID(stateSnapshotNID), nil
}

func (t *roomsTable) SelectLatestEventsNIDsForUpdate(
	ctx context.Context, roomNID types.RoomNID,
) ([]types.EventNID, types.EventNID, types.StateSnapshotNID, error) {
	db := t.cm.Connection(ctx, false)

	var nids pq.Int64Array
	var lastEventSentNID int64
	var stateSnapshotNID int64
	row := db.Raw(t.selectLatestEventNIDsForUpdateSQL, int64(roomNID)).Row()
	err := row.Scan(&nids, &lastEventSentNID, &stateSnapshotNID)
	if err != nil {
		return nil, 0, 0, err
	}
	eventNIDs := make([]types.EventNID, len(nids))
	for i := range nids {
		eventNIDs[i] = types.EventNID(nids[i])
	}
	return eventNIDs, types.EventNID(lastEventSentNID), types.StateSnapshotNID(stateSnapshotNID), nil
}

func (t *roomsTable) UpdateLatestEventNIDs(
	ctx context.Context,
	roomNID types.RoomNID,
	eventNIDs []types.EventNID,
	lastEventSentNID types.EventNID,
	stateSnapshotNID types.StateSnapshotNID,
) error {
	db := t.cm.Connection(ctx, false)

	err := db.Exec(
		t.updateLatestEventNIDsSQL,
		roomNID,
		eventNIDsAsArray(eventNIDs),
		int64(lastEventSentNID),
		int64(stateSnapshotNID),
	).Error
	return err
}

func (t *roomsTable) SelectRoomVersionsForRoomNIDs(
	ctx context.Context, roomNIDs []types.RoomNID,
) (map[types.RoomNID]gomatrixserverlib.RoomVersion, error) {
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.selectRoomVersionsForRoomNIDsSQL, roomNIDsAsArray(roomNIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectRoomVersionsForRoomNIDsStmt: rows.close() failed")
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

func (t *roomsTable) BulkSelectRoomIDs(ctx context.Context, roomNIDs []types.RoomNID) ([]string, error) {
	db := t.cm.Connection(ctx, true)

	var array pq.Int64Array
	for _, nid := range roomNIDs {
		array = append(array, int64(nid))
	}
	rows, err := db.Raw(t.bulkSelectRoomIDsSQL, array).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectRoomIDsStmt: rows.close() failed")
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

func (t *roomsTable) BulkSelectRoomNIDs(ctx context.Context, roomIDs []string) ([]types.RoomNID, error) {
	db := t.cm.Connection(ctx, true)

	var array pq.StringArray
	for _, roomID := range roomIDs {
		array = append(array, roomID)
	}
	rows, err := db.Raw(t.bulkSelectRoomNIDsSQL, array).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectRoomNIDsStmt: rows.close() failed")
	var roomNIDs []types.RoomNID
	var roomNID types.RoomNID
	for rows.Next() {
		if err = rows.Scan(&roomNID); err != nil {
			return nil, err
		}
		roomNIDs = append(roomNIDs, roomNID)
	}
	return roomNIDs, rows.Err()
}

func roomNIDsAsArray(roomNIDs []types.RoomNID) pq.Int64Array {
	nids := make([]int64, len(roomNIDs))
	for i := range roomNIDs {
		nids[i] = int64(roomNIDs[i])
	}
	return nids
}
