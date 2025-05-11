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
	"database/sql"
	"time"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/pitabwire/frame"
)

// Schema for peeks table
const peeksSchema = `
CREATE TABLE IF NOT EXISTS syncapi_peeks (
	id BIGINT DEFAULT nextval('syncapi_stream_id'),
	room_id TEXT NOT NULL,
	user_id TEXT NOT NULL,
	device_id TEXT NOT NULL,
	deleted BOOL NOT NULL DEFAULT false,
    -- When the peek was created in UNIX epoch ms.
    creation_ts BIGINT NOT NULL,
    UNIQUE(room_id, user_id, device_id)
);

CREATE INDEX IF NOT EXISTS syncapi_peeks_room_id_idx ON syncapi_peeks(room_id);
CREATE INDEX IF NOT EXISTS syncapi_peeks_user_id_device_id_idx ON syncapi_peeks(user_id, device_id);
`

// Revert schema for peeks table
const peeksSchemaRevert = `
DROP INDEX IF EXISTS syncapi_peeks_user_id_device_id_idx;
DROP INDEX IF EXISTS syncapi_peeks_room_id_idx;
DROP TABLE IF EXISTS syncapi_peeks;
`

// SQL query to insert a peek
const insertPeekSQL = `
INSERT INTO syncapi_peeks
 (room_id, user_id, device_id, creation_ts)
 VALUES ($1, $2, $3, $4)
 ON CONFLICT (room_id, user_id, device_id) DO UPDATE SET deleted=false, creation_ts=$4
 RETURNING id
`

// SQL query to delete a peek
const deletePeekSQL = `
UPDATE syncapi_peeks SET deleted=true, id=nextval('syncapi_stream_id') WHERE room_id = $1 AND user_id = $2 AND device_id = $3 RETURNING id
`

// SQL query to delete all peeks for a user in a room
const deletePeeksSQL = `
UPDATE syncapi_peeks SET deleted=true, id=nextval('syncapi_stream_id') WHERE room_id = $1 AND user_id = $2 RETURNING id
`

// SQL query to select peeks in range
// we care about all the peeks which were created in this range, deleted in this range,
// or were created before this range but haven't been deleted yet.
const selectPeeksInRangeSQL = `
SELECT room_id, deleted, (id > $3 AND id <= $4) AS changed FROM syncapi_peeks WHERE user_id = $1 AND device_id = $2 AND ((id <= $3 AND NOT deleted) OR (id > $3 AND id <= $4))
`

// SQL query to select peeking devices
const selectPeekingDevicesSQL = `
SELECT room_id, user_id, device_id FROM syncapi_peeks WHERE deleted=false
`

// SQL query to select max peek ID
const selectMaxPeekIDSQL = `
SELECT MAX(id) FROM syncapi_peeks
`

// SQL query to purge peeks
const purgePeeksSQL = `
DELETE FROM syncapi_peeks WHERE room_id = $1
`

// peeksTable implements tables.Peeks
type peeksTable struct {
	cm                      sqlutil.ConnectionManager
	insertPeekSQL           string
	deletePeekSQL           string
	deletePeeksSQL          string
	selectPeeksInRangeSQL   string
	selectPeekingDevicesSQL string
	selectMaxPeekIDSQL      string
	purgePeeksSQL           string
}

// NewPostgresPeeksTable creates a new peeks table
func NewPostgresPeeksTable(_ context.Context, cm sqlutil.ConnectionManager) (tables.Peeks, error) {

	// Perform the migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "syncapi_peeks_table_schema_001",
		Patch:       peeksSchema,
		RevertPatch: peeksSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	t := &peeksTable{
		cm:                      cm,
		insertPeekSQL:           insertPeekSQL,
		deletePeekSQL:           deletePeekSQL,
		deletePeeksSQL:          deletePeeksSQL,
		selectPeeksInRangeSQL:   selectPeeksInRangeSQL,
		selectPeekingDevicesSQL: selectPeekingDevicesSQL,
		selectMaxPeekIDSQL:      selectMaxPeekIDSQL,
		purgePeeksSQL:           purgePeeksSQL,
	}

	return t, nil
}

// InsertPeek adds a new peek
func (t *peeksTable) InsertPeek(
	ctx context.Context, roomID, userID, deviceID string,
) (streamPos types.StreamPosition, err error) {
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	db := t.cm.Connection(ctx, false)
	row := db.Raw(t.insertPeekSQL, roomID, userID, deviceID, nowMilli).Row()
	err = row.Scan(&streamPos)
	return
}

// DeletePeek marks a peek as deleted
func (t *peeksTable) DeletePeek(
	ctx context.Context, roomID, userID, deviceID string,
) (streamPos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	row := db.Raw(t.deletePeekSQL, roomID, userID, deviceID).Row()
	err = row.Scan(&streamPos)
	return
}

// DeletePeeks marks all peeks for a user in a room as deleted
func (t *peeksTable) DeletePeeks(
	ctx context.Context, roomID, userID string,
) (streamPos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	row := db.Raw(t.deletePeeksSQL, roomID, userID).Row()
	err = row.Scan(&streamPos)
	return
}

// SelectPeeksInRange retrieves peeks in the given range
func (t *peeksTable) SelectPeeksInRange(
	ctx context.Context, userID, deviceID string, r types.Range,
) (peeks []types.Peek, err error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectPeeksInRangeSQL, userID, deviceID, r.Low(), r.High()).Rows()
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectPeeksInRange: rows.close() failed")

	for rows.Next() {
		peek := types.Peek{}
		var changed bool
		if err = rows.Scan(&peek.RoomID, &peek.Deleted, &changed); err != nil {
			return
		}
		peek.New = changed && !peek.Deleted
		peeks = append(peeks, peek)
	}

	return peeks, rows.Err()
}

// SelectPeekingDevices returns all peeking devices
func (t *peeksTable) SelectPeekingDevices(
	ctx context.Context,
) (peekingDevices map[string][]types.PeekingDevice, err error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectPeekingDevicesSQL).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectPeekingDevices: rows.close() failed")

	result := make(map[string][]types.PeekingDevice)
	for rows.Next() {
		var roomID, userID, deviceID string
		if err := rows.Scan(&roomID, &userID, &deviceID); err != nil {
			return nil, err
		}
		devices := result[roomID]
		devices = append(devices, types.PeekingDevice{UserID: userID, DeviceID: deviceID})
		result[roomID] = devices
	}
	return result, rows.Err()
}

// SelectMaxPeekID retrieves the maximum peek ID
func (t *peeksTable) SelectMaxPeekID(
	ctx context.Context,
) (id int64, err error) {
	var nullableID sql.NullInt64
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectMaxPeekIDSQL).Row()
	err = row.Scan(&nullableID)
	if nullableID.Valid {
		id = nullableID.Int64
	}
	return
}

// PurgePeeks deletes all peeks for a room
func (t *peeksTable) PurgePeeks(
	ctx context.Context, roomID string,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.purgePeeksSQL, roomID).Error
}
