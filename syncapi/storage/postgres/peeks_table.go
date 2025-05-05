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
	"database/sql"
	"time"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
)

const peeksSchema = `
-- This sequence is shared between all the tables generated from kafka logs.
CREATE SEQUENCE IF NOT EXISTS syncapi_stream_id;

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

const peeksSchemaRevert = `DROP TABLE IF EXISTS syncapi_peeks;`

const insertPeekSQL = "" +
	"INSERT INTO syncapi_peeks" +
	" (room_id, user_id, device_id, creation_ts)" +
	" VALUES ($1, $2, $3, $4)" +
	" ON CONFLICT (room_id, user_id, device_id) DO UPDATE SET deleted=false, creation_ts=$4" +
	" RETURNING id"

const deletePeekSQL = "" +
	"UPDATE syncapi_peeks SET deleted=true, id=nextval('syncapi_stream_id') WHERE room_id = $1 AND user_id = $2 AND device_id = $3 RETURNING id"

const deletePeeksSQL = "" +
	"UPDATE syncapi_peeks SET deleted=true, id=nextval('syncapi_stream_id') WHERE room_id = $1 AND user_id = $2 RETURNING id"

// we care about all the peeks which were created in this range, deleted in this range,
// or were created before this range but haven't been deleted yet.
const selectPeeksInRangeSQL = "" +
	"SELECT room_id, deleted, (id > $3 AND id <= $4) AS changed FROM syncapi_peeks WHERE user_id = $1 AND device_id = $2 AND ((id <= $3 AND NOT deleted) OR (id > $3 AND id <= $4))"

const selectPeekingDevicesSQL = "" +
	"SELECT room_id, user_id, device_id FROM syncapi_peeks WHERE deleted=false"

const selectMaxPeekIDSQL = "" +
	"SELECT MAX(id) FROM syncapi_peeks"

const purgePeeksSQL = "" +
	"DELETE FROM syncapi_peeks WHERE room_id = $1"

// peeksTable implements tables.Peeks using a connection manager and SQL constants.
type peeksTable struct {
	cm                      *sqlutil.Connections
	insertPeekSQL           string
	deletePeekSQL           string
	deletePeeksSQL          string
	selectPeeksInRangeSQL   string
	selectPeekingDevicesSQL string
	selectMaxPeekIDSQL      string
	purgePeeksSQL           string
}

// NewPostgresPeeksTable creates a new Peeks table using a connection manager.
func NewPostgresPeeksTable(cm *sqlutil.Connections) tables.Peeks {
	return &peeksTable{
		cm:                      cm,
		insertPeekSQL:           insertPeekSQL,
		deletePeekSQL:           deletePeekSQL,
		deletePeeksSQL:          deletePeeksSQL,
		selectPeeksInRangeSQL:   selectPeeksInRangeSQL,
		selectPeekingDevicesSQL: selectPeekingDevicesSQL,
		selectMaxPeekIDSQL:      selectMaxPeekIDSQL,
		purgePeeksSQL:           purgePeeksSQL,
	}
}

// InsertPeek inserts or updates a peek for a user/device in a room and returns the new stream position.
func (t *peeksTable) InsertPeek(
	ctx context.Context,
	roomID, userID, deviceID string,
) (streamPos types.StreamPosition, err error) {
	// Use the current time in milliseconds as the creation timestamp.
	creationTS := time.Now().UnixNano() / int64(time.Millisecond)
	db := t.cm.Connection(ctx, false)
	err = db.Raw(t.insertPeekSQL, roomID, userID, deviceID, creationTS).Row().Scan(&streamPos)
	return
}

// DeletePeek marks a peek as deleted for a specific user/device in a room and returns the new stream position.
func (t *peeksTable) DeletePeek(
	ctx context.Context,
	roomID, userID, deviceID string,
) (streamPos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Raw(t.deletePeekSQL, roomID, userID, deviceID).Row().Scan(&streamPos)
	return
}

// DeletePeeks marks all peeks for a user in a room as deleted and returns the new stream position.
func (t *peeksTable) DeletePeeks(
	ctx context.Context,
	roomID, userID string,
) (streamPos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Raw(t.deletePeeksSQL, roomID, userID).Row().Scan(&streamPos)
	return
}

// SelectPeeksInRange returns peeks for a user/device in a given stream position range.
// Only peeks that changed in the range are returned.
func (t *peeksTable) SelectPeeksInRange(
	ctx context.Context,
	userID, deviceID string, r types.Range,
) (peeks []types.Peek, err error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectPeeksInRangeSQL, userID, deviceID, r.Low(), r.High()).Rows()
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var p types.Peek
		var changed bool
		if err = rows.Scan(&p.RoomID, &p.Deleted, &changed); err != nil {
			return
		}
		if changed {
			peeks = append(peeks, p)
		}
	}
	return
}

// SelectPeekingDevices returns all peeking devices, grouped by room ID.
func (t *peeksTable) SelectPeekingDevices(
	ctx context.Context,
) (peekingDevices map[string][]types.PeekingDevice, err error) {
	peekingDevices = make(map[string][]types.PeekingDevice)
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectPeekingDevicesSQL).Rows()
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var pd types.PeekingDevice
		var roomID string
		if err = rows.Scan(&roomID, &pd.UserID, &pd.DeviceID); err != nil {
			return
		}
		peekingDevices[roomID] = append(peekingDevices[roomID], pd)
	}
	return
}

// SelectMaxPeekID returns the maximum stream position for peeks.
func (t *peeksTable) SelectMaxPeekID(
	ctx context.Context,
) (id int64, err error) {
	db := t.cm.Connection(ctx, true)
	var nullableID sql.NullInt64
	err = db.Raw(t.selectMaxPeekIDSQL).Row().Scan(&nullableID)
	if nullableID.Valid {
		id = nullableID.Int64
	}
	return
}

// PurgePeeks removes all peeks for a given room.
func (t *peeksTable) PurgePeeks(
	ctx context.Context,
	roomID string,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.purgePeeksSQL, roomID).Error
}
