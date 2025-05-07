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
	"time"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
)

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

type peeksTable struct {
	cm                       *sqlutil.Connections
	insertPeekSQL            string
	deletePeekSQL            string
	deletePeeksSQL           string
	selectPeeksInRangeSQL    string
	selectPeekingDevicesSQL  string
	selectMaxPeekIDSQL       string
	purgePeeksSQL            string
}

func NewPostgresPeeksTable(ctx context.Context, cm *sqlutil.Connections) (tables.Peeks, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(peeksSchema).Error; err != nil {
		return nil, err
	}
	
	// Initialize the table with SQL statements
	s := &peeksTable{
		cm:                      cm,
		insertPeekSQL:           insertPeekSQL,
		deletePeekSQL:           deletePeekSQL,
		deletePeeksSQL:          deletePeeksSQL,
		selectPeeksInRangeSQL:   selectPeeksInRangeSQL,
		selectPeekingDevicesSQL: selectPeekingDevicesSQL,
		selectMaxPeekIDSQL:      selectMaxPeekIDSQL,
		purgePeeksSQL:           purgePeeksSQL,
	}
	return s, nil
}

func (s *peeksTable) InsertPeek(
	ctx context.Context, roomID, userID, deviceID string,
) (streamPos types.StreamPosition, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	row := db.Raw(s.insertPeekSQL, roomID, userID, deviceID, nowMilli).Row()
	err = row.Scan(&streamPos)
	return
}

func (s *peeksTable) DeletePeek(
	ctx context.Context, roomID, userID, deviceID string,
) (streamPos types.StreamPosition, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	row := db.Raw(s.deletePeekSQL, roomID, userID, deviceID).Row()
	err = row.Scan(&streamPos)
	return
}

func (s *peeksTable) DeletePeeks(
	ctx context.Context, roomID, userID string,
) (streamPos types.StreamPosition, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	row := db.Raw(s.deletePeeksSQL, roomID, userID).Row()
	err = row.Scan(&streamPos)
	return
}

func (s *peeksTable) SelectPeeksInRange(
	ctx context.Context, userID, deviceID string, r types.Range,
) (peeks []types.Peek, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	rows, err := db.Raw(s.selectPeeksInRangeSQL, userID, deviceID, r.Low(), r.High()).Rows()
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

func (s *peeksTable) SelectPeekingDevices(
	ctx context.Context,
) (peekingDevices map[string][]types.PeekingDevice, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	rows, err := db.Raw(s.selectPeekingDevicesSQL).Rows()
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

func (s *peeksTable) SelectMaxPeekID(
	ctx context.Context,
) (id int64, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	err = db.Raw(s.selectMaxPeekIDSQL).Scan(&id).Error
	return
}

func (s *peeksTable) PurgePeeks(
	ctx context.Context, roomID string,
) error {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	return db.Exec(s.purgePeeksSQL, roomID).Error
}
