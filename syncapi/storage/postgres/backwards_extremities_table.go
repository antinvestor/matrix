// Copyright 2018 New Vector Ltd
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

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
)

const backwardExtremitiesSchema = `
-- Stores output room events received from the roomserver.
CREATE TABLE IF NOT EXISTS syncapi_backward_extremities (
	-- The 'room_id' key for the event.
	room_id TEXT NOT NULL,
	-- The event ID for the last known event. This is the backwards extremity.
	event_id TEXT NOT NULL,
	-- The prev_events for the last known event. This is used to update extremities.
	prev_event_id TEXT NOT NULL,
	PRIMARY KEY(room_id, event_id, prev_event_id)
);
`

const insertBackwardExtremitySQL = "" +
	"INSERT INTO syncapi_backward_extremities (room_id, event_id, prev_event_id)" +
	" VALUES ($1, $2, $3)" +
	" ON CONFLICT DO NOTHING"

const selectBackwardExtremitiesForRoomSQL = "" +
	"SELECT event_id, prev_event_id FROM syncapi_backward_extremities WHERE room_id = $1"

const deleteBackwardExtremitySQL = "" +
	"DELETE FROM syncapi_backward_extremities WHERE room_id = $1 AND prev_event_id = $2"

const purgeBackwardExtremitiesSQL = "" +
	"DELETE FROM syncapi_backward_extremities WHERE room_id = $1"

type backwardsExtremitiesTable struct {
	cm                                   *sqlutil.Connections
	insertBackwardExtremitySQL           string
	selectBackwardExtremitiesForRoomSQL  string
	deleteBackwardExtremitySQL           string
	purgeBackwardExtremitiesSQL          string
}

func NewPostgresBackwardsExtremitiesTable(ctx context.Context, cm *sqlutil.Connections) (tables.BackwardsExtremities, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(backwardExtremitiesSchema).Error; err != nil {
		return nil, err
	}
	
	// Initialize the table with SQL statements
	s := &backwardsExtremitiesTable{
		cm:                                  cm,
		insertBackwardExtremitySQL:          insertBackwardExtremitySQL,
		selectBackwardExtremitiesForRoomSQL: selectBackwardExtremitiesForRoomSQL,
		deleteBackwardExtremitySQL:          deleteBackwardExtremitySQL,
		purgeBackwardExtremitiesSQL:         purgeBackwardExtremitiesSQL,
	}
	
	return s, nil
}

func (s *backwardsExtremitiesTable) InsertsBackwardExtremity(
	ctx context.Context, roomID, eventID string, prevEventID string,
) (err error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	// Execute query
	return db.Exec(s.insertBackwardExtremitySQL, roomID, eventID, prevEventID).Error
}

func (s *backwardsExtremitiesTable) SelectBackwardExtremitiesForRoom(
	ctx context.Context, roomID string,
) (bwExtrems map[string][]string, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	rows, err := db.Raw(s.selectBackwardExtremitiesForRoomSQL, roomID).Rows()
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectBackwardExtremitiesForRoom: rows.close() failed")

	bwExtrems = make(map[string][]string)
	for rows.Next() {
		var eID string
		var prevEventID string
		if err = rows.Scan(&eID, &prevEventID); err != nil {
			return
		}
		bwExtrems[eID] = append(bwExtrems[eID], prevEventID)
	}

	return bwExtrems, rows.Err()
}

func (s *backwardsExtremitiesTable) DeleteBackwardExtremity(
	ctx context.Context, roomID, knownEventID string,
) (err error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	// Execute query
	return db.Exec(s.deleteBackwardExtremitySQL, roomID, knownEventID).Error
}

func (s *backwardsExtremitiesTable) PurgeBackwardExtremities(
	ctx context.Context, roomID string,
) error {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	// Execute query
	return db.Exec(s.purgeBackwardExtremitiesSQL, roomID).Error
}
