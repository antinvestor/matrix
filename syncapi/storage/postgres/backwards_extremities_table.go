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

const backwardExtremitiesSchemaRevert = `DROP TABLE IF EXISTS syncapi_backward_extremities;`

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

// backwardsExtremitiesTable implements tables.BackwardsExtremities using a connection manager and SQL constants.
type backwardsExtremitiesTable struct {
	cm                                  *sqlutil.Connections
	insertBackwardExtremitySQL          string
	selectBackwardExtremitiesForRoomSQL string
	deleteBackwardExtremitySQL          string
	purgeBackwardExtremitiesSQL         string
}

// NewPostgresBackwardsExtremitiesTable creates a new BackwardsExtremities table using a connection manager.
func NewPostgresBackwardsExtremitiesTable(cm *sqlutil.Connections) tables.BackwardsExtremities {
	return &backwardsExtremitiesTable{
		cm:                                  cm,
		insertBackwardExtremitySQL:          insertBackwardExtremitySQL,
		selectBackwardExtremitiesForRoomSQL: selectBackwardExtremitiesForRoomSQL,
		deleteBackwardExtremitySQL:          deleteBackwardExtremitySQL,
		purgeBackwardExtremitiesSQL:         purgeBackwardExtremitiesSQL,
	}
}

// InsertsBackwardExtremity inserts a backwards extremity for a room.
func (t *backwardsExtremitiesTable) InsertsBackwardExtremity(ctx context.Context, roomID, eventID, prevEventID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.insertBackwardExtremitySQL, roomID, eventID, prevEventID).Error
}

// SelectBackwardExtremitiesForRoom returns a map of event_id to prev_event_ids for a room.
func (t *backwardsExtremitiesTable) SelectBackwardExtremitiesForRoom(ctx context.Context, roomID string) (bwExtrems map[string][]string, err error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectBackwardExtremitiesForRoomSQL, roomID).Rows()
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectBackwardExtremitiesForRoom: rows.close() failed")
	bwExtrems = make(map[string][]string)
	for rows.Next() {
		var eID, prevEventID string
		if err = rows.Scan(&eID, &prevEventID); err != nil {
			return
		}
		bwExtrems[eID] = append(bwExtrems[eID], prevEventID)
	}
	return bwExtrems, rows.Err()
}

// DeleteBackwardExtremity deletes a backwards extremity for a room.
func (t *backwardsExtremitiesTable) DeleteBackwardExtremity(ctx context.Context, roomID, knownEventID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteBackwardExtremitySQL, roomID, knownEventID).Error
}

// PurgeBackwardExtremities deletes all backwards extremities for a room.
func (t *backwardsExtremitiesTable) PurgeBackwardExtremities(ctx context.Context, roomID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.purgeBackwardExtremitiesSQL, roomID).Error
}
