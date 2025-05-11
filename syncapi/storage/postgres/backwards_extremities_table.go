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
	"github.com/pitabwire/frame"
)

// Schema for creating the backward extremities table
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

// Revert schema for backward extremities table
const backwardExtremitiesSchemaRevert = `
DROP TABLE IF EXISTS syncapi_backward_extremities;
`

// SQL query for inserting a backward extremity
const insertBackwardExtremitySQL = `
INSERT INTO syncapi_backward_extremities (room_id, event_id, prev_event_id)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING
`

// SQL query for selecting backward extremities for a room
const selectBackwardExtremitiesForRoomSQL = `
SELECT event_id, prev_event_id FROM syncapi_backward_extremities WHERE room_id = $1
`

// SQL query for deleting a backward extremity
const deleteBackwardExtremitySQL = `
DELETE FROM syncapi_backward_extremities WHERE room_id = $1 AND prev_event_id = $2
`

// SQL query for purging backward extremities
const purgeBackwardExtremitiesSQL = `
DELETE FROM syncapi_backward_extremities WHERE room_id = $1
`

// backwardExtremitiesTable represents a table storing backward extremities
type backwardExtremitiesTable struct {
	cm                                  sqlutil.ConnectionManager
	insertBackwardExtremitySQL          string
	selectBackwardExtremitiesForRoomSQL string
	deleteBackwardExtremitySQL          string
	purgeBackwardExtremitiesSQL         string
}

// NewPostgresBackwardsExtremitiesTable creates a new backward extremities table
func NewPostgresBackwardsExtremitiesTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.BackwardsExtremities, error) {
	t := &backwardExtremitiesTable{
		cm:                                  cm,
		insertBackwardExtremitySQL:          insertBackwardExtremitySQL,
		selectBackwardExtremitiesForRoomSQL: selectBackwardExtremitiesForRoomSQL,
		deleteBackwardExtremitySQL:          deleteBackwardExtremitySQL,
		purgeBackwardExtremitiesSQL:         purgeBackwardExtremitiesSQL,
	}

	// Perform the migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "syncapi_backward_extremities_table_schema_001",
		Patch:       backwardExtremitiesSchema,
		RevertPatch: backwardExtremitiesSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (t *backwardExtremitiesTable) InsertsBackwardExtremity(
	ctx context.Context, roomID, eventID string, prevEventID string,
) (err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Exec(t.insertBackwardExtremitySQL, roomID, eventID, prevEventID).Error
	return
}

func (t *backwardExtremitiesTable) SelectBackwardExtremitiesForRoom(
	ctx context.Context, roomID string,
) (bwExtrems map[string][]string, err error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectBackwardExtremitiesForRoomSQL, roomID).Rows()
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

func (t *backwardExtremitiesTable) DeleteBackwardExtremity(
	ctx context.Context, roomID, knownEventID string,
) (err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Exec(t.deleteBackwardExtremitySQL, roomID, knownEventID).Error
	return
}

func (t *backwardExtremitiesTable) PurgeBackwardExtremities(
	ctx context.Context, roomID string,
) error {
	db := t.cm.Connection(ctx, false)
	err := db.Exec(t.purgeBackwardExtremitiesSQL, roomID).Error
	return err
}
