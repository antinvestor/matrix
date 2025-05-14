// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Global.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/pitabwire/frame"
)

// Schema for the previous events table
const previousEventSchema = `
-- The previous events table stores the event_ids referenced by the events
-- stored in the events table.
-- This is used to tell if a new event is already referenced by an event in
-- the database.
CREATE TABLE IF NOT EXISTS roomserver_previous_events (
    -- The string event ID taken from the prev_events key of an event.
    previous_event_id TEXT NOT NULL,
    -- A list of numeric event IDs of events that reference this prev_event.
    event_nids BIGINT[] NOT NULL,
    CONSTRAINT roomserver_previous_event_id_unique UNIQUE (previous_event_id)
);
`

// Schema revert script for migration purposes
const previousEventSchemaRevert = `DROP TABLE IF EXISTS roomserver_previous_events;`

// Insert an entry into the previous_events table.
// If there is already an entry indicating that an event references that previous event then
// add the event NID to the list to indicate that this event references that previous event as well.
// This should only be modified while holding a "FOR UPDATE" lock on the row in the rooms table for this room.
// The lock is necessary to avoid data races when checking whether an event is already referenced by another event.
const insertPreviousEventSQL = "" +
	"INSERT INTO roomserver_previous_events" +
	" (previous_event_id, event_nids)" +
	" VALUES ($1, array_append('{}'::bigint[], $2))" +
	" ON CONFLICT ON CONSTRAINT roomserver_previous_event_id_unique" +
	" DO UPDATE SET event_nids = array_append(roomserver_previous_events.event_nids, $2)" +
	" WHERE $2 != ALL(roomserver_previous_events.event_nids)"

// Check if the event is referenced by another event in the table.
// This should only be done while holding a "FOR UPDATE" lock on the row in the rooms table for this room.
const selectPreviousEventExistsSQL = "" +
	"SELECT 1 FROM roomserver_previous_events" +
	" WHERE previous_event_id = $1"

// previousEventsTable implements the tables.PreviousEvents interface using GORM
type previousEventsTable struct {
	cm sqlutil.ConnectionManager

	// SQL query strings loaded from constants
	insertPreviousEventSQL       string
	selectPreviousEventExistsSQL string
}

// NewPostgresPreviousEventsTable creates a new previous events table
func NewPostgresPreviousEventsTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.PreviousEvents, error) {
	// Create the table if it doesn't exist using migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "roomserver_previous_events_table_schema_001",
		Patch:       previousEventSchema,
		RevertPatch: previousEventSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Initialize the table struct with just the connection manager
	t := &previousEventsTable{
		cm: cm,

		// Initialize SQL query strings from constants
		insertPreviousEventSQL:       insertPreviousEventSQL,
		selectPreviousEventExistsSQL: selectPreviousEventExistsSQL,
	}

	return t, nil
}

func (t *previousEventsTable) InsertPreviousEvent(
	ctx context.Context,
	previousEventID string,
	eventNID types.EventNID,
) error {
	db := t.cm.Connection(ctx, false)

	result := db.Exec(
		t.insertPreviousEventSQL, previousEventID, int64(eventNID),
	)
	return result.Error
}

// SelectPreviousEventExists checks if the event reference exists
// Returns an error if the event reference doesn't exist.
func (t *previousEventsTable) SelectPreviousEventExists(
	ctx context.Context, eventID string,
) error {
	db := t.cm.Connection(ctx, true)

	var ok int64
	row := db.Raw(t.selectPreviousEventExistsSQL, eventID).Row()
	err := row.Scan(&ok)
	return err
}
