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
)

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

const previousEventSchemaRevert = `DROP TABLE IF EXISTS roomserver_previous_events;`

const insertPreviousEventSQL = "" +
	"INSERT INTO roomserver_previous_events" +
	" (previous_event_id, event_nids)" +
	" VALUES ($1, array_append('{}'::bigint[], $2))" +
	" ON CONFLICT ON CONSTRAINT roomserver_previous_event_id_unique" +
	" DO UPDATE SET event_nids = array_append(roomserver_previous_events.event_nids, $2)" +
	" WHERE $2 != ALL(roomserver_previous_events.event_nids)"

const selectPreviousEventExistsSQL = "" +
	"SELECT 1 FROM roomserver_previous_events" +
	" WHERE previous_event_id = $1"

type previousEventsTable struct {
	cm                           *sqlutil.Connections
	insertPreviousEventSQL       string
	selectPreviousEventExistsSQL string
}

func NewPostgresPreviousEventsTable(cm *sqlutil.Connections) tables.PreviousEvents {
	return &previousEventsTable{
		cm:                           cm,
		insertPreviousEventSQL:       insertPreviousEventSQL,
		selectPreviousEventExistsSQL: selectPreviousEventExistsSQL,
	}
}

func (t *previousEventsTable) InsertPreviousEvent(ctx context.Context, previousEventID string, eventNID types.EventNID) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.insertPreviousEventSQL, previousEventID, int64(eventNID)).Error
}

func (t *previousEventsTable) SelectPreviousEventExists(ctx context.Context, eventID string) error {
	db := t.cm.Connection(ctx, true)
	var ok int64
	row := db.Raw(t.selectPreviousEventExistsSQL, eventID).Row()
	return row.Scan(&ok)
}
