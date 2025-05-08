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

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres/deltas"
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

// SQL query constants for previous events operations
const (
	// insertPreviousEventSQL inserts a previous event into the table and appends to the event_nids array
	insertPreviousEventSQL = "" +
		"INSERT INTO roomserver_previous_events" +
		" (previous_event_id, event_nids)" +
		" VALUES ($1, array_append('{}'::bigint[], $2))" +
		" ON CONFLICT ON CONSTRAINT roomserver_previous_event_id_unique" +
		" DO UPDATE SET event_nids = array_append(roomserver_previous_events.event_nids, $2)" +
		" WHERE $2 != ALL(roomserver_previous_events.event_nids)"

	// selectPreviousEventExistsSQL checks if a previous event ID exists in the table
	selectPreviousEventExistsSQL = "" +
		"SELECT 1 FROM roomserver_previous_events" +
		" WHERE previous_event_id = $1"
)

type previousEventsStatements struct {
	cm *sqlutil.Connections

	// SQL statements stored as struct fields
	insertPreviousEventStmt       string
	selectPreviousEventExistsStmt string
}

// NewPostgresPreviousEventsTable creates a new PostgreSQL previous events table and prepares all statements
func NewPostgresPreviousEventsTable(ctx context.Context, cm *sqlutil.Connections) (tables.PreviousEvents, error) {
	// Create the table first
	if err := cm.Writer.ExecSQL(ctx, previousEventSchema); err != nil {
		return nil, err
	}

	// Run any migrations
	m := sqlutil.NewMigrator(cm.Writer.DB)
	m.AddMigrations([]sqlutil.Migration{
		{
			Version: "roomserver: drop column reference_sha from roomserver_prev_events",
			Up:      deltas.UpDropEventReferenceSHAPrevEvents,
		},
	}...)
	if err := m.Up(ctx); err != nil {
		return nil, err
	}

	// Initialize the table
	s := &previousEventsStatements{
		cm: cm,

		// Initialize SQL statement fields with the constants
		insertPreviousEventStmt:       insertPreviousEventSQL,
		selectPreviousEventExistsStmt: selectPreviousEventExistsSQL,
	}

	return s, nil
}

// InsertPreviousEvent inserts a previous event into the table
// This updates the event_nids list for the previous_event_id in the table
func (s *previousEventsStatements) InsertPreviousEvent(
	ctx context.Context,
	previousEventID string,
	eventNID types.EventNID,
) error {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	if txn != nil {
		return txn.Exec(s.insertPreviousEventStmt, previousEventID, int64(eventNID)).Error
	}

	return db.Exec(s.insertPreviousEventStmt, previousEventID, int64(eventNID)).Error
}

// SelectPreviousEventExists checks if the event reference exists
// Returns sql.ErrNoRows if the event reference doesn't exist.
func (s *previousEventsStatements) SelectPreviousEventExists(
	ctx context.Context, eventID string,
) error {
	// Get database connection
	var count int64
	var err error

	db := s.cm.Connection(ctx, true)
	err = db.Raw(s.selectPreviousEventExistsStmt, eventID).Row().Scan(&count)

	if err != nil {
		return err
	}

	if count == 0 {
		return sql.ErrNoRows
	}

	return nil
}
