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

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
)

// Schema for the event types table
const eventTypesSchema = `
-- Numeric versions of the event "type"s. Event types tend to be taken from a
-- small internal pool. Assigning each a numeric ID should reduce the amount of
-- data that needs to be stored and fetched from the database.
-- It also means that many operations can work with int64 arrays rather than
-- string arrays which may help reduce GC pressure.
-- Well known event types are pre-assigned numeric IDs:
--   1 -> m.room.create
--   2 -> m.room.power_levels
--   3 -> m.room.join_rules
--   4 -> m.room.third_party_invite
--   5 -> m.room.member
--   6 -> m.room.redaction
--   7 -> m.room.history_visibility
-- Picking well-known numeric IDs for the events types that require special
-- attention during state conflict resolution means that we write that code
-- using numeric constants.
-- It also means that the numeric IDs for internal event types should be
-- consistent between different instances which might make ad-hoc debugging
-- easier.
-- Other event types are automatically assigned numeric IDs starting from 2**16.
-- This leaves room to add more pre-assigned numeric IDs and clearly separates
-- the automatically assigned IDs from the pre-assigned IDs.
CREATE SEQUENCE IF NOT EXISTS roomserver_event_type_nid_seq START 65536;
CREATE TABLE IF NOT EXISTS roomserver_event_types (
    -- Local numeric ID for the event type.
    event_type_nid BIGINT PRIMARY KEY DEFAULT nextval('roomserver_event_type_nid_seq'),
    -- The string event_type.
    event_type TEXT NOT NULL CONSTRAINT roomserver_event_type_unique UNIQUE
);
INSERT INTO roomserver_event_types (event_type_nid, event_type) VALUES
    (1, 'm.room.create'),
    (2, 'm.room.power_levels'),
    (3, 'm.room.join_rules'),
    (4, 'm.room.third_party_invite'),
    (5, 'm.room.member'),
    (6, 'm.room.redaction'),
    (7, 'm.room.history_visibility') ON CONFLICT DO NOTHING;
`

// Schema revert script for migration purposes
const eventTypesSchemaRevert = `DROP TABLE IF EXISTS roomserver_event_types;`

// SQL for inserting a new event type and returning its numeric ID
// The usual case is that the event type is not in the database.
// In that case the ID will be assigned using the next value from the sequence.
// We use `RETURNING` to tell postgres to return the assigned ID.
// But it's possible that the type was added in a query that raced with us.
// This will result in a conflict on the event_type_unique constraint, in this
// case we do nothing. Postgresql won't return a row in that case so we rely on
// the caller catching the sql.ErrNoRows error and running a select to get the row.
const insertEventTypeNIDSQL = "" +
	"INSERT INTO roomserver_event_types (event_type) VALUES ($1)" +
	" ON CONFLICT ON CONSTRAINT roomserver_event_type_unique" +
	" DO NOTHING RETURNING (event_type_nid)"

// SQL to select a numeric ID for an event type
const selectEventTypeNIDSQL = "" +
	"SELECT event_type_nid FROM roomserver_event_types WHERE event_type = $1"

// SQL to bulk select numeric IDs for event types
// Takes an array of strings as the query parameter.
const bulkSelectEventTypeNIDSQL = "" +
	"SELECT event_type, event_type_nid FROM roomserver_event_types" +
	" WHERE event_type = ANY($1)"

// eventTypesTable implements the tables.EventTypes interface using GORM
type eventTypesTable struct {
	cm *sqlutil.Connections

	// SQL query strings loaded from constants
	insertEventTypeNIDSQL     string
	selectEventTypeNIDSQL     string
	bulkSelectEventTypeNIDSQL string
}

// NewPostgresEventTypesTable creates a new event types table
func NewPostgresEventTypesTable(ctx context.Context, cm *sqlutil.Connections) (tables.EventTypes, error) {
	// Create the table if it doesn't exist using migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "roomserver_event_types_table_schema_001",
		Patch:       eventTypesSchema,
		RevertPatch: eventTypesSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Initialize the table struct with just the connection manager
	t := &eventTypesTable{
		cm: cm,

		// Initialize SQL query strings from constants
		insertEventTypeNIDSQL:     insertEventTypeNIDSQL,
		selectEventTypeNIDSQL:     selectEventTypeNIDSQL,
		bulkSelectEventTypeNIDSQL: bulkSelectEventTypeNIDSQL,
	}

	return t, nil
}

func (t *eventTypesTable) InsertEventTypeNID(
	ctx context.Context, eventType string,
) (types.EventTypeNID, error) {
	db := t.cm.Connection(ctx, false)

	var eventTypeNID int64
	row := db.Raw(t.insertEventTypeNIDSQL, eventType).Row()
	err := row.Scan(&eventTypeNID)
	return types.EventTypeNID(eventTypeNID), err
}

func (t *eventTypesTable) SelectEventTypeNID(
	ctx context.Context, eventType string,
) (types.EventTypeNID, error) {
	db := t.cm.Connection(ctx, true)

	var eventTypeNID int64
	row := db.Raw(t.selectEventTypeNIDSQL, eventType).Row()
	err := row.Scan(&eventTypeNID)
	return types.EventTypeNID(eventTypeNID), err
}

func (t *eventTypesTable) BulkSelectEventTypeNID(
	ctx context.Context, eventTypes []string,
) (map[string]types.EventTypeNID, error) {
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.bulkSelectEventTypeNIDSQL, pq.StringArray(eventTypes)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectEventTypeNID: rows.close() failed")

	result := make(map[string]types.EventTypeNID, len(eventTypes))
	var eventType string
	var eventTypeNID int64
	for rows.Next() {
		if err := rows.Scan(&eventType, &eventTypeNID); err != nil {
			return nil, err
		}
		result[eventType] = types.EventTypeNID(eventTypeNID)
	}
	return result, rows.Err()
}
