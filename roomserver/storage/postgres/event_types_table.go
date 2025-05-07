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

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
)

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
    -- The string event type.
    event_type TEXT NOT NULL CONSTRAINT roomserver_event_type_unique UNIQUE
);
`

// SQL query constants for event types operations
const (
	// insertEventTypeNIDSQL inserts a new event type into the table
	insertEventTypeNIDSQL = "" +
		"INSERT INTO roomserver_event_types (event_type)" +
		" VALUES ($1)" +
		" ON CONFLICT ON CONSTRAINT roomserver_event_type_unique" +
		" DO NOTHING" +
		" RETURNING event_type_nid"

	// selectEventTypeNIDSQL selects an event type NID from its type string
	selectEventTypeNIDSQL = "" +
		"SELECT event_type_nid FROM roomserver_event_types" +
		" WHERE event_type = $1"

	// bulkSelectEventTypeNIDSQL selects event type NIDs from an array of type strings
	bulkSelectEventTypeNIDSQL = "" +
		"SELECT event_type, event_type_nid FROM roomserver_event_types" +
		" WHERE event_type = ANY($1)"

	// bulkSelectEventTypeSQL selects event types from an array of type NIDs
	bulkSelectEventTypeSQL = "" +
		"SELECT event_type_nid, event_type FROM roomserver_event_types" +
		" WHERE event_type_nid = ANY($1)"
)

type eventTypesStatements struct {
	cm *sqlutil.Connections

	// SQL statements stored as struct fields
	insertEventTypeNIDStmt     string
	selectEventTypeNIDStmt     string
	bulkSelectEventTypeNIDStmt string
	bulkSelectEventTypeStmt    string
}

// NewPostgresEventTypesTable creates a new PostgreSQL event types table and prepares all statements
func NewPostgresEventTypesTable(ctx context.Context, cm *sqlutil.Connections) (tables.EventTypes, error) {
	// Create the table first
	if err := cm.Writer.ExecSQL(ctx, eventTypesSchema); err != nil {
		return nil, err
	}

	// Initialize the statements
	s := &eventTypesStatements{
		cm: cm,

		// Initialize SQL statement fields with the constants
		insertEventTypeNIDStmt:     insertEventTypeNIDSQL,
		selectEventTypeNIDStmt:     selectEventTypeNIDSQL,
		bulkSelectEventTypeNIDStmt: bulkSelectEventTypeNIDSQL,
		bulkSelectEventTypeStmt:    bulkSelectEventTypeSQL,
	}

	return s, nil
}

// InsertEventTypeNID implements tables.EventTypes
func (s *eventTypesStatements) InsertEventTypeNID(
	ctx context.Context, txn *sql.Tx, eventType string,
) (types.EventTypeNID, error) {
	// Get database connection
	var db *sql.Conn
	var err error

	if txn != nil {
		// Use existing transaction.
		var eventTypeNID int64
		err = txn.QueryRowContext(ctx, s.insertEventTypeNIDStmt, eventType).Scan(&eventTypeNID)
		if err == sql.ErrNoRows {
			// We didn't insert a new event type so we need to look up what
			// the existing NID is.
			err = txn.QueryRowContext(ctx, s.selectEventTypeNIDStmt, eventType).Scan(&eventTypeNID)
		}
		return types.EventTypeNID(eventTypeNID), err
	}

	// Acquire a new connection, since we're not using a transaction.
	if db, err = s.cm.Writer.GetSQLConn(ctx); err != nil {
		return 0, err
	}
	defer internal.CloseAndLogIfError(ctx, db, "InsertEventTypeNID: failed to close connection")

	// Execute the insertion
	var eventTypeNID int64
	err = db.QueryRowContext(ctx, s.insertEventTypeNIDStmt, eventType).Scan(&eventTypeNID)
	if err == sql.ErrNoRows {
		// We didn't insert a new event type so we need to look up what
		// the existing NID is.
		err = db.QueryRowContext(ctx, s.selectEventTypeNIDStmt, eventType).Scan(&eventTypeNID)
	}
	return types.EventTypeNID(eventTypeNID), err
}

// SelectEventTypeNID implements tables.EventTypes
func (s *eventTypesStatements) SelectEventTypeNID(
	ctx context.Context, txn *sql.Tx, eventType string,
) (types.EventTypeNID, error) {
	// Get database connection
	var eventTypeNID int64
	var err error

	if txn != nil {
		// Use existing transaction.
		err = txn.QueryRowContext(ctx, s.selectEventTypeNIDStmt, eventType).Scan(&eventTypeNID)
		return types.EventTypeNID(eventTypeNID), err
	}

	// Use a new connection since we're not in a transaction.
	db := s.cm.Connection(ctx, true)
	err = db.Raw(s.selectEventTypeNIDStmt, eventType).Row().Scan(&eventTypeNID)
	return types.EventTypeNID(eventTypeNID), err
}

// BulkSelectEventTypeNID implements tables.EventTypes
func (s *eventTypesStatements) BulkSelectEventTypeNID(
	ctx context.Context, txn *sql.Tx, eventTypes []string,
) (map[string]types.EventTypeNID, error) {
	if len(eventTypes) == 0 {
		return nil, nil
	}

	// Get database connection
	var err error
	var rows *sql.Rows

	if txn != nil {
		// Use existing transaction.
		rows, err = txn.QueryContext(ctx, s.bulkSelectEventTypeNIDStmt, pq.StringArray(eventTypes))
	} else {
		// Use a new connection since we're not in a transaction.
		db := s.cm.Connection(ctx, true)
		rows, err = db.Raw(s.bulkSelectEventTypeNIDStmt, pq.StringArray(eventTypes)).Rows()
	}

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "BulkSelectEventTypeNID: rows.close() failed")

	// Create result map
	result := make(map[string]types.EventTypeNID, len(eventTypes))
	var eventType string
	var eventTypeNID int64

	// Process rows
	for rows.Next() {
		if err = rows.Scan(&eventType, &eventTypeNID); err != nil {
			return nil, err
		}
		result[eventType] = types.EventTypeNID(eventTypeNID)
	}

	return result, rows.Err()
}

// BulkSelectEventType implements tables.EventTypes
func (s *eventTypesStatements) BulkSelectEventType(
	ctx context.Context, txn *sql.Tx, nids []types.EventTypeNID,
) (map[types.EventTypeNID]string, error) {
	if len(nids) == 0 {
		return nil, nil
	}

	// Convert nids to int64 array
	eventTypeNIDs := make([]int64, len(nids))
	for i := range nids {
		eventTypeNIDs[i] = int64(nids[i])
	}

	// Get database connection
	var err error
	var rows *sql.Rows

	if txn != nil {
		// Use existing transaction.
		rows, err = txn.QueryContext(ctx, s.bulkSelectEventTypeStmt, pq.Int64Array(eventTypeNIDs))
	} else {
		// Use a new connection since we're not in a transaction.
		db := s.cm.Connection(ctx, true)
		rows, err = db.Raw(s.bulkSelectEventTypeStmt, pq.Int64Array(eventTypeNIDs)).Rows()
	}

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "BulkSelectEventType: rows.close() failed")

	// Create result map
	result := make(map[types.EventTypeNID]string, len(nids))
	var eventTypeNID int64
	var eventType string

	// Process rows
	for rows.Next() {
		if err = rows.Scan(&eventTypeNID, &eventType); err != nil {
			return nil, err
		}
		result[types.EventTypeNID(eventTypeNID)] = eventType
	}

	return result, rows.Err()
}
