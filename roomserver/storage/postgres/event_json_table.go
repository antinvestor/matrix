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
	"encoding/json"
	"errors"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
)

const eventJSONSchema = `
-- Stores the JSON for each event. This kept separate from the main events
-- table to keep the rows in the main events table small.
CREATE TABLE IF NOT EXISTS roomserver_event_json (
    -- Local numeric event ID for the event.
    event_nid BIGINT NOT NULL PRIMARY KEY,
    -- The JSON for the event.
    -- Stored as TEXT because this should be valid UTF-8.
    -- Not stored as JSON because we want to preserve the exact string formatting.
    event_json TEXT NOT NULL
);
`

// SQL query constants for event JSON operations
const (
	// Insert a new event JSON entry
	insertEventJSONSQL = "" +
		"INSERT INTO roomserver_event_json (event_nid, event_json)" +
		" VALUES ($1, $2)" +
		" ON CONFLICT DO NOTHING"

	// Select the JSON for a single event by its NID
	selectEventJSONSQL = "" +
		"SELECT event_json FROM roomserver_event_json WHERE event_nid = $1"

	// Select the JSON for multiple events by their NIDs
	bulkSelectEventJSONSQL = "" +
		"SELECT event_nid, event_json FROM roomserver_event_json WHERE event_nid = ANY($1)"

	// Delete an event JSON entry by its NID
	deleteEventJSONSQL = "" +
		"DELETE FROM roomserver_event_json WHERE event_nid = $1"

	// Delete multiple event JSON entries by their NIDs
	purgeEventJSONSQL = "" +
		"DELETE FROM roomserver_event_json WHERE event_nid = ANY($1)"
)

// eventJSONTable implements tables.EventJSON
type eventJSONTable struct {
	cm *sqlutil.Connections
	
	// SQL statements stored as struct fields
	insertEventJSONStmt      string
	selectEventJSONStmt      string
	bulkSelectEventJSONStmt  string
	deleteEventJSONStmt      string
	purgeEventJSONStmt       string
}

// NewPostgresEventJSONTable creates a new PostgreSQL event JSON table
func NewPostgresEventJSONTable(ctx context.Context, cm *sqlutil.Connections) (tables.EventJSON, error) {
	// Create the table first
	if err := cm.Writer.ExecSQL(ctx, eventJSONSchema); err != nil {
		return nil, err
	}

	// Initialize the table struct with SQL statements
	s := &eventJSONTable{
		cm: cm,
		
		// Initialize SQL statement fields with the constants
		insertEventJSONStmt:     insertEventJSONSQL,
		selectEventJSONStmt:     selectEventJSONSQL,
		bulkSelectEventJSONStmt: bulkSelectEventJSONSQL,
		deleteEventJSONStmt:     deleteEventJSONSQL,
		purgeEventJSONStmt:      purgeEventJSONSQL,
	}

	return s, nil
}

// InsertEventJSON implements tables.EventJSON
func (s *eventJSONTable) InsertEventJSON(
	ctx context.Context, eventNID types.EventNID, eventJSON []byte,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.insertEventJSONStmt, int64(eventNID), string(eventJSON)).Error
}

// BulkInsertEventJSON implements tables.EventJSON
func (s *eventJSONTable) BulkInsertEventJSON(
	ctx context.Context, eventNIDs []types.EventNID, eventJSONs [][]byte,
) error {
	if len(eventNIDs) == 0 {
		return nil
	}
	
	db := s.cm.Connection(ctx, false)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	
	defer internal.EndTransaction(ctx, tx, &err)
	
	for i := range eventNIDs {
		if err = tx.Exec(s.insertEventJSONStmt, int64(eventNIDs[i]), string(eventJSONs[i])).Error; err != nil {
			return err
		}
	}
	
	return nil
}

// SelectEventJSON implements tables.EventJSON
func (s *eventJSONTable) SelectEventJSON(
	ctx context.Context, eventNID types.EventNID,
) ([]byte, error) {
	db := s.cm.Connection(ctx, true)
	var eventJSON string
	
	if err := db.Raw(s.selectEventJSONStmt, int64(eventNID)).Row().Scan(&eventJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	
	return []byte(eventJSON), nil
}

// BulkSelectEventJSON implements tables.EventJSON
func (s *eventJSONTable) BulkSelectEventJSON(
	ctx context.Context, eventNIDs []types.EventNID,
) (map[types.EventNID][]byte, error) {
	if len(eventNIDs) == 0 {
		return nil, nil
	}
	
	db := s.cm.Connection(ctx, true)
	
	// Convert event NIDs to int64s for the query
	eventNIDInt64s := make([]int64, len(eventNIDs))
	for i := range eventNIDs {
		eventNIDInt64s[i] = int64(eventNIDs[i])
	}
	
	// Execute query
	rows, err := db.Raw(s.bulkSelectEventJSONStmt, pq.Int64Array(eventNIDInt64s)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectEventJSON: rows.close() failed")
	
	// Scan results
	result := make(map[types.EventNID][]byte, len(eventNIDs))
	var eventNID int64
	var eventJSON string
	
	for rows.Next() {
		if err = rows.Scan(&eventNID, &eventJSON); err != nil {
			return nil, err
		}
		result[types.EventNID(eventNID)] = []byte(eventJSON)
	}
	
	return result, rows.Err()
}

// DeleteEventJSON implements tables.EventJSON
func (s *eventJSONTable) DeleteEventJSON(
	ctx context.Context, eventNID types.EventNID,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteEventJSONStmt, int64(eventNID)).Error
}

// PurgeEventJSON implements tables.EventJSON
func (s *eventJSONTable) PurgeEventJSON(
	ctx context.Context, eventNIDs []types.EventNID,
) error {
	if len(eventNIDs) == 0 {
		return nil
	}
	
	db := s.cm.Connection(ctx, false)
	
	// Convert event NIDs to int64s for the query
	eventNIDInt64s := make([]int64, len(eventNIDs))
	for i := range eventNIDs {
		eventNIDInt64s[i] = int64(eventNIDs[i])
	}
	
	// Execute query
	return db.Exec(s.purgeEventJSONStmt, pq.Int64Array(eventNIDInt64s)).Error
}
