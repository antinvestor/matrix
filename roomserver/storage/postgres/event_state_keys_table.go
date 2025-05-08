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
	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
)

const eventStateKeysSchema = `
-- Numeric versions of the event "state_key"s. State keys tend to be reused so
-- assigning each a numeric ID should reduce the amount of data that needs to be
-- stored and fetched from the database.
-- These are numeric versions of the strings in roomserver_event_state_keys_table.
-- We can't use a normal foreign key relationship because we want to reuse state
-- keys across many different rooms and event types.
CREATE SEQUENCE IF NOT EXISTS roomserver_event_state_key_nid_seq;
CREATE TABLE IF NOT EXISTS roomserver_event_state_keys (
    -- Local numeric ID for the state key.
    event_state_key_nid BIGINT PRIMARY KEY DEFAULT nextval('roomserver_event_state_key_nid_seq'),
    -- The string state key. These tend to be reused so we cluster on this column.
    event_state_key TEXT NOT NULL CONSTRAINT roomserver_event_state_key_unique UNIQUE
);
`

// SQL query constants for event state keys operations
const (
	// insertEventStateKeySQL inserts a new state key into the table
	insertEventStateKeySQL = "" +
		"INSERT INTO roomserver_event_state_keys (event_state_key)" +
		" VALUES ($1)" +
		" ON CONFLICT ON CONSTRAINT roomserver_event_state_key_unique" +
		" DO NOTHING" +
		" RETURNING event_state_key_nid"

	// selectEventStateKeySQL selects an event state key NID from the event_state_key string
	selectEventStateKeySQL = "" +
		"SELECT event_state_key_nid FROM roomserver_event_state_keys" +
		" WHERE event_state_key = $1"

	// bulkSelectEventStateKeySQL selects event state key NIDs from an array of event_state_keys
	bulkSelectEventStateKeySQL = "" +
		"SELECT event_state_key, event_state_key_nid FROM roomserver_event_state_keys" +
		" WHERE event_state_key = ANY($1)"

	// bulkSelectEventStateKeyNIDSQL selects event state keys from an array of event_state_key_nids
	bulkSelectEventStateKeyNIDSQL = "" +
		"SELECT event_state_key_nid, event_state_key FROM roomserver_event_state_keys" +
		" WHERE event_state_key_nid = ANY($1)"
)

type eventStateKeysStatements struct {
	cm *sqlutil.Connections

	// SQL statements stored as struct fields
	insertEventStateKeyStmt        string
	selectEventStateKeyStmt        string
	bulkSelectEventStateKeyStmt    string
	bulkSelectEventStateKeyNIDStmt string
}

// NewPostgresEventStateKeysTable creates a new PostgreSQL event state keys table and prepares all statements
func NewPostgresEventStateKeysTable(ctx context.Context, cm *sqlutil.Connections) (tables.EventStateKeys, error) {
	// Create the table first
	if err := cm.Writer.ExecSQL(ctx, eventStateKeysSchema); err != nil {
		return nil, err
	}

	// Initialize and return the statements
	s := &eventStateKeysStatements{
		cm: cm,

		// Initialize SQL statement fields with the constants
		insertEventStateKeyStmt:        insertEventStateKeySQL,
		selectEventStateKeyStmt:        selectEventStateKeySQL,
		bulkSelectEventStateKeyStmt:    bulkSelectEventStateKeySQL,
		bulkSelectEventStateKeyNIDStmt: bulkSelectEventStateKeyNIDSQL,
	}

	return s, nil
}

// InsertEventStateKey implements tables.EventStateKeys
func (s *eventStateKeysStatements) InsertEventStateKey(
	ctx context.Context, key string,
) (types.EventStateKeyNID, error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)

	// Execute the insertion
	var eventStateKeyNID int64
	row := db.Raw(s.insertEventStateKeyStmt, key).Row()
	err := row.Scan(&eventStateKeyNID)
	if frame.DBErrorIsRecordNotFound(err) {
		// We didn't insert a new event state key so we need to look up what
		// the existing NID is.
		row = db.Raw(s.selectEventStateKeyStmt, key).Row()
		err = row.Scan(&eventStateKeyNID)
	}
	return types.EventStateKeyNID(eventStateKeyNID), err
}

// SelectEventStateKeyNID implements tables.EventStateKeys
func (s *eventStateKeysStatements) SelectEventStateKeyNID(
	ctx context.Context, key string,
) (types.EventStateKeyNID, error) {
	// Get database connection
	var eventStateKeyNID int64
	var err error

	// Use a new connection since we're not in a transaction.
	db := s.cm.Connection(ctx, true)
	err = db.Raw(s.selectEventStateKeyStmt, key).Row().Scan(&eventStateKeyNID)
	return types.EventStateKeyNID(eventStateKeyNID), err
}

// BulkSelectEventStateKeyNID implements tables.EventStateKeys
func (s *eventStateKeysStatements) BulkSelectEventStateKeyNID(
	ctx context.Context, keys []string,
) (map[string]types.EventStateKeyNID, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// Get database connection
	var err error
	var rows *sql.Rows

	// Use a new connection since we're not in a transaction.
	db := s.cm.Connection(ctx, true)
	rows, err = db.Raw(s.bulkSelectEventStateKeyStmt, pq.StringArray(keys)).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "BulkSelectEventStateKeyNID: rows.close() failed")

	// Create result map
	result := make(map[string]types.EventStateKeyNID, len(keys))
	var eventStateKey string
	var eventStateKeyNID int64

	// Process rows
	for rows.Next() {
		if err = rows.Scan(&eventStateKey, &eventStateKeyNID); err != nil {
			return nil, err
		}
		result[eventStateKey] = types.EventStateKeyNID(eventStateKeyNID)
	}

	return result, rows.Err()
}

// BulkSelectEventStateKey implements tables.EventStateKeys
func (s *eventStateKeysStatements) BulkSelectEventStateKey(
	ctx context.Context, nids []types.EventStateKeyNID,
) (map[types.EventStateKeyNID]string, error) {
	if len(nids) == 0 {
		return nil, nil
	}

	// Convert nids to int64 array
	stateKeyNIDs := make([]int64, len(nids))
	for i := range nids {
		stateKeyNIDs[i] = int64(nids[i])
	}

	// Get database connection
	var err error
	var rows *sql.Rows

	// Use a new connection since we're not in a transaction.
	db := s.cm.Connection(ctx, true)
	rows, err = db.Raw(s.bulkSelectEventStateKeyNIDStmt, pq.Int64Array(stateKeyNIDs)).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "BulkSelectEventStateKey: rows.close() failed")

	// Create result map and process rows
	result := make(map[types.EventStateKeyNID]string, len(nids))
	var eventStateKeyNID int64
	var eventStateKey string

	for rows.Next() {
		if err = rows.Scan(&eventStateKeyNID, &eventStateKey); err != nil {
			return nil, err
		}
		result[types.EventStateKeyNID(eventStateKeyNID)] = eventStateKey
	}

	return result, rows.Err()
}
