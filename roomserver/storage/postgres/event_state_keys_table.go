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

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
)

// Schema for the event state keys table
const eventStateKeysSchema = `
-- Numeric versions of the event "state_key"s. State keys tend to be reused so
-- assigning each string a numeric ID should reduce the amount of data that
-- needs to be stored and fetched from the database.
-- It also means that many operations can work with int64 arrays rather than
-- string arrays which may help reduce GC pressure.
-- Well known state keys are pre-assigned numeric IDs:
--   1 -> "" (the empty string)
-- Other state keys are automatically assigned numeric IDs starting from 2**16.
-- This leaves room to add more pre-assigned numeric IDs and clearly separates
-- the automatically assigned IDs from the pre-assigned IDs.
CREATE SEQUENCE IF NOT EXISTS roomserver_event_state_key_nid_seq START 65536;
CREATE TABLE IF NOT EXISTS roomserver_event_state_keys (
    -- Local numeric ID for the state key.
    event_state_key_nid BIGINT PRIMARY KEY DEFAULT nextval('roomserver_event_state_key_nid_seq'),
    event_state_key TEXT NOT NULL CONSTRAINT roomserver_event_state_key_unique UNIQUE
);
INSERT INTO roomserver_event_state_keys (event_state_key_nid, event_state_key) VALUES
    (1, '') ON CONFLICT DO NOTHING;
`

// Schema revert script for migration purposes
const eventStateKeysSchemaRevert = `DROP TABLE IF EXISTS roomserver_event_state_keys;`

// SQL to insert a new event state key and return its numeric ID
const insertEventStateKeyNIDSQL = "" +
	"INSERT INTO roomserver_event_state_keys (event_state_key) VALUES ($1)" +
	" ON CONFLICT ON CONSTRAINT roomserver_event_state_key_unique" +
	" DO NOTHING RETURNING (event_state_key_nid)"

// SQL to select a numeric ID for an event state key
const selectEventStateKeyNIDSQL = "" +
	"SELECT event_state_key_nid FROM roomserver_event_state_keys" +
	" WHERE event_state_key = $1"

// SQL to bulk select numeric IDs for event state keys
// Takes an array of strings as the query parameter.
const bulkSelectEventStateKeyNIDSQL = "" +
	"SELECT event_state_key, event_state_key_nid FROM roomserver_event_state_keys" +
	" WHERE event_state_key = ANY($1)"

// SQL to bulk select event state keys for numeric IDs
// Takes an array of numeric IDs as the query parameter.
const bulkSelectEventStateKeySQL = "" +
	"SELECT event_state_key, event_state_key_nid FROM roomserver_event_state_keys" +
	" WHERE event_state_key_nid = ANY($1)"

// eventStateKeysTable implements the tables.EventStateKeys interface using GORM
type eventStateKeysTable struct {
	cm sqlutil.ConnectionManager

	// SQL query strings loaded from constants
	insertEventStateKeyNIDSQL     string
	selectEventStateKeyNIDSQL     string
	bulkSelectEventStateKeyNIDSQL string
	bulkSelectEventStateKeySQL    string
}

// NewPostgresEventStateKeysTable creates a new event state keys table
func NewPostgresEventStateKeysTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.EventStateKeys, error) {
	// Create the table if it doesn't exist using migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "roomserver_event_state_keys_table_schema_001",
		Patch:       eventStateKeysSchema,
		RevertPatch: eventStateKeysSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Initialize the table struct with just the connection manager
	t := &eventStateKeysTable{
		cm: cm,

		// Initialize SQL query strings from constants
		insertEventStateKeyNIDSQL:     insertEventStateKeyNIDSQL,
		selectEventStateKeyNIDSQL:     selectEventStateKeyNIDSQL,
		bulkSelectEventStateKeyNIDSQL: bulkSelectEventStateKeyNIDSQL,
		bulkSelectEventStateKeySQL:    bulkSelectEventStateKeySQL,
	}

	return t, nil
}

func (t *eventStateKeysTable) InsertEventStateKeyNID(
	ctx context.Context, eventStateKey string,
) (types.EventStateKeyNID, error) {
	db := t.cm.Connection(ctx, false)

	var eventStateKeyNID int64
	row := db.Raw(t.insertEventStateKeyNIDSQL, eventStateKey).Row()
	err := row.Scan(&eventStateKeyNID)
	return types.EventStateKeyNID(eventStateKeyNID), err
}

func (t *eventStateKeysTable) SelectEventStateKeyNID(
	ctx context.Context, eventStateKey string,
) (types.EventStateKeyNID, error) {
	db := t.cm.Connection(ctx, true)

	var eventStateKeyNID int64
	row := db.Raw(t.selectEventStateKeyNIDSQL, eventStateKey).Row()
	err := row.Scan(&eventStateKeyNID)
	return types.EventStateKeyNID(eventStateKeyNID), err
}

func (t *eventStateKeysTable) BulkSelectEventStateKeyNID(
	ctx context.Context, eventStateKeys []string,
) (map[string]types.EventStateKeyNID, error) {
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(
		t.bulkSelectEventStateKeyNIDSQL, pq.StringArray(eventStateKeys),
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectEventStateKeyNID: rows.close() failed")

	result := make(map[string]types.EventStateKeyNID, len(eventStateKeys))
	var stateKey string
	var stateKeyNID int64
	for rows.Next() {
		if err := rows.Scan(&stateKey, &stateKeyNID); err != nil {
			return nil, err
		}
		result[stateKey] = types.EventStateKeyNID(stateKeyNID)
	}
	return result, rows.Err()
}

func (t *eventStateKeysTable) BulkSelectEventStateKey(
	ctx context.Context, eventStateKeyNIDs []types.EventStateKeyNID,
) (map[types.EventStateKeyNID]string, error) {
	db := t.cm.Connection(ctx, true)

	nIDs := make(pq.Int64Array, len(eventStateKeyNIDs))
	for i := range eventStateKeyNIDs {
		nIDs[i] = int64(eventStateKeyNIDs[i])
	}

	rows, err := db.Raw(t.bulkSelectEventStateKeySQL, nIDs).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectEventStateKey: rows.close() failed")

	result := make(map[types.EventStateKeyNID]string, len(eventStateKeyNIDs))
	var stateKey string
	var stateKeyNID int64
	for rows.Next() {
		if err := rows.Scan(&stateKey, &stateKeyNID); err != nil {
			return nil, err
		}
		result[types.EventStateKeyNID(stateKeyNID)] = stateKey
	}
	return result, rows.Err()
}
