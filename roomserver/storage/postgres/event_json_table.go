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
	"github.com/pitabwire/frame"
)

// Schema for the event JSON table
const eventJSONSchema = `
-- Stores the JSON for each event. This kept separate from the main events
-- table to keep the rows in the main events table small.
CREATE TABLE IF NOT EXISTS roomserver_event_json (
    -- Local numeric ID for the event.
    event_nid BIGINT NOT NULL PRIMARY KEY,
    -- The JSON for the event.
    -- Stored as TEXT because this should be valid UTF-8.
    -- Not stored as a JSONB because we always just pull the entire event
    -- so there is no point in postgres parsing it.
    -- Not stored as JSON because we already validate the JSON in the server
    -- so there is no point in postgres validating it.
    -- TODO: Should we be compressing the events with Snappy or DEFLATE?
    event_json TEXT NOT NULL
);
`

// Schema revert script for migration purposes
const eventJSONSchemaRevert = `DROP TABLE IF EXISTS roomserver_event_json;`

// SQL to insert an event JSON record
const insertEventJSONSQL = "" +
	"INSERT INTO roomserver_event_json (event_nid, event_json) VALUES ($1, $2)" +
	" ON CONFLICT (event_nid) DO UPDATE SET event_json=$2"

// SQL to bulk select event JSON by numeric event ID
// Sort by the numeric event ID.
// This means that we can use binary search to lookup by numeric event ID.
const bulkSelectEventJSONSQL = "" +
	"SELECT event_nid, event_json FROM roomserver_event_json" +
	" WHERE event_nid = ANY($1)" +
	" ORDER BY event_nid ASC"

// eventJSONTable implements the tables.EventJSON interface using GORM
type eventJSONTable struct {
	cm sqlutil.ConnectionManager

	// SQL query strings loaded from constants
	insertEventJSONSQL     string
	bulkSelectEventJSONSQL string
}

// NewPostgresEventJSONTable creates a new event JSON table
func NewPostgresEventJSONTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.EventJSON, error) {
	// Create the table if it doesn't exist using migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "roomserver_event_json_table_schema_001",
		Patch:       eventJSONSchema,
		RevertPatch: eventJSONSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Initialize the table struct with just the connection manager
	t := &eventJSONTable{
		cm: cm,

		// Initialize SQL query strings from constants
		insertEventJSONSQL:     insertEventJSONSQL,
		bulkSelectEventJSONSQL: bulkSelectEventJSONSQL,
	}

	return t, nil
}

func (t *eventJSONTable) InsertEventJSON(
	ctx context.Context, eventNID types.EventNID, eventJSON []byte,
) error {
	db := t.cm.Connection(ctx, false)

	result := db.Exec(t.insertEventJSONSQL, int64(eventNID), eventJSON)
	return result.Error
}

func (t *eventJSONTable) BulkSelectEventJSON(
	ctx context.Context, eventNIDs []types.EventNID,
) ([]tables.EventJSONPair, error) {
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.bulkSelectEventJSONSQL, eventNIDsAsArray(eventNIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectEventJSON: rows.close() failed")

	// We know that we will only get as many results as event NIDs
	// because of the unique constraint on event NIDs.
	// So we can allocate an array of the correct size now.
	// We might get fewer results than NIDs so we adjust the length of the slice before returning it.
	results := make([]tables.EventJSONPair, len(eventNIDs))
	i := 0
	var eventNID int64
	for ; rows.Next(); i++ {
		result := &results[i]
		if err := rows.Scan(&eventNID, &result.EventJSON); err != nil {
			return nil, err
		}
		result.EventNID = types.EventNID(eventNID)
	}
	return results[:i], rows.Err()
}
