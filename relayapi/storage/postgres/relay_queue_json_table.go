// Copyright 2022 The Global.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/relayapi/storage/tables"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
)

// relayQueueJSONSchema defines the table structure for storing JSON event content
const relayQueueJSONSchema = `
-- The relayapi_queue_json table contains event contents that
-- we are storing for future forwarding. 
CREATE TABLE IF NOT EXISTS relayapi_queue_json (
	-- The JSON NID. This allows cross-referencing to find the JSON blob.
	json_nid BIGSERIAL,
	-- The JSON body. Text so that we preserve UTF-8.
	json_body TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS relayapi_queue_json_json_nid_idx
	ON relayapi_queue_json (json_nid);
`

// relayQueueJSONSchemaRevert defines SQL to revert the schema for migrations
const relayQueueJSONSchemaRevert = `
DROP TABLE IF EXISTS relayapi_queue_json;
`

// insertQueueJSONSQL inserts a new JSON blob and returns its NID
const insertQueueJSONSQL = "" +
	"INSERT INTO relayapi_queue_json (json_body)" +
	" VALUES ($1)" +
	" RETURNING json_nid"

// deleteQueueJSONSQL deletes JSON blobs by their NIDs
const deleteQueueJSONSQL = "" +
	"DELETE FROM relayapi_queue_json WHERE json_nid = ANY($1)"

// selectQueueJSONSQL retrieves JSON blobs by their NIDs
const selectQueueJSONSQL = "" +
	"SELECT json_nid, json_body FROM relayapi_queue_json" +
	" WHERE json_nid = ANY($1)"

// relayQueueJSONTable implements the tables.RelayQueueJSON interface
type relayQueueJSONTable struct {
	cm sqlutil.ConnectionManager

	// SQL queries stored as struct fields
	insertQueueJSONSQL string
	deleteQueueJSONSQL string
	selectQueueJSONSQL string
}

// NewPostgresRelayQueueJSONTable creates a new relay queue JSON table
func NewPostgresRelayQueueJSONTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.RelayQueueJSON, error) {
	t := &relayQueueJSONTable{
		cm: cm,

		// Initialise SQL query struct fields
		insertQueueJSONSQL: insertQueueJSONSQL,
		deleteQueueJSONSQL: deleteQueueJSONSQL,
		selectQueueJSONSQL: selectQueueJSONSQL,
	}

	// Migrate the table schema
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "relayapi_relay_queue_json_table_schema_001",
		Patch:       relayQueueJSONSchema,
		RevertPatch: relayQueueJSONSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (t *relayQueueJSONTable) InsertQueueJSON(
	ctx context.Context, json string,
) (int64, error) {
	db := t.cm.Connection(ctx, false) // Not read-only
	var lastid int64
	err := db.Raw(t.insertQueueJSONSQL, json).Row().Scan(&lastid)
	if err != nil {
		return 0, err
	}
	return lastid, nil
}

func (t *relayQueueJSONTable) DeleteQueueJSON(
	ctx context.Context, nids []int64,
) error {
	db := t.cm.Connection(ctx, false) // Not read-only
	return db.Exec(t.deleteQueueJSONSQL, pq.Int64Array(nids)).Error
}

func (t *relayQueueJSONTable) SelectQueueJSON(
	ctx context.Context, jsonNIDs []int64,
) (map[int64][]byte, error) {
	blobs := map[int64][]byte{}
	db := t.cm.Connection(ctx, true) // Read-only
	rows, err := db.Raw(t.selectQueueJSONSQL, pq.Int64Array(jsonNIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectJSON: rows.close() failed")
	for rows.Next() {
		var nid int64
		var blob []byte
		if err = rows.Scan(&nid, &blob); err != nil {
			return nil, err
		}
		blobs[nid] = blob
	}
	return blobs, rows.Err()
}
