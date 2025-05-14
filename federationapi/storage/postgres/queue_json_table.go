// Copyright 2025 Ant Investor Ltd.
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

	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
)

// Schema for the queue JSON table
const queueJSONSchema = `
-- The federationsender_queue_json table contains event contents that
-- we failed to send. 
CREATE TABLE IF NOT EXISTS federationsender_queue_json (
	-- The JSON NID. This allows the federationsender_queue_retry table to
	-- cross-reference to find the JSON blob.
	json_nid BIGSERIAL,
	-- The JSON body. Text so that we preserve UTF-8.
	json_body TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS federationsender_queue_json_json_nid_idx
    ON federationsender_queue_json (json_nid);
`

// Schema revert for the queue JSON table
const queueJSONSchemaRevert = `
DROP TABLE IF EXISTS federationsender_queue_json;
`

// SQL for inserting JSON
const insertJSONSQL = "" +
	"INSERT INTO federationsender_queue_json (json_body)" +
	" VALUES ($1)" +
	" RETURNING json_nid"

// SQL for deleting JSON
const deleteJSONSQL = "" +
	"DELETE FROM federationsender_queue_json WHERE json_nid = ANY($1)"

// SQL for selecting JSON
const selectJSONSQL = "" +
	"SELECT json_nid, json_body FROM federationsender_queue_json" +
	" WHERE json_nid = ANY($1)"

// queueJSONTable stores JSON for federation queue
type queueJSONTable struct {
	cm sqlutil.ConnectionManager
	// SQL query string fields, initialise at construction
	insertJSONSQL string
	deleteJSONSQL string
	selectJSONSQL string
}

// NewPostgresQueueJSONTable creates a new postgres queue JSON table
func NewPostgresQueueJSONTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.FederationQueueJSON, error) {
	s := &queueJSONTable{
		cm:            cm,
		insertJSONSQL: insertJSONSQL,
		deleteJSONSQL: deleteJSONSQL,
		selectJSONSQL: selectJSONSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "federationapi_queue_json_table_schema_001",
		Patch:       queueJSONSchema,
		RevertPatch: queueJSONSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertQueueJSON inserts JSON into the queue
func (s *queueJSONTable) InsertQueueJSON(ctx context.Context, json string) (int64, error) {
	var lastid int64

	db := s.cm.Connection(ctx, false)
	row := db.Raw(s.insertJSONSQL, json).Row()
	if err := row.Scan(&lastid); err != nil {
		return 0, err
	}
	return lastid, nil
}

// DeleteQueueJSON deletes JSON from the queue
func (s *queueJSONTable) DeleteQueueJSON(ctx context.Context, nids []int64) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteJSONSQL, pq.Int64Array(nids)).Error
}

// SelectQueueJSON gets JSON from the queue
func (s *queueJSONTable) SelectQueueJSON(ctx context.Context, jsonNIDs []int64) (map[int64][]byte, error) {
	blobs := map[int64][]byte{}

	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectJSONSQL, pq.Int64Array(jsonNIDs)).Rows()
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
