// Copyright 2020 The Global.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
)

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

const queueJSONSchemaRevert = `DROP TABLE IF EXISTS federationsender_queue_json;`

// InsertJSONSQL inserts a JSON blob into the federationsender_queue_json table.
const insertJSONSQL = "" +
	"INSERT INTO federationsender_queue_json (json_body)" +
	" VALUES ($1)" +
	" RETURNING json_nid"

// DeleteJSONSQL deletes JSON blobs from the federationsender_queue_json table.
const deleteJSONSQL = "" +
	"DELETE FROM federationsender_queue_json WHERE json_nid = ANY($1)"

// SelectJSONSQL selects JSON blobs from the federationsender_queue_json table.
const selectJSONSQL = "" +
	"SELECT json_nid, json_body FROM federationsender_queue_json" +
	" WHERE json_nid = ANY($1)"

type queueJSONTable struct {
	cm        *sqlutil.Connections
	InsertSQL string
	DeleteSQL string
	SelectSQL string
}

// NewPostgresQueueJSONTable initializes a queueJSONTable with SQL constants and a connection manager
func NewPostgresQueueJSONTable(cm *sqlutil.Connections) tables.FederationQueueJSON {
	return &queueJSONTable{
		cm:        cm,
		InsertSQL: insertJSONSQL,
		DeleteSQL: deleteJSONSQL,
		SelectSQL: selectJSONSQL,
	}
}

func (t *queueJSONTable) InsertQueueJSON(ctx context.Context, json string) (int64, error) {
	db := t.cm.Connection(ctx, false)
	row := db.Raw(t.InsertSQL, json).Row()
	var lastid int64
	if err := row.Scan(&lastid); err != nil {
		return 0, err
	}
	return lastid, nil
}

func (t *queueJSONTable) DeleteQueueJSON(ctx context.Context, nids []int64) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.DeleteSQL, pq.Int64Array(nids))
	return result.Error
}

func (t *queueJSONTable) SelectQueueJSON(ctx context.Context, jsonNIDs []int64) (map[int64][]byte, error) {
	blobs := map[int64][]byte{}
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.SelectSQL, pq.Int64Array(jsonNIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var nid int64
		var jsonBody []byte
		if err := rows.Scan(&nid, &jsonBody); err != nil {
			return nil, err
		}
		blobs[nid] = jsonBody
	}
	return blobs, nil
}
