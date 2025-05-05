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
	"github.com/antinvestor/matrix/relayapi/storage/tables"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
)

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

const relayQueueJSONSchemaRevert = `DROP TABLE IF EXISTS relayapi_queue_json;`

const insertQueueJSONSQL = "" +
	"INSERT INTO relayapi_queue_json (json_body)" +
	" VALUES ($1)" +
	" RETURNING json_nid"

const deleteQueueJSONSQL = "" +
	"DELETE FROM relayapi_queue_json WHERE json_nid = ANY($1)"

const selectQueueJSONSQL = "" +
	"SELECT json_nid, json_body FROM relayapi_queue_json" +
	" WHERE json_nid = ANY($1)"

// Refactored relayQueueJSONStatements to use connection manager and GORM-style logic, removing *sql.Tx from all methods but preserving function names and signatures as per the updated table interface.
type relayQueueJSONStatements struct {
	cm        *sqlutil.Connections
	InsertSQL string
	DeleteSQL string
	SelectSQL string
}

func NewPostgresRelayQueueJSONTable(cm *sqlutil.Connections) tables.RelayQueueJSON {
	return &relayQueueJSONStatements{
		cm:        cm,
		InsertSQL: insertQueueJSONSQL,
		DeleteSQL: deleteQueueJSONSQL,
		SelectSQL: selectQueueJSONSQL,
	}
}

func (s *relayQueueJSONStatements) InsertQueueJSON(ctx context.Context, json string) (int64, error) {
	db := s.cm.Connection(ctx, false)
	var lastid int64
	row := db.Raw(s.InsertSQL, json).Row()
	if err := row.Scan(&lastid); err != nil {
		return 0, err
	}
	return lastid, nil
}

func (s *relayQueueJSONStatements) DeleteQueueJSON(ctx context.Context, nids []int64) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.DeleteSQL, pq.Int64Array(nids)).Error
}

func (s *relayQueueJSONStatements) SelectQueueJSON(ctx context.Context, jsonNIDs []int64) (map[int64][]byte, error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.SelectSQL, pq.Int64Array(jsonNIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	blobs := map[int64][]byte{}
	for rows.Next() {
		var nid int64
		var body []byte
		if err := rows.Scan(&nid, &body); err != nil {
			return nil, err
		}
		blobs[nid] = body
	}
	return blobs, rows.Err()
}
