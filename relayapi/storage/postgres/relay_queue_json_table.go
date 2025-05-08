// Copyright 2022 The Matrix.org Foundation C.I.C.
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
	"gorm.io/gorm"
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

// SQL query constants for relay queue JSON operations
const (
	// insertQueueJSONSQL inserts a new JSON blob and returns its ID
	insertQueueJSONSQL = `
	INSERT INTO relayapi_queue_json (json_body)
	VALUES ($1)
	RETURNING json_nid
	`

	// deleteQueueJSONSQL removes JSON blobs by their IDs
	deleteQueueJSONSQL = `
	DELETE FROM relayapi_queue_json WHERE json_nid = ANY($1)
	`

	// selectQueueJSONSQL retrieves JSON blobs by their IDs
	selectQueueJSONSQL = `
	SELECT json_nid, json_body FROM relayapi_queue_json
	WHERE json_nid = ANY($1)
	`
)

// relayQueueJSONStatements contains the PostgreSQL statements for interacting with relay queue JSON
type relayQueueJSONStatements struct {
	insertJSONStmt string
	deleteJSONStmt string
	selectJSONStmt string
}

// postgresRelayQueueJSONTable implements the tables.RelayQueueJSON interface
type postgresRelayQueueJSONTable struct {
	cm         *sqlutil.Connections
	statements relayQueueJSONStatements
}

// NewPostgresRelayQueueJSONTable creates a new PostgreSQL-backed relay queue JSON table
func NewPostgresRelayQueueJSONTable(ctx context.Context, cm *sqlutil.Connections) (tables.RelayQueueJSON, error) {
	// Initialize schema using GORM
	gormDB := cm.Connection(ctx, false)
	if err := gormDB.Exec(relayQueueJSONSchema).Error; err != nil {
		return nil, err
	}

	// Initialize statements
	s := &postgresRelayQueueJSONTable{
		cm: cm,
		statements: relayQueueJSONStatements{
			insertJSONStmt: insertQueueJSONSQL,
			deleteJSONStmt: deleteQueueJSONSQL,
			selectJSONStmt: selectQueueJSONSQL,
		},
	}

	return s, nil
}

// InsertQueueJSON adds a JSON blob to the relay queue
func (s *postgresRelayQueueJSONTable) InsertQueueJSON(
	ctx context.Context, json string,
) (int64, error) {
	db := s.cm.Connection(ctx, false)

	var lastid int64
	row := db.Raw(s.statements.insertJSONStmt, json).Row()
	if err := row.Scan(&lastid); err != nil {
		return 0, err
	}
	return lastid, nil
}

// DeleteQueueJSON removes JSON blobs from the relay queue
func (s *postgresRelayQueueJSONTable) DeleteQueueJSON(
	ctx context.Context, nids []int64,
) error {
	db := s.cm.Connection(ctx, false)

	return db.Exec(s.statements.deleteJSONStmt, pq.Int64Array(nids)).Error
}

// SelectQueueJSON retrieves JSON blobs from the relay queue
func (s *postgresRelayQueueJSONTable) SelectQueueJSON(
	ctx context.Context, jsonNIDs []int64,
) (map[int64][]byte, error) {
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(s.statements.selectJSONStmt, pq.Int64Array(jsonNIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectJSON: rows.close() failed")

	blobs := map[int64][]byte{}
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
