// Copyright 2020 The Matrix.org Foundation C.I.C.
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

// SQL query string constants
const (
	// insertJSONSQL inserts a new JSON blob into the queue and returns its NID
	insertJSONSQL = "" +
		"INSERT INTO federationsender_queue_json (json_body)" +
		" VALUES ($1)" +
		" RETURNING json_nid"

	// deleteJSONSQL removes JSON blobs from the queue by their NIDs
	deleteJSONSQL = "" +
		"DELETE FROM federationsender_queue_json WHERE json_nid = ANY($1)"

	// selectJSONSQL retrieves JSON blobs from the queue by their NIDs
	selectJSONSQL = "" +
		"SELECT json_nid, json_body FROM federationsender_queue_json" +
		" WHERE json_nid = ANY($1)"
)

// queueJSONTable contains the postgres-specific implementation
type queueJSONTable struct {
	cm *sqlutil.Connections

	insertJSONStmt string
	deleteJSONStmt string
	selectJSONStmt string
}

// NewPostgresQueueJSONTable creates a new postgres queue JSON table and prepares all statements
func NewPostgresQueueJSONTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationQueueJSON, error) {
	// Initialize schema using GORM
	gormDB := cm.Connection(ctx, false)
	if err := gormDB.Exec(queueJSONSchema).Error; err != nil {
		return nil, err
	}

	s := &queueJSONTable{
		cm:             cm,
		insertJSONStmt: insertJSONSQL,
		deleteJSONStmt: deleteJSONSQL,
		selectJSONStmt: selectJSONSQL,
	}

	return s, nil
}

// InsertQueueJSON adds a new JSON blob to the queue and returns its NID
func (s *queueJSONTable) InsertQueueJSON(
	ctx context.Context, json string,
) (int64, error) {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	var nid int64
	row := db.Raw(s.insertJSONStmt, json).Row()
	if err := row.Scan(&nid); err != nil {
		return 0, err
	}
	return nid, nil
}

// DeleteQueueJSON removes JSON blobs from the queue by their NIDs
func (s *queueJSONTable) DeleteQueueJSON(
	ctx context.Context, nids []int64,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(s.deleteJSONStmt, pq.Array(nids)).Error
}

// SelectQueueJSON retrieves JSON blobs from the queue by their NIDs
func (s *queueJSONTable) SelectQueueJSON(
	ctx context.Context, jsonNIDs []int64,
) (map[int64][]byte, error) {
	// Get read-only database connection
	db := s.cm.Connection(ctx, true)

	stmt := s.selectJSONStmt
	rows, err := db.Raw(stmt, pq.Array(jsonNIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectJSON: rows.close() failed")

	result := make(map[int64][]byte)
	for rows.Next() {
		var nid int64
		var blob string
		if err = rows.Scan(&nid, &blob); err != nil {
			return nil, err
		}
		result[nid] = []byte(blob)
	}

	return result, nil
}
