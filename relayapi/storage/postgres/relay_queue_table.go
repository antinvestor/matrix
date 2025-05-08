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
	"github.com/pitabwire/frame"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/relayapi/storage/tables"
	"github.com/lib/pq"
)

// SQL query definitions as package-level constants
const relayQueueSchema = `
CREATE TABLE IF NOT EXISTS relayapi_queue (
	-- The transaction ID that was generated before persisting the event.
	transaction_id TEXT NOT NULL,
	-- The destination server that we will send the event to.
	server_name TEXT NOT NULL,
	-- The JSON NID from the relayapi_queue_json table.
	json_nid BIGINT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS relayapi_queue_queue_json_nid_idx
	ON relayapi_queue (json_nid, server_name);
CREATE INDEX IF NOT EXISTS relayapi_queue_json_nid_idx
	ON relayapi_queue (json_nid);
CREATE INDEX IF NOT EXISTS relayapi_queue_server_name_idx
	ON relayapi_queue (server_name);
`

// Schema revert to use for migration revert if needed
const relayQueueSchemaRevert = `
DROP TABLE IF EXISTS relayapi_queue;
`

// insertQueueEntrySQL inserts a new entry into the queue
const insertQueueEntrySQL = "" +
	"INSERT INTO relayapi_queue (transaction_id, server_name, json_nid)" +
	" VALUES ($1, $2, $3)"

// deleteQueueEntriesSQL deletes queue entries for a server and set of JSON NIDs
const deleteQueueEntriesSQL = "" +
	"DELETE FROM relayapi_queue WHERE server_name = $1 AND json_nid = ANY($2)"

// selectQueueEntriesSQL retrieves queue entries for a server up to a limit
const selectQueueEntriesSQL = "" +
	"SELECT json_nid FROM relayapi_queue" +
	" WHERE server_name = $1" +
	" ORDER BY json_nid" +
	" LIMIT $2"

// selectQueueEntryCountSQL counts queue entries for a server
const selectQueueEntryCountSQL = "" +
	"SELECT COUNT(*) FROM relayapi_queue" +
	" WHERE server_name = $1"

// relayQueueTable implements the tables.RelayQueue interface
type relayQueueTable struct {
	cm *sqlutil.Connections

	// SQL queries stored as struct fields
	insertQueueEntrySQL      string
	deleteQueueEntriesSQL    string
	selectQueueEntriesSQL    string
	selectQueueEntryCountSQL string
}

// NewPostgresRelayQueueTable creates a new relay queue table
func NewPostgresRelayQueueTable(
	ctx context.Context, cm *sqlutil.Connections,
) (tables.RelayQueue, error) {
	t := &relayQueueTable{
		cm: cm,

		// Initialize SQL query struct fields
		insertQueueEntrySQL:      insertQueueEntrySQL,
		deleteQueueEntriesSQL:    deleteQueueEntriesSQL,
		selectQueueEntriesSQL:    selectQueueEntriesSQL,
		selectQueueEntryCountSQL: selectQueueEntryCountSQL,
	}

	// Migrate the table schema
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "relayapi_relay_queue_table_schema_001",
		Patch:       relayQueueSchema,
		RevertPatch: relayQueueSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (t *relayQueueTable) InsertQueueEntry(
	ctx context.Context,
	transactionID gomatrixserverlib.TransactionID,
	serverName spec.ServerName,
	nid int64,
) error {
	db := t.cm.Connection(ctx, false) // Not read-only
	return db.Exec(t.insertQueueEntrySQL, transactionID, serverName, nid).Error
}

func (t *relayQueueTable) DeleteQueueEntries(
	ctx context.Context,
	serverName spec.ServerName,
	jsonNIDs []int64,
) error {
	db := t.cm.Connection(ctx, false) // Not read-only
	return db.Exec(t.deleteQueueEntriesSQL, serverName, pq.Int64Array(jsonNIDs)).Error
}

func (t *relayQueueTable) SelectQueueEntries(
	ctx context.Context,
	serverName spec.ServerName,
	limit int,
) ([]int64, error) {
	db := t.cm.Connection(ctx, true) // Read-only
	rows, err := db.Raw(t.selectQueueEntriesSQL, serverName, limit).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "queueFromStmt: rows.close() failed")

	var result []int64
	for rows.Next() {
		var nid int64
		if err = rows.Scan(&nid); err != nil {
			return nil, err
		}
		result = append(result, nid)
	}

	return result, rows.Err()
}

func (t *relayQueueTable) SelectQueueEntryCount(
	ctx context.Context,
	serverName spec.ServerName,
) (int64, error) {
	var count int64
	db := t.cm.Connection(ctx, true) // Read-only
	err := db.Raw(t.selectQueueEntryCountSQL, serverName).Row().Scan(&count)
	if frame.DBErrorIsRecordNotFound(err) {
		// It's acceptable for there to be no rows referencing a given
		// JSON NID but it's not an error condition. Just return as if
		// there's a zero count.
		return 0, nil
	}
	return count, err
}
