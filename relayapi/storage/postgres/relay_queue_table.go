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
	"database/sql"
	"errors"
	"github.com/antinvestor/matrix/internal"

	"github.com/antinvestor/matrix/relayapi/storage/tables"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
)

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

const relayQueueSchemaRevert = `DROP TABLE IF EXISTS relayapi_queue;`

const insertQueueEntrySQL = "" +
	"INSERT INTO relayapi_queue (transaction_id, server_name, json_nid)" +
	" VALUES ($1, $2, $3)"

const deleteQueueEntriesSQL = "" +
	"DELETE FROM relayapi_queue WHERE server_name = $1 AND json_nid = ANY($2)"

const selectQueueEntriesSQL = "" +
	"SELECT json_nid FROM relayapi_queue" +
	" WHERE server_name = $1" +
	" ORDER BY json_nid" +
	" LIMIT $2"

const selectQueueEntryCountSQL = "" +
	"SELECT COUNT(*) FROM relayapi_queue" +
	" WHERE server_name = $1"

// relayQueueStatements implements the RelayQueue table for mapping server name/transaction ID to JSON NID.
// Methods ensure correct handling of duplicates, deletion, and empty result sets as described in the interface.
type relayQueueStatements struct {
	cm             *sqlutil.Connections
	InsertSQL      string
	DeleteSQL      string
	SelectSQL      string
	SelectCountSQL string
}

// NewPostgresRelayQueueTable creates a new RelayQueueStatements using the provided connection manager.
func NewPostgresRelayQueueTable(cm *sqlutil.Connections) tables.RelayQueue {
	return &relayQueueStatements{
		cm:             cm,
		InsertSQL:      insertQueueEntrySQL,
		DeleteSQL:      deleteQueueEntriesSQL,
		SelectSQL:      selectQueueEntriesSQL,
		SelectCountSQL: selectQueueEntryCountSQL,
	}
}

// InsertQueueEntry adds a new transaction_id:server_name mapping with associated json table nid to the table.
// Will ensure only one transaction id is present for each server_name:nid mapping. Adding duplicates will silently do nothing.
func (s *relayQueueStatements) InsertQueueEntry(
	ctx context.Context,
	transactionID gomatrixserverlib.TransactionID,
	serverName spec.ServerName,
	nid int64,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.InsertSQL, transactionID, serverName, nid).Error
}

// DeleteQueueEntries removes multiple entries from the table corresponding to the list of nids provided.
// If any of the provided nids don't match a row in the table, that deletion is considered successful.
func (s *relayQueueStatements) DeleteQueueEntries(
	ctx context.Context,
	serverName spec.ServerName,
	jsonNIDs []int64,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.DeleteSQL, serverName, pq.Int64Array(jsonNIDs)).Error
}

// SelectQueueEntries gets a list of nids associated with the provided server name.
// Returns up to `limit` nids, oldest first. Will return an empty slice if no matches were found.
func (s *relayQueueStatements) SelectQueueEntries(
	ctx context.Context,
	serverName spec.ServerName,
	limit int,
) ([]int64, error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.SelectSQL, serverName, limit).Rows()
	if err != nil {
		return []int64{}, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")
	var nids []int64
	for rows.Next() {
		var nid int64
		if err := rows.Scan(&nid); err != nil {
			return []int64{}, err
		}
		nids = append(nids, nid)
	}
	if len(nids) == 0 {
		return []int64{}, nil
	}
	return nids, rows.Err()
}

// SelectQueueEntryCount gets the number of entries in the table associated with the provided server name.
// If there are no matching rows, a count of 0 is returned with err set to nil.
func (s *relayQueueStatements) SelectQueueEntryCount(
	ctx context.Context,
	serverName spec.ServerName,
) (int64, error) {
	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.SelectCountSQL, serverName).Row()
	var count int64
	err := row.Scan(&count)
	if errors.Is(err, sql.ErrNoRows) {
		// It's acceptable for there to be no rows referencing a given server name.
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return count, nil
}
