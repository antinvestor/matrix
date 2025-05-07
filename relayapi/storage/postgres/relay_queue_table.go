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
	"errors"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/relayapi/storage/tables"
	"github.com/lib/pq"
	"gorm.io/gorm"
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

// SQL query constants for relay queue operations
const (
	// insertQueueEntrySQL inserts a new entry into the relay queue
	insertQueueEntrySQL = `
	INSERT INTO relayapi_queue (transaction_id, server_name, json_nid)
	VALUES ($1, $2, $3)
	`

	// deleteQueueEntriesSQL removes entries from the relay queue for a specific server and JSON NIDs
	deleteQueueEntriesSQL = `
	DELETE FROM relayapi_queue WHERE server_name = $1 AND json_nid = ANY($2)
	`

	// selectQueueEntriesSQL retrieves queue entries for a specific server with a limit
	selectQueueEntriesSQL = `
	SELECT json_nid FROM relayapi_queue
	WHERE server_name = $1
	ORDER BY json_nid
	LIMIT $2
	`

	// selectQueueEntryCountSQL counts the number of queue entries for a specific server
	selectQueueEntryCountSQL = `
	SELECT COUNT(*) FROM relayapi_queue
	WHERE server_name = $1
	`
)

// relayQueueStatements contains the PostgreSQL statements for interacting with the relay queue
type relayQueueStatements struct {
	insertQueueEntryStmt      string
	deleteQueueEntriesStmt    string
	selectQueueEntriesStmt    string
	selectQueueEntryCountStmt string
}

// postgresRelayQueueTable implements the tables.RelayQueue interface
type postgresRelayQueueTable struct {
	cm         *sqlutil.Connections
	statements relayQueueStatements
}

// NewPostgresRelayQueueTable creates a new PostgreSQL-backed relay queue table
func NewPostgresRelayQueueTable(
	ctx context.Context, cm *sqlutil.Connections,
) (tables.RelayQueue, error) {
	// Initialize schema using GORM
	gormDB := cm.Connection(ctx, false)
	if err := gormDB.Exec(relayQueueSchema).Error; err != nil {
		return nil, err
	}

	// Initialize statements
	s := &postgresRelayQueueTable{
		cm: cm,
		statements: relayQueueStatements{
			insertQueueEntryStmt:      insertQueueEntrySQL,
			deleteQueueEntriesStmt:    deleteQueueEntriesSQL,
			selectQueueEntriesStmt:    selectQueueEntriesSQL,
			selectQueueEntryCountStmt: selectQueueEntryCountSQL,
		},
	}

	return s, nil
}

// InsertQueueEntry adds an entry to the relay queue
func (s *postgresRelayQueueTable) InsertQueueEntry(
	ctx context.Context,
	txn *gorm.DB,
	transactionID gomatrixserverlib.TransactionID,
	serverName spec.ServerName,
	nid int64,
) error {
	// Use provided transaction or create a new one
	db := txn
	if db == nil {
		db = s.cm.Connection(ctx, false)
	}

	return db.Exec(
		s.statements.insertQueueEntryStmt,
		transactionID, // the transaction ID that we initially attempted
		serverName,    // destination server name
		nid,           // JSON blob NID
	).Error
}

// DeleteQueueEntries removes entries from the relay queue
func (s *postgresRelayQueueTable) DeleteQueueEntries(
	ctx context.Context,
	txn *gorm.DB,
	serverName spec.ServerName,
	jsonNIDs []int64,
) error {
	// Use provided transaction or create a new one
	db := txn
	if db == nil {
		db = s.cm.Connection(ctx, false)
	}

	return db.Exec(
		s.statements.deleteQueueEntriesStmt,
		serverName,
		pq.Int64Array(jsonNIDs),
	).Error
}

// SelectQueueEntries retrieves entries from the relay queue
func (s *postgresRelayQueueTable) SelectQueueEntries(
	ctx context.Context,
	txn *gorm.DB,
	serverName spec.ServerName,
	limit int,
) ([]int64, error) {
	// Use provided transaction or create a new one
	db := txn
	if db == nil {
		db = s.cm.Connection(ctx, true)
	}

	rows, err := db.Raw(
		s.statements.selectQueueEntriesStmt,
		serverName,
		limit,
	).Rows()

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

// SelectQueueEntryCount counts the number of entries in the relay queue
func (s *postgresRelayQueueTable) SelectQueueEntryCount(
	ctx context.Context,
	txn *gorm.DB,
	serverName spec.ServerName,
) (int64, error) {
	// Use provided transaction or create a new one
	db := txn
	if db == nil {
		db = s.cm.Connection(ctx, true)
	}

	var count int64
	row := db.Raw(
		s.statements.selectQueueEntryCountStmt,
		serverName,
	).Row()

	err := row.Scan(&count)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// It's acceptable for there to be no rows referencing a given
		// JSON NID but it's not an error condition. Just return as if
		// there's a zero count.
		return 0, nil
	}
	
	return count, err
}
