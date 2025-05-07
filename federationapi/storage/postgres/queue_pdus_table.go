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
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
)

const queuePDUsSchema = `
CREATE TABLE IF NOT EXISTS federationsender_queue_pdus (
    -- The transaction ID that was generated before persisting the event.
	transaction_id TEXT NOT NULL,
    -- The destination server that we will send the event to.
	server_name TEXT NOT NULL,
	-- The JSON NID from the federationsender_queue_pdus_json table.
	json_nid BIGINT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS federationsender_queue_pdus_pdus_json_nid_idx
    ON federationsender_queue_pdus (json_nid, server_name);
CREATE INDEX IF NOT EXISTS federationsender_queue_pdus_json_nid_idx
    ON federationsender_queue_pdus (json_nid);
CREATE INDEX IF NOT EXISTS federationsender_queue_pdus_server_name_idx
    ON federationsender_queue_pdus (server_name);
`

// SQL query string constants
const (
	// insertQueuePDUSQL inserts a PDU into the queue for a server with a given transaction ID and JSON NID
	insertQueuePDUSQL = "" +
		"INSERT INTO federationsender_queue_pdus (transaction_id, server_name, json_nid)" +
		" VALUES ($1, $2, $3)"

	// deleteQueuePDUSQL removes PDUs from the queue for a server with specified JSON NIDs
	deleteQueuePDUSQL = "" +
		"DELETE FROM federationsender_queue_pdus WHERE server_name = $1 AND json_nid = ANY($2)"

	// selectQueuePDUsSQL retrieves PDU JSON NIDs from the queue for a server with a limit
	selectQueuePDUsSQL = "" +
		"SELECT json_nid FROM federationsender_queue_pdus" +
		" WHERE server_name = $1" +
		" LIMIT $2"

	// selectQueuePDUReferenceJSONCountSQL counts how many queue entries reference a specific JSON NID
	selectQueuePDUReferenceJSONCountSQL = "" +
		"SELECT COUNT(*) FROM federationsender_queue_pdus" +
		" WHERE json_nid = $1"

	// selectQueuePDUServerNamesSQL retrieves distinct server names that have PDUs queued
	selectQueuePDUServerNamesSQL = "" +
		"SELECT DISTINCT server_name FROM federationsender_queue_pdus"
)

// queuePDUsTable contains the postgres-specific implementation
type queuePDUsTable struct {
	cm *sqlutil.Connections

	insertQueuePDUStmt                   string
	deleteQueuePDUsStmt                  string
	selectQueuePDUsStmt                  string
	selectQueuePDUReferenceJSONCountStmt string
	selectQueuePDUServerNamesStmt        string
}

// NewPostgresQueuePDUsTable creates a new postgres queue PDUs table and prepares all statements
func NewPostgresQueuePDUsTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationQueuePDUs, error) {
	// Initialize schema using GORM
	gormDB := cm.Connection(ctx, false)
	if err := gormDB.Exec(queuePDUsSchema).Error; err != nil {
		return nil, err
	}

	s := &queuePDUsTable{
		cm:                                   cm,
		insertQueuePDUStmt:                   insertQueuePDUSQL,
		deleteQueuePDUsStmt:                  deleteQueuePDUSQL,
		selectQueuePDUsStmt:                  selectQueuePDUsSQL,
		selectQueuePDUReferenceJSONCountStmt: selectQueuePDUReferenceJSONCountSQL,
		selectQueuePDUServerNamesStmt:        selectQueuePDUServerNamesSQL,
	}

	return s, nil
}

// InsertQueuePDU adds a PDU to the queue
func (s *queuePDUsTable) InsertQueuePDU(
	ctx context.Context,
	transactionID gomatrixserverlib.TransactionID,
	serverName spec.ServerName,
	nid int64,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(
		s.insertQueuePDUStmt,
		transactionID, serverName, nid,
	).Error
}

// DeleteQueuePDUs removes PDUs from the queue for a server
func (s *queuePDUsTable) DeleteQueuePDUs(
	ctx context.Context,
	serverName spec.ServerName,
	jsonNIDs []int64,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(
		s.deleteQueuePDUsStmt,
		serverName,
		pq.Array(jsonNIDs),
	).Error
}

// SelectQueuePDUReferenceJSONCount returns the count of queue entries referencing a JSON NID
func (s *queuePDUsTable) SelectQueuePDUReferenceJSONCount(
	ctx context.Context, jsonNID int64,
) (int64, error) {
	// Get read-only database connection
	db := s.cm.Connection(ctx, true)

	var count int64
	err := db.Raw(
		s.selectQueuePDUReferenceJSONCountStmt,
		jsonNID,
	).Row().Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// SelectQueuePDUs retrieves PDUs from the queue for a server with a limit
func (s *queuePDUsTable) SelectQueuePDUs(
	ctx context.Context,
	serverName spec.ServerName,
	limit int,
) ([]int64, error) {
	// Get read-only database connection
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(
		s.selectQueuePDUsStmt,
		serverName,
		limit,
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectQueuePDUs: rows.close() failed")

	var result []int64
	for rows.Next() {
		var nid int64
		if err = rows.Scan(&nid); err != nil {
			return nil, err
		}
		result = append(result, nid)
	}

	return result, nil
}

// SelectQueuePDUServerNames retrieves all distinct server names with queued PDUs
func (s *queuePDUsTable) SelectQueuePDUServerNames(
	ctx context.Context,
) ([]spec.ServerName, error) {
	// Get read-only database connection
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(
		s.selectQueuePDUServerNamesStmt,
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectQueuePDUServerNames: rows.close() failed")

	var result []spec.ServerName
	for rows.Next() {
		var serverName string
		if err = rows.Scan(&serverName); err != nil {
			return nil, err
		}
		result = append(result, spec.ServerName(serverName))
	}

	return result, nil
}
