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

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
)

// Schema for the queue PDUs table
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

// Schema revert for the queue PDUs table
const queuePDUsSchemaRevert = `
DROP TABLE IF EXISTS federationsender_queue_pdus;
`

// SQL for inserting a queue PDU
const insertQueuePDUSQL = "" +
	"INSERT INTO federationsender_queue_pdus (transaction_id, server_name, json_nid)" +
	" VALUES ($1, $2, $3)"

// SQL for deleting queue PDUs
const deleteQueuePDUSQL = "" +
	"DELETE FROM federationsender_queue_pdus WHERE server_name = $1 AND json_nid = ANY($2)"

// SQL for selecting queue PDUs
const selectQueuePDUsSQL = "" +
	"SELECT json_nid FROM federationsender_queue_pdus" +
	" WHERE server_name = $1" +
	" LIMIT $2"

// SQL for selecting queue PDU reference JSON count
const selectQueuePDUReferenceJSONCountSQL = "" +
	"SELECT COUNT(*) FROM federationsender_queue_pdus" +
	" WHERE json_nid = $1"

// SQL for selecting queue PDU server names
const selectQueuePDUServerNamesSQL = "" +
	"SELECT DISTINCT server_name FROM federationsender_queue_pdus"

// queuePDUTable stores PDUs for sending to other servers
type queuePDUTable struct {
	cm sqlutil.ConnectionManager
	// SQL query string fields, initialise at construction
	insertQueuePDUSQL                   string
	deleteQueuePDUSQL                   string
	selectQueuePDUsSQL                  string
	selectQueuePDUReferenceJSONCountSQL string
	selectQueuePDUServerNamesSQL        string
}

// NewPostgresQueuePDUsTable creates a new postgres queue PDUs table
func NewPostgresQueuePDUsTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.FederationQueuePDUs, error) {
	s := &queuePDUTable{
		cm:                                  cm,
		insertQueuePDUSQL:                   insertQueuePDUSQL,
		deleteQueuePDUSQL:                   deleteQueuePDUSQL,
		selectQueuePDUsSQL:                  selectQueuePDUsSQL,
		selectQueuePDUReferenceJSONCountSQL: selectQueuePDUReferenceJSONCountSQL,
		selectQueuePDUServerNamesSQL:        selectQueuePDUServerNamesSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "federationapi_queue_pdus_table_schema_001",
		Patch:       queuePDUsSchema,
		RevertPatch: queuePDUsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertQueuePDU inserts a PDU into the queue
func (s *queuePDUTable) InsertQueuePDU(
	ctx context.Context,
	transactionID gomatrixserverlib.TransactionID,
	serverName spec.ServerName,
	nid int64,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(
		s.insertQueuePDUSQL,
		transactionID, // the transaction ID that we initially attempted
		serverName,    // destination server name
		nid,           // JSON blob NID
	).Error
}

// DeleteQueuePDUs deletes PDUs from the queue
func (s *queuePDUTable) DeleteQueuePDUs(
	ctx context.Context,
	serverName spec.ServerName,
	jsonNIDs []int64,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteQueuePDUSQL, serverName, pq.Int64Array(jsonNIDs)).Error
}

// SelectQueuePDUReferenceJSONCount gets the count of PDUs referencing a JSON NID
func (s *queuePDUTable) SelectQueuePDUReferenceJSONCount(
	ctx context.Context, jsonNID int64,
) (int64, error) {
	var count int64

	db := s.cm.Connection(ctx, true)
	err := db.Raw(s.selectQueuePDUReferenceJSONCountSQL, jsonNID).Scan(&count).Error
	if err != nil {
		if sqlutil.ErrorIsNoRows(err) {
			// It's acceptable for there to be no rows referencing a given
			// JSON NID but it's not an error condition. Just return as if
			// there's a zero count.
			return 0, nil
		}
		return 0, err
	}
	return count, nil
}

// SelectQueuePDUs gets PDUs from the queue
func (s *queuePDUTable) SelectQueuePDUs(
	ctx context.Context,
	serverName spec.ServerName,
	limit int,
) ([]int64, error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectQueuePDUsSQL, serverName, limit).Rows()
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

// SelectQueuePDUServerNames gets all server names with PDUs in the queue
func (s *queuePDUTable) SelectQueuePDUServerNames(
	ctx context.Context,
) ([]spec.ServerName, error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectQueuePDUServerNamesSQL).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "queueFromStmt: rows.close() failed")

	var result []spec.ServerName
	for rows.Next() {
		var serverName spec.ServerName
		if err = rows.Scan(&serverName); err != nil {
			return nil, err
		}
		result = append(result, serverName)
	}

	return result, rows.Err()
}
