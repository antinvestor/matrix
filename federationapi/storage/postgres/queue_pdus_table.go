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
	"github.com/antinvestor/matrix/internal"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
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

const queuePDUsSchemaRevert = `DROP TABLE IF EXISTS federationsender_queue_pdus;`

const insertQueuePDUSQL = "" +
	"INSERT INTO federationsender_queue_pdus (transaction_id, server_name, json_nid)" +
	" VALUES ($1, $2, $3)"

const deleteQueuePDUSQL = "" +
	"DELETE FROM federationsender_queue_pdus WHERE server_name = $1 AND json_nid = ANY($2)"

const selectQueuePDUsSQL = "" +
	"SELECT json_nid FROM federationsender_queue_pdus" +
	" WHERE server_name = $1" +
	" LIMIT $2"

const selectQueuePDUReferenceJSONCountSQL = "" +
	"SELECT COUNT(*) FROM federationsender_queue_pdus" +
	" WHERE json_nid = $1"

const selectQueuePDUServerNamesSQL = "" +
	"SELECT DISTINCT server_name FROM federationsender_queue_pdus"

type queuePDUsTable struct {
	cm                      *sqlutil.Connections
	InsertSQL               string
	DeleteSQL               string
	SelectSQL               string
	SelectReferenceCountSQL string
	SelectServerNamesSQL    string
}

// NewPostgresQueuePDUsTable initializes a queuePDUsTable with SQL constants and a connection manager
func NewPostgresQueuePDUsTable(cm *sqlutil.Connections) tables.FederationQueuePDUs {
	return &queuePDUsTable{
		cm:                      cm,
		InsertSQL:               insertQueuePDUSQL,
		DeleteSQL:               deleteQueuePDUSQL,
		SelectSQL:               selectQueuePDUsSQL,
		SelectReferenceCountSQL: selectQueuePDUReferenceJSONCountSQL,
		SelectServerNamesSQL:    selectQueuePDUServerNamesSQL,
	}
}

func (t *queuePDUsTable) InsertQueuePDU(ctx context.Context, transactionID gomatrixserverlib.TransactionID, serverName spec.ServerName, nid int64) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.InsertSQL, transactionID, serverName, nid)
	return result.Error
}

func (t *queuePDUsTable) DeleteQueuePDUs(ctx context.Context, serverName spec.ServerName, jsonNIDs []int64) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.DeleteSQL, serverName, pq.Int64Array(jsonNIDs))
	return result.Error
}

func (t *queuePDUsTable) SelectQueuePDUReferenceJSONCount(ctx context.Context, jsonNID int64) (int64, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.SelectReferenceCountSQL, jsonNID).Row()
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (t *queuePDUsTable) SelectQueuePDUs(ctx context.Context, serverName spec.ServerName, limit int) ([]int64, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.SelectSQL, serverName, limit).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")
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

func (t *queuePDUsTable) SelectQueuePDUServerNames(ctx context.Context) ([]spec.ServerName, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.SelectServerNamesSQL).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")
	var result []spec.ServerName
	for rows.Next() {
		var name spec.ServerName
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, nil
}
