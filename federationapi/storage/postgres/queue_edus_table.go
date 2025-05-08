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
	"errors"

	"github.com/lib/pq"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/pitabwire/frame"
)

// Schema for the queue EDUs table
const queueEDUsSchema = `
CREATE TABLE IF NOT EXISTS federationsender_queue_edus (
	-- The type of the event (informational).
	edu_type TEXT NOT NULL,
    -- The domain part of the user ID the EDU event is for.
	server_name TEXT NOT NULL,
	-- The JSON NID from the federationsender_queue_edus_json table.
	json_nid BIGINT NOT NULL,
	-- The expiry time of this edu, if any.
	expires_at BIGINT NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS federationsender_queue_edus_json_nid_idx
    ON federationsender_queue_edus (json_nid, server_name);
CREATE INDEX IF NOT EXISTS federationsender_queue_edus_nid_idx
    ON federationsender_queue_edus (json_nid);
CREATE INDEX IF NOT EXISTS federationsender_queue_edus_server_name_idx
    ON federationsender_queue_edus (server_name);
`

// Schema revert for the queue EDUs table
const queueEDUsSchemaRevert = `
DROP TABLE IF EXISTS federationsender_queue_edus;
`

// SQL to insert a queue EDU
const insertQueueEDUSQL = "" +
	"INSERT INTO federationsender_queue_edus (edu_type, server_name, json_nid, expires_at)" +
	" VALUES ($1, $2, $3, $4)"

// SQL to delete queue EDUs
const deleteQueueEDUSQL = "" +
	"DELETE FROM federationsender_queue_edus WHERE server_name = $1 AND json_nid = ANY($2)"

// SQL to select queue EDUs
const selectQueueEDUSQL = "" +
	"SELECT json_nid FROM federationsender_queue_edus" +
	" WHERE server_name = $1" +
	" LIMIT $2"

// SQL to count queue EDU references
const selectQueueEDUReferenceJSONCountSQL = "" +
	"SELECT COUNT(*) FROM federationsender_queue_edus" +
	" WHERE json_nid = $1"

// SQL to select queue server names
const selectQueueServerNamesSQL = "" +
	"SELECT DISTINCT server_name FROM federationsender_queue_edus"

// SQL to select expired EDUs
const selectExpiredEDUsSQL = "" +
	"SELECT DISTINCT json_nid FROM federationsender_queue_edus WHERE expires_at > 0 AND expires_at <= $1"

// SQL to delete expired EDUs
const deleteExpiredEDUsSQL = "" +
	"DELETE FROM federationsender_queue_edus WHERE expires_at > 0 AND expires_at <= $1"

// queueEDUTable implements the tables.FederationQueueEDUs interface using postgres
type queueEDUTable struct {
	cm *sqlutil.Connections
	// SQL query string fields, initialized at construction
	insertQueueEDUSQL                   string
	deleteQueueEDUSQL                   string
	selectQueueEDUSQL                   string
	selectQueueEDUReferenceJSONCountSQL string
	selectQueueEDUServerNamesSQL        string
	selectExpiredEDUsSQL                string
	deleteExpiredEDUsSQL                string
}

// NewPostgresQueueEDUsTable creates a new postgres queue EDUs table
func NewPostgresQueueEDUsTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationQueueEDUs, error) {
	t := &queueEDUTable{
		cm:                                  cm,
		insertQueueEDUSQL:                   insertQueueEDUSQL,
		deleteQueueEDUSQL:                   deleteQueueEDUSQL,
		selectQueueEDUSQL:                   selectQueueEDUSQL,
		selectQueueEDUReferenceJSONCountSQL: selectQueueEDUReferenceJSONCountSQL,
		selectQueueEDUServerNamesSQL:        selectQueueServerNamesSQL,
		selectExpiredEDUsSQL:                selectExpiredEDUsSQL,
		deleteExpiredEDUsSQL:                deleteExpiredEDUsSQL,
	}

	// Perform schema migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "federationapi_queue_edus_table_schema_001",
		Patch:       queueEDUsSchema,
		RevertPatch: queueEDUsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// InsertQueueEDU inserts a new EDU into the queue
func (t *queueEDUTable) InsertQueueEDU(
	ctx context.Context,
	eduType string,
	serverName spec.ServerName,
	nid int64,
	expiresAt spec.Timestamp,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.insertQueueEDUSQL, eduType, serverName, nid, expiresAt).Error
}

// DeleteQueueEDUs deletes the given EDUs from the queue
func (t *queueEDUTable) DeleteQueueEDUs(
	ctx context.Context,
	serverName spec.ServerName,
	jsonNIDs []int64,
) error {
	if len(jsonNIDs) == 0 {
		return nil
	}
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteQueueEDUSQL, serverName, pq.Int64Array(jsonNIDs)).Error
}

// SelectQueueEDUs selects EDUs for the given server
func (t *queueEDUTable) SelectQueueEDUs(
	ctx context.Context,
	serverName spec.ServerName,
	limit int,
) ([]int64, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectQueueEDUSQL, serverName, limit).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectQueueEDUs: rows.close() failed")

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

// SelectQueueEDUReferenceJSONCount returns the number of references to the given JSON NID
func (t *queueEDUTable) SelectQueueEDUReferenceJSONCount(
	ctx context.Context, jsonNID int64,
) (int64, error) {
	var count int64
	db := t.cm.Connection(ctx, true)
	err := db.Raw(t.selectQueueEDUReferenceJSONCountSQL, jsonNID).Row().Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// SelectQueueEDUServerNames returns the server names with EDUs queued
func (t *queueEDUTable) SelectQueueEDUServerNames(
	ctx context.Context,
) ([]spec.ServerName, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectQueueEDUServerNamesSQL).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectQueueEDUServerNames: rows.close() failed")

	var result []spec.ServerName
	for rows.Next() {
		var serverName spec.ServerName
		if err = rows.Scan(&serverName); err != nil {
			return nil, err
		}
		result = append(result, serverName)
	}
	if len(result) == 0 {
		return nil, errors.New("no rows found")
	}
	return result, rows.Err()
}

// SelectExpiredEDUs selects EDUs that have expired
func (t *queueEDUTable) SelectExpiredEDUs(
	ctx context.Context,
	expiredBefore spec.Timestamp,
) ([]int64, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectExpiredEDUsSQL, expiredBefore).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectExpiredEDUs: rows.close() failed")

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

// DeleteExpiredEDUs deletes EDUs that have expired
func (t *queueEDUTable) DeleteExpiredEDUs(
	ctx context.Context,
	expiredBefore spec.Timestamp,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteExpiredEDUsSQL, expiredBefore).Error
}
