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

	"github.com/lib/pq"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
)

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

const queueEDUsSchemaRevert = `DROP TABLE IF EXISTS federationsender_queue_edus;`

const insertQueueEDUSQL = "" +
	"INSERT INTO federationsender_queue_edus (edu_type, server_name, json_nid, expires_at)" +
	" VALUES ($1, $2, $3, $4)" // Insert a new EDU into the queue.

const deleteQueueEDUSQL = "" +
	"DELETE FROM federationsender_queue_edus WHERE server_name = $1 AND json_nid = ANY($2)" // Delete EDUs from the queue.

const selectQueueEDUSQL = "" +
	"SELECT json_nid FROM federationsender_queue_edus" +
	" WHERE server_name = $1" +
	" LIMIT $2" // Select EDUs from the queue.

const selectQueueEDUReferenceJSONCountSQL = "" +
	"SELECT COUNT(*) FROM federationsender_queue_edus" +
	" WHERE json_nid = $1" // Select the count of EDUs referencing a JSON blob.

const selectQueueServerNamesSQL = "" +
	"SELECT DISTINCT server_name FROM federationsender_queue_edus" // Select the server names from the queue.

const selectExpiredEDUsSQL = "" +
	"SELECT DISTINCT json_nid FROM federationsender_queue_edus WHERE expires_at > 0 AND expires_at <= $1" // Select expired EDUs from the queue.

const deleteExpiredEDUsSQL = "" +
	"DELETE FROM federationsender_queue_edus WHERE expires_at > 0 AND expires_at <= $1" // Delete expired EDUs from the queue.

type queueEDUsTable struct {
	cm                      *sqlutil.Connections
	InsertSQL               string
	DeleteSQL               string
	SelectSQL               string
	SelectReferenceCountSQL string
	SelectServerNamesSQL    string
	SelectExpiredSQL        string
	DeleteExpiredSQL        string
}

// NewPostgresQueueEDUsTable initializes a queueEDUsTable with SQL constants and a connection manager
func NewPostgresQueueEDUsTable(cm *sqlutil.Connections) tables.FederationQueueEDUs {
	return &queueEDUsTable{
		cm:                      cm,
		InsertSQL:               insertQueueEDUSQL,
		DeleteSQL:               deleteQueueEDUSQL,
		SelectSQL:               selectQueueEDUSQL,
		SelectReferenceCountSQL: selectQueueEDUReferenceJSONCountSQL,
		SelectServerNamesSQL:    selectQueueServerNamesSQL,
		SelectExpiredSQL:        selectExpiredEDUsSQL,
		DeleteExpiredSQL:        deleteExpiredEDUsSQL,
	}
}

func (t *queueEDUsTable) InsertQueueEDU(ctx context.Context, eduType string, serverName spec.ServerName, nid int64, expiresAt spec.Timestamp) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.InsertSQL, eduType, serverName, nid, expiresAt)
	return result.Error
}

func (t *queueEDUsTable) DeleteQueueEDUs(ctx context.Context, serverName spec.ServerName, jsonNIDs []int64) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.DeleteSQL, serverName, pq.Int64Array(jsonNIDs))
	return result.Error
}

func (t *queueEDUsTable) SelectQueueEDUs(ctx context.Context, serverName spec.ServerName, limit int) ([]int64, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.SelectSQL, serverName, limit).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (t *queueEDUsTable) SelectQueueEDUReferenceJSONCount(ctx context.Context, jsonNID int64) (int64, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.SelectReferenceCountSQL, jsonNID).Row()
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (t *queueEDUsTable) SelectQueueEDUServerNames(ctx context.Context) ([]spec.ServerName, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.SelectServerNamesSQL).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (t *queueEDUsTable) SelectExpiredEDUs(ctx context.Context, expiredBefore spec.Timestamp) ([]int64, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.SelectExpiredSQL, expiredBefore).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (t *queueEDUsTable) DeleteExpiredEDUs(ctx context.Context, expiredBefore spec.Timestamp) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.DeleteExpiredSQL, expiredBefore)
	return result.Error
}
