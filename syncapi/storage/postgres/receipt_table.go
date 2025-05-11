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
	"database/sql"
	"fmt"

	"github.com/lib/pq"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/pitabwire/frame"
)

// Schema for receipts table
const receiptsSchema = `
CREATE SEQUENCE IF NOT EXISTS syncapi_receipt_id;

-- Stores data about receipts
CREATE TABLE IF NOT EXISTS syncapi_receipts (
	-- The ID
	id BIGINT PRIMARY KEY DEFAULT nextval('syncapi_receipt_id'),
	room_id TEXT NOT NULL,
	receipt_type TEXT NOT NULL,
	user_id TEXT NOT NULL,
	event_id TEXT NOT NULL,
	receipt_ts BIGINT NOT NULL,
	CONSTRAINT syncapi_receipts_unique UNIQUE (room_id, receipt_type, user_id)
);
CREATE INDEX IF NOT EXISTS syncapi_receipts_room_id ON syncapi_receipts(room_id);
`

// Revert schema for receipts table
const receiptsSchemaRevert = `
DROP INDEX IF EXISTS syncapi_receipts_room_id;
DROP TABLE IF EXISTS syncapi_receipts;
DROP SEQUENCE IF EXISTS syncapi_receipt_id;
`

// SQL query to upsert receipt
const upsertReceiptSQL = `
INSERT INTO syncapi_receipts
 (room_id, receipt_type, user_id, event_id, receipt_ts)
 VALUES ($1, $2, $3, $4, $5)
 ON CONFLICT (room_id, receipt_type, user_id)
 DO UPDATE SET id = nextval('syncapi_receipt_id'), event_id = $4, receipt_ts = $5
 RETURNING id
`

// SQL query to select room receipts
const selectRoomReceiptsSQL = `
SELECT id, room_id, receipt_type, user_id, event_id, receipt_ts
 FROM syncapi_receipts
 WHERE room_id = ANY($1) AND id > $2
`

// SQL query to select max receipt ID
const selectMaxReceiptIDSQL = `
SELECT MAX(id) FROM syncapi_receipts
`

// SQL query to purge receipts
const purgeReceiptsSQL = `
DELETE FROM syncapi_receipts WHERE room_id = $1
`

// receiptTable implements tables.Receipts
type receiptTable struct {
	cm                    sqlutil.ConnectionManager
	upsertReceiptSQL      string
	selectRoomReceiptsSQL string
	selectMaxReceiptIDSQL string
	purgeReceiptsSQL      string
}

// NewPostgresReceiptsTable creates a new receipts table
func NewPostgresReceiptsTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.Receipts, error) {
	t := &receiptTable{
		cm:                    cm,
		upsertReceiptSQL:      upsertReceiptSQL,
		selectRoomReceiptsSQL: selectRoomReceiptsSQL,
		selectMaxReceiptIDSQL: selectMaxReceiptIDSQL,
		purgeReceiptsSQL:      purgeReceiptsSQL,
	}

	// Perform the migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "syncapi_receipts_table_schema_001",
		Patch:       receiptsSchema,
		RevertPatch: receiptsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// UpsertReceipt creates or updates a receipt
func (t *receiptTable) UpsertReceipt(ctx context.Context, roomId, receiptType, userId, eventId string, timestamp spec.Timestamp) (pos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.upsertReceiptSQL, roomId, receiptType, userId, eventId, timestamp)
	err = result.Error
	if err != nil {
		return
	}
	err = result.Row().Scan(&pos)
	return
}

// SelectRoomReceiptsAfter retrieves receipts for rooms after a given position
func (t *receiptTable) SelectRoomReceiptsAfter(ctx context.Context, roomIDs []string, streamPos types.StreamPosition) (types.StreamPosition, []types.OutputReceiptEvent, error) {
	var lastPos types.StreamPosition
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectRoomReceiptsSQL, pq.Array(roomIDs), streamPos).Rows()
	if err != nil {
		return 0, nil, fmt.Errorf("unable to query room receipts: %w", err)
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectRoomReceiptsAfter: rows.close() failed")
	var res []types.OutputReceiptEvent
	for rows.Next() {
		r := types.OutputReceiptEvent{}
		var id types.StreamPosition
		err = rows.Scan(&id, &r.RoomID, &r.Type, &r.UserID, &r.EventID, &r.Timestamp)
		if err != nil {
			return 0, res, fmt.Errorf("unable to scan row to api.Receipts: %w", err)
		}
		res = append(res, r)
		if id > lastPos {
			lastPos = id
		}
	}
	return lastPos, res, rows.Err()
}

// SelectMaxReceiptID retrieves the maximum receipt ID
func (t *receiptTable) SelectMaxReceiptID(ctx context.Context) (id int64, err error) {
	var nullableID sql.NullInt64
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectMaxReceiptIDSQL).Row()
	err = row.Scan(&nullableID)
	if nullableID.Valid {
		id = nullableID.Int64
	}
	return
}

// PurgeReceipts deletes all receipts for a room
func (t *receiptTable) PurgeReceipts(ctx context.Context, roomID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.purgeReceiptsSQL, roomID).Error
}
