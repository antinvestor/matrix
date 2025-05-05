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
	"fmt"

	"github.com/lib/pq"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
)

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

const receiptSchemaRevert = `DROP TABLE IF EXISTS syncapi_receipts;`

const upsertReceipt = "" +
	"INSERT INTO syncapi_receipts" +
	" (room_id, receipt_type, user_id, event_id, receipt_ts)" +
	" VALUES ($1, $2, $3, $4, $5)" +
	" ON CONFLICT (room_id, receipt_type, user_id)" +
	" DO UPDATE SET id = nextval('syncapi_receipt_id'), event_id = $4, receipt_ts = $5" +
	" RETURNING id"

const selectRoomReceipts = "" +
	"SELECT id, room_id, receipt_type, user_id, event_id, receipt_ts" +
	" FROM syncapi_receipts" +
	" WHERE room_id = ANY($1) AND id > $2"

const selectMaxReceiptIDSQL = "" +
	"SELECT MAX(id) FROM syncapi_receipts"

const purgeReceiptsSQL = "" +
	"DELETE FROM syncapi_receipts WHERE room_id = $1"

// receiptTable implements tables.Receipts using a connection manager and SQL constants.
// This table stores receipts and provides methods for upserting and querying them.
type receiptTable struct {
	cm                    *sqlutil.Connections
	upsertReceiptSQL      string
	selectRoomReceiptsSQL string
	selectMaxReceiptIDSQL string
	purgeReceiptsSQL      string
}

// NewPostgresReceiptsTable creates a new Receipts table using a connection manager.
func NewPostgresReceiptsTable(cm *sqlutil.Connections) tables.Receipts {
	return &receiptTable{
		cm:                    cm,
		upsertReceiptSQL:      upsertReceipt,
		selectRoomReceiptsSQL: selectRoomReceipts,
		selectMaxReceiptIDSQL: selectMaxReceiptIDSQL,
		purgeReceiptsSQL:      purgeReceiptsSQL,
	}
}

// UpsertReceipt inserts or updates a receipt for a user in a room.
func (t *receiptTable) UpsertReceipt(ctx context.Context, roomId, receiptType, userId, eventId string, timestamp spec.Timestamp) (pos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Raw(t.upsertReceiptSQL, roomId, receiptType, userId, eventId, timestamp).Row().Scan(&pos)
	return
}

// SelectRoomReceiptsAfter returns receipts for a set of rooms after a given stream position.
func (t *receiptTable) SelectRoomReceiptsAfter(ctx context.Context, roomIDs []string, streamPos types.StreamPosition) (types.StreamPosition, []types.OutputReceiptEvent, error) {
	var lastPos types.StreamPosition
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectRoomReceiptsSQL, pq.Array(roomIDs), streamPos).Rows()
	if err != nil {
		return 0, nil, err
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

// SelectMaxReceiptID returns the maximum stream position for receipts.
func (t *receiptTable) SelectMaxReceiptID(ctx context.Context) (id int64, err error) {
	db := t.cm.Connection(ctx, true)
	err = db.Raw(t.selectMaxReceiptIDSQL).Row().Scan(&id)
	return
}

// PurgeReceipts removes all receipts for a given room.
func (t *receiptTable) PurgeReceipts(ctx context.Context, roomID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.purgeReceiptsSQL, roomID).Error
}
