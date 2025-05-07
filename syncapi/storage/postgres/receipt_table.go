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
	"fmt"

	"github.com/lib/pq"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/postgres/deltas"
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

const upsertReceiptSQL = "" +
	"INSERT INTO syncapi_receipts" +
	" (room_id, receipt_type, user_id, event_id, receipt_ts)" +
	" VALUES ($1, $2, $3, $4, $5)" +
	" ON CONFLICT (room_id, receipt_type, user_id)" +
	" DO UPDATE SET id = nextval('syncapi_receipt_id'), event_id = $4, receipt_ts = $5" +
	" RETURNING id"

const selectRoomReceiptsSQL = "" +
	"SELECT id, room_id, receipt_type, user_id, event_id, receipt_ts" +
	" FROM syncapi_receipts" +
	" WHERE room_id = ANY($1) AND id > $2"

const selectMaxReceiptIDSQL = "" +
	"SELECT COALESCE(MAX(id), 0) FROM syncapi_receipts"

const purgeReceiptsSQL = "" +
	"DELETE FROM syncapi_receipts WHERE room_id = $1"

type receiptTable struct {
	cm                     *sqlutil.Connections
	upsertReceiptSQL       string
	selectRoomReceiptsSQL  string
	selectMaxReceiptIDSQL  string
	purgeReceiptsSQL       string
}

func NewPostgresReceiptsTable(ctx context.Context, cm *sqlutil.Connections) (tables.Receipts, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(receiptsSchema).Error; err != nil {
		return nil, err
	}
	
	// Run migrations
	m := sqlutil.NewMigrator(db.DB())
	m.AddMigrations(sqlutil.Migration{
		Version: "syncapi: fix sequences",
		Up:      deltas.UpFixSequences,
	})
	err := m.Up(ctx)
	if err != nil {
		return nil, err
	}
	
	// Initialize the table with SQL statements
	r := &receiptTable{
		cm:                    cm,
		upsertReceiptSQL:      upsertReceiptSQL,
		selectRoomReceiptsSQL: selectRoomReceiptsSQL,
		selectMaxReceiptIDSQL: selectMaxReceiptIDSQL,
		purgeReceiptsSQL:      purgeReceiptsSQL,
	}
	return r, nil
}

func (r *receiptTable) UpsertReceipt(ctx context.Context, roomId, receiptType, userId, eventId string, timestamp spec.Timestamp) (pos types.StreamPosition, err error) {
	// Get database connection
	db := r.cm.Connection(ctx, false)
	
	row := db.Raw(r.upsertReceiptSQL, roomId, receiptType, userId, eventId, timestamp).Row()
	err = row.Scan(&pos)
	return
}

func (r *receiptTable) SelectRoomReceiptsAfter(ctx context.Context, roomIDs []string, streamPos types.StreamPosition) (types.StreamPosition, []types.OutputReceiptEvent, error) {
	// Get database connection
	db := r.cm.Connection(ctx, true)
	
	var lastPos types.StreamPosition
	rows, err := db.Raw(r.selectRoomReceiptsSQL, pq.Array(roomIDs), streamPos).Rows()
	if err != nil {
		return 0, nil, fmt.Errorf("unable to query room receipts: %w", err)
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectRoomReceiptsAfter: rows.close() failed")
	var res []types.OutputReceiptEvent
	for rows.Next() {
		receipt := types.OutputReceiptEvent{}
		var id types.StreamPosition
		err = rows.Scan(&id, &receipt.RoomID, &receipt.Type, &receipt.UserID, &receipt.EventID, &receipt.Timestamp)
		if err != nil {
			return 0, res, fmt.Errorf("unable to scan row to api.Receipts: %w", err)
		}
		res = append(res, receipt)
		if id > lastPos {
			lastPos = id
		}
	}
	return lastPos, res, rows.Err()
}

func (s *receiptTable) SelectMaxReceiptID(ctx context.Context) (id int64, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	err = db.Raw(s.selectMaxReceiptIDSQL).Scan(&id).Error
	return
}

func (s *receiptTable) PurgeReceipts(ctx context.Context, roomID string) error {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	return db.Exec(s.purgeReceiptsSQL, roomID).Error
}
