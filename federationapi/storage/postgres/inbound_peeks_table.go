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
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/types"

	"github.com/antinvestor/matrix/internal/sqlutil"
)

const inboundPeeksSchema = `
CREATE TABLE IF NOT EXISTS federationsender_inbound_peeks (
	room_id TEXT NOT NULL,
	server_name TEXT NOT NULL,
	peek_id TEXT NOT NULL,
    creation_ts BIGINT NOT NULL,
    renewed_ts BIGINT NOT NULL,
    renewal_interval BIGINT NOT NULL,
	UNIQUE (room_id, server_name, peek_id)
);
`

const inboundPeeksSchemaRevert = `DROP TABLE IF EXISTS federationsender_inbound_peeks;`

// SQL queries for inbound peeks operations
const (
	// Insert a new inbound peek
	insertInboundPeekSQL = "INSERT INTO federationsender_inbound_peeks (room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval) VALUES ($1, $2, $3, $4, $5, $6)"

	// Select a specific inbound peek
	selectInboundPeekSQL = "SELECT room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_inbound_peeks WHERE room_id = $1 and server_name = $2 and peek_id = $3"

	// Select all inbound peeks for a room
	selectInboundPeeksSQL = "SELECT room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_inbound_peeks WHERE room_id = $1 ORDER by creation_ts"

	// Renew an inbound peek
	renewInboundPeekSQL = "UPDATE federationsender_inbound_peeks SET renewed_ts=$1, renewal_interval=$2 WHERE room_id = $3 and server_name = $4 and peek_id = $5"

	// Delete a specific inbound peek
	deleteInboundPeekSQL = "DELETE FROM federationsender_inbound_peeks WHERE room_id = $1 and server_name = $2 and peek_id = $3"

	// Delete all inbound peeks for a room
	deleteInboundPeeksSQL = "DELETE FROM federationsender_inbound_peeks WHERE room_id = $1"
)

// inboundPeeksTable provides methods for inbound peeks operations using GORM.
type inboundPeeksTable struct {
	cm                  *sqlutil.Connections
	InsertSQL           string
	SelectSQL           string
	SelectAllSQL        string
	RenewSQL            string
	DeleteSQL           string
	DeleteAllForRoomSQL string
}

// NewPostgresInboundPeeksTable initializes an inboundPeeksTable with SQL constants and a connection manager
func NewPostgresInboundPeeksTable(cm *sqlutil.Connections) tables.FederationInboundPeeks {
	return &inboundPeeksTable{
		cm:                  cm,
		InsertSQL:           insertInboundPeekSQL,
		SelectSQL:           selectInboundPeekSQL,
		SelectAllSQL:        selectInboundPeeksSQL,
		RenewSQL:            renewInboundPeekSQL,
		DeleteSQL:           deleteInboundPeekSQL,
		DeleteAllForRoomSQL: deleteInboundPeeksSQL,
	}
}

// InsertInboundPeek inserts a new inbound peek
func (t *inboundPeeksTable) InsertInboundPeek(ctx context.Context, serverName spec.ServerName, roomID, peekID string, renewalInterval int64) error {
	db := t.cm.Connection(ctx, false)
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	return db.Exec(t.InsertSQL, roomID, serverName, peekID, nowMilli, nowMilli, renewalInterval).Error
}

// RenewInboundPeek updates the renewed_ts and renewal_interval for an inbound peek
func (t *inboundPeeksTable) RenewInboundPeek(ctx context.Context, serverName spec.ServerName, roomID, peekID string, renewalInterval int64) error {
	db := t.cm.Connection(ctx, false)
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	return db.Exec(t.RenewSQL, nowMilli, renewalInterval, roomID, serverName, peekID).Error
}

// SelectInboundPeek retrieves a specific inbound peek
func (t *inboundPeeksTable) SelectInboundPeek(ctx context.Context, serverName spec.ServerName, roomID, peekID string) (*types.InboundPeek, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.SelectSQL, roomID, serverName, peekID).Row()
	var peek types.InboundPeek
	err := row.Scan(&peek.RoomID, &peek.ServerName, &peek.PeekID, &peek.CreationTimestamp, &peek.RenewedTimestamp, &peek.RenewalInterval)
	if err != nil {
		return nil, nil // Not found
	}
	return &peek, nil
}

// SelectInboundPeeks retrieves all inbound peeks for a room
func (t *inboundPeeksTable) SelectInboundPeeks(ctx context.Context, roomID string) ([]types.InboundPeek, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.SelectAllSQL, roomID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var peeks []types.InboundPeek
	for rows.Next() {
		var peek types.InboundPeek
		err := rows.Scan(&peek.RoomID, &peek.ServerName, &peek.PeekID, &peek.CreationTimestamp, &peek.RenewedTimestamp, &peek.RenewalInterval)
		if err != nil {
			return nil, err
		}
		peeks = append(peeks, peek)
	}
	return peeks, nil
}

// DeleteInboundPeek deletes a specific inbound peek
func (t *inboundPeeksTable) DeleteInboundPeek(ctx context.Context, serverName spec.ServerName, roomID, peekID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteSQL, roomID, serverName, peekID).Error
}

// DeleteInboundPeeks deletes all inbound peeks for a room
func (t *inboundPeeksTable) DeleteInboundPeeks(ctx context.Context, roomID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteAllForRoomSQL, roomID).Error
}
