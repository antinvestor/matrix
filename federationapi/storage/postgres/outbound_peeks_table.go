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
	"database/sql"
	"errors"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/types"
	"github.com/antinvestor/matrix/internal/sqlutil"
)

const outboundPeeksSchema = `
CREATE TABLE IF NOT EXISTS federationsender_outbound_peeks (
	room_id TEXT NOT NULL,
	server_name TEXT NOT NULL,
	peek_id TEXT NOT NULL,
    creation_ts BIGINT NOT NULL,
    renewed_ts BIGINT NOT NULL,
    renewal_interval BIGINT NOT NULL,
	UNIQUE (room_id, server_name, peek_id)
);
`

const outboundPeeksSchemaRevert = `DROP TABLE IF EXISTS federationsender_outbound_peeks;`

// insertOutboundPeekSQL inserts a new outbound peek into the database.
const insertOutboundPeekSQL = "" +
	"INSERT INTO federationsender_outbound_peeks (room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval) VALUES ($1, $2, $3, $4, $5, $6)"

// selectOutboundPeekSQL selects an outbound peek from the database.
const selectOutboundPeekSQL = "" +
	"SELECT room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_outbound_peeks WHERE room_id = $1 and server_name = $2 and peek_id = $3"

// selectOutboundPeeksSQL selects all outbound peeks for a room from the database.
const selectOutboundPeeksSQL = "" +
	"SELECT room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_outbound_peeks WHERE room_id = $1 ORDER BY creation_ts"

// renewOutboundPeekSQL renews an outbound peek in the database.
const renewOutboundPeekSQL = "" +
	"UPDATE federationsender_outbound_peeks SET renewed_ts=$1, renewal_interval=$2 WHERE room_id = $3 and server_name = $4 and peek_id = $5"

// deleteOutboundPeekSQL deletes an outbound peek from the database.
const deleteOutboundPeekSQL = "" +
	"DELETE FROM federationsender_outbound_peeks WHERE room_id = $1 and server_name = $2 and peek_id = $3"

// deleteOutboundPeeksSQL deletes all outbound peeks for a room from the database.
const deleteOutboundPeeksSQL = "" +
	"DELETE FROM federationsender_outbound_peeks WHERE room_id = $1"

// outboundPeeksTable provides methods for outbound peeks operations using GORM.
type outboundPeeksTable struct {
	cm           *sqlutil.Connections // Connection manager for database access
	InsertSQL    string
	SelectSQL    string
	SelectAllSQL string
	RenewSQL     string
	DeleteSQL    string
	DeleteAllSQL string
}

// NewPostgresOutboundPeeksTable initializes an outboundPeeksTable with SQL constants and a connection manager
func NewPostgresOutboundPeeksTable(cm *sqlutil.Connections) tables.FederationOutboundPeeks {
	return &outboundPeeksTable{
		cm:           cm,
		InsertSQL:    insertOutboundPeekSQL,
		SelectSQL:    selectOutboundPeekSQL,
		SelectAllSQL: selectOutboundPeeksSQL,
		RenewSQL:     renewOutboundPeekSQL,
		DeleteSQL:    deleteOutboundPeekSQL,
		DeleteAllSQL: deleteOutboundPeeksSQL,
	}
}

func (t *outboundPeeksTable) InsertOutboundPeek(ctx context.Context, serverName spec.ServerName, roomID, peekID string, renewalInterval int64) error {
	db := t.cm.Connection(ctx, false)
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	return db.Exec(t.InsertSQL, roomID, serverName, peekID, nowMilli, nowMilli, renewalInterval).Error
}

func (t *outboundPeeksTable) RenewOutboundPeek(ctx context.Context, serverName spec.ServerName, roomID, peekID string, renewalInterval int64) error {
	db := t.cm.Connection(ctx, false)
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	return db.Exec(t.RenewSQL, nowMilli, renewalInterval, roomID, serverName, peekID).Error
}

func (t *outboundPeeksTable) SelectOutboundPeek(ctx context.Context, serverName spec.ServerName, roomID, peekID string) (*types.OutboundPeek, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.SelectSQL, roomID, serverName, peekID).Row()
	var peek types.OutboundPeek
	if err := row.Scan(&peek.RoomID, &peek.ServerName, &peek.PeekID, &peek.CreationTimestamp, &peek.RenewedTimestamp, &peek.RenewalInterval); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &peek, nil
}

func (t *outboundPeeksTable) SelectOutboundPeeks(ctx context.Context, roomID string) ([]types.OutboundPeek, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.SelectAllSQL, roomID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")
	var peeks []types.OutboundPeek
	for rows.Next() {
		var peek types.OutboundPeek
		if err := rows.Scan(&peek.RoomID, &peek.ServerName, &peek.PeekID, &peek.CreationTimestamp, &peek.RenewedTimestamp, &peek.RenewalInterval); err != nil {
			return nil, err
		}
		peeks = append(peeks, peek)
	}
	return peeks, nil
}

func (t *outboundPeeksTable) DeleteOutboundPeek(ctx context.Context, serverName spec.ServerName, roomID, peekID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteSQL, roomID, serverName, peekID).Error
}

func (t *outboundPeeksTable) DeleteOutboundPeeks(ctx context.Context, roomID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteAllSQL, roomID).Error
}
