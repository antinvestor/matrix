// Copyright 2025 Ant Investor Ltd.
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
	"time"

	"github.com/antinvestor/matrix/federationapi/storage/tables"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/types"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/pitabwire/frame"
	"gorm.io/gorm"
)

// Schema for the inbound peeks table
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

// Schema revert for the inbound peeks table
const inboundPeeksSchemaRevert = `
DROP TABLE IF EXISTS federationsender_inbound_peeks;
`

// SQL for inserting an inbound peek into the table
const insertInboundPeekSQL = "" +
	"INSERT INTO federationsender_inbound_peeks (room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval) VALUES ($1, $2, $3, $4, $5, $6)"

// SQL for selecting a specific inbound peek
const selectInboundPeekSQL = "" +
	"SELECT room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_inbound_peeks WHERE room_id = $1 and server_name = $2 and peek_id = $3"

// SQL for selecting all inbound peeks for a room
const selectInboundPeeksSQL = "" +
	"SELECT room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_inbound_peeks WHERE room_id = $1 ORDER by creation_ts"

// SQL for renewing an inbound peek
const renewInboundPeekSQL = "" +
	"UPDATE federationsender_inbound_peeks SET renewed_ts=$1, renewal_interval=$2 WHERE room_id = $3 and server_name = $4 and peek_id = $5"

// SQL for deleting a specific inbound peek
const deleteInboundPeekSQL = "" +
	"DELETE FROM federationsender_inbound_peeks WHERE room_id = $1 and server_name = $2 and peek_id = $3"

// SQL for deleting all inbound peeks for a room
const deleteInboundPeeksSQL = "" +
	"DELETE FROM federationsender_inbound_peeks WHERE room_id = $1"

// inboundPeeksTable stores information about inbound peeks from other servers
type inboundPeeksTable struct {
	cm sqlutil.ConnectionManager
	// SQL query string fields, initialise at construction
	insertInboundPeekSQL  string
	selectInboundPeekSQL  string
	selectInboundPeeksSQL string
	renewInboundPeekSQL   string
	deleteInboundPeekSQL  string
	deleteInboundPeeksSQL string
}

// NewPostgresInboundPeeksTable creates a new postgres inbound peeks table
func NewPostgresInboundPeeksTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.FederationInboundPeeks, error) {
	s := &inboundPeeksTable{
		cm:                    cm,
		insertInboundPeekSQL:  insertInboundPeekSQL,
		selectInboundPeekSQL:  selectInboundPeekSQL,
		selectInboundPeeksSQL: selectInboundPeeksSQL,
		renewInboundPeekSQL:   renewInboundPeekSQL,
		deleteInboundPeekSQL:  deleteInboundPeekSQL,
		deleteInboundPeeksSQL: deleteInboundPeeksSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "federationapi_inbound_peeks_table_schema_001",
		Patch:       inboundPeeksSchema,
		RevertPatch: inboundPeeksSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertInboundPeek adds an inbound peek to the table
func (s *inboundPeeksTable) InsertInboundPeek(
	ctx context.Context, serverName spec.ServerName, roomID, peekID string, renewalInterval int64,
) (err error) {
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.insertInboundPeekSQL, roomID, serverName, peekID, nowMilli, nowMilli, renewalInterval).Error
}

// RenewInboundPeek updates the renewal timestamp and interval for an inbound peek
func (s *inboundPeeksTable) RenewInboundPeek(
	ctx context.Context, serverName spec.ServerName, roomID, peekID string, renewalInterval int64,
) (err error) {
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.renewInboundPeekSQL, nowMilli, renewalInterval, roomID, serverName, peekID).Error
}

// SelectInboundPeek gets a specific inbound peek
func (s *inboundPeeksTable) SelectInboundPeek(
	ctx context.Context, serverName spec.ServerName, roomID, peekID string,
) (*types.InboundPeek, error) {
	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.selectInboundPeekSQL, roomID, serverName, peekID).Row()

	inboundPeek := types.InboundPeek{}
	err := row.Scan(
		&inboundPeek.RoomID,
		&inboundPeek.ServerName,
		&inboundPeek.PeekID,
		&inboundPeek.CreationTimestamp,
		&inboundPeek.RenewedTimestamp,
		&inboundPeek.RenewalInterval,
	)
	if err != nil {
		if sqlutil.ErrorIsNoRows(err) {
			return nil, nil
		}
		return nil, err
	}
	return &inboundPeek, nil
}

// SelectInboundPeeks gets all inbound peeks for a room
func (s *inboundPeeksTable) SelectInboundPeeks(
	ctx context.Context, roomID string,
) (inboundPeeks []types.InboundPeek, err error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectInboundPeeksSQL, roomID).Rows()
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectInboundPeeks: rows.close() failed")

	for rows.Next() {
		inboundPeek := types.InboundPeek{}
		if err = rows.Scan(
			&inboundPeek.RoomID,
			&inboundPeek.ServerName,
			&inboundPeek.PeekID,
			&inboundPeek.CreationTimestamp,
			&inboundPeek.RenewedTimestamp,
			&inboundPeek.RenewalInterval,
		); err != nil {
			return
		}
		inboundPeeks = append(inboundPeeks, inboundPeek)
	}

	return inboundPeeks, rows.Err()
}

// DeleteInboundPeek removes a specific inbound peek
func (s *inboundPeeksTable) DeleteInboundPeek(
	ctx context.Context, serverName spec.ServerName, roomID, peekID string,
) (err error) {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteInboundPeekSQL, roomID, serverName, peekID).Error
}

// DeleteInboundPeeks removes all inbound peeks for a room
func (s *inboundPeeksTable) DeleteInboundPeeks(
	ctx context.Context, roomID string,
) (err error) {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteInboundPeeksSQL, roomID).Error
}

// GetDB returns the underlying GORM database connection
func (s *inboundPeeksTable) GetDB() *gorm.DB {
	return s.cm.Connection(context.Background(), true)
}
