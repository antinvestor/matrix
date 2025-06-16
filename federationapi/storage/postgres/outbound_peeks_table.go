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

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/federationapi/types"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/pitabwire/frame"
)

// Schema for the outbound peeks table
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

// Schema revert for the outbound peeks table
const outboundPeeksSchemaRevert = `
DROP TABLE IF EXISTS federationsender_outbound_peeks;
`

// SQL for inserting an outbound peek into the table
const insertOutboundPeekSQL = "" +
	"INSERT INTO federationsender_outbound_peeks (room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval) VALUES ($1, $2, $3, $4, $5, $6)"

// SQL for selecting a specific outbound peek
const selectOutboundPeekSQL = "" +
	"SELECT room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_outbound_peeks WHERE room_id = $1 and server_name = $2 and peek_id = $3"

// SQL for selecting all outbound peeks for a room
const selectOutboundPeeksSQL = "" +
	"SELECT room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_outbound_peeks WHERE room_id = $1 ORDER BY creation_ts"

// SQL for renewing an outbound peek
const renewOutboundPeekSQL = "" +
	"UPDATE federationsender_outbound_peeks SET renewed_ts=$1, renewal_interval=$2 WHERE room_id = $3 and server_name = $4 and peek_id = $5"

// SQL for deleting a specific outbound peek
const deleteOutboundPeekSQL = "" +
	"DELETE FROM federationsender_outbound_peeks WHERE room_id = $1 and server_name = $2 and peek_id = $3"

// SQL for deleting all outbound peeks for a room
const deleteOutboundPeeksSQL = "" +
	"DELETE FROM federationsender_outbound_peeks WHERE room_id = $1"

// outboundPeeksTable stores information about outbound peeks to other servers
type outboundPeeksTable struct {
	cm sqlutil.ConnectionManager
	// SQL query string fields, initialise at construction
	insertOutboundPeekSQL  string
	selectOutboundPeekSQL  string
	selectOutboundPeeksSQL string
	renewOutboundPeekSQL   string
	deleteOutboundPeekSQL  string
	deleteOutboundPeeksSQL string
}

// NewPostgresOutboundPeeksTable creates a new postgres outbound peeks table
func NewPostgresOutboundPeeksTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.FederationOutboundPeeks, error) {
	s := &outboundPeeksTable{
		cm:                     cm,
		insertOutboundPeekSQL:  insertOutboundPeekSQL,
		selectOutboundPeekSQL:  selectOutboundPeekSQL,
		selectOutboundPeeksSQL: selectOutboundPeeksSQL,
		renewOutboundPeekSQL:   renewOutboundPeekSQL,
		deleteOutboundPeekSQL:  deleteOutboundPeekSQL,
		deleteOutboundPeeksSQL: deleteOutboundPeeksSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "federationapi_outbound_peeks_table_schema_001",
		Patch:       outboundPeeksSchema,
		RevertPatch: outboundPeeksSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertOutboundPeek adds an outbound peek to the table
func (s *outboundPeeksTable) InsertOutboundPeek(
	ctx context.Context, serverName spec.ServerName, roomID, peekID string, renewalInterval int64,
) (err error) {
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.insertOutboundPeekSQL, roomID, serverName, peekID, nowMilli, nowMilli, renewalInterval).Error
}

// RenewOutboundPeek updates the renewal timestamp and interval for an outbound peek
func (s *outboundPeeksTable) RenewOutboundPeek(
	ctx context.Context, serverName spec.ServerName, roomID, peekID string, renewalInterval int64,
) (err error) {
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.renewOutboundPeekSQL, nowMilli, renewalInterval, roomID, serverName, peekID).Error
}

// SelectOutboundPeek gets a specific outbound peek
func (s *outboundPeeksTable) SelectOutboundPeek(
	ctx context.Context, serverName spec.ServerName, roomID, peekID string,
) (*types.OutboundPeek, error) {
	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.selectOutboundPeekSQL, roomID, serverName, peekID).Row()

	outboundPeek := types.OutboundPeek{}
	err := row.Scan(
		&outboundPeek.RoomID,
		&outboundPeek.ServerName,
		&outboundPeek.PeekID,
		&outboundPeek.CreationTimestamp,
		&outboundPeek.RenewedTimestamp,
		&outboundPeek.RenewalInterval,
	)
	if err != nil {
		if sqlutil.ErrorIsNoRows(err) {
			return nil, nil
		}
		return nil, err
	}
	return &outboundPeek, nil
}

// SelectOutboundPeeks gets all outbound peeks for a room
func (s *outboundPeeksTable) SelectOutboundPeeks(
	ctx context.Context, roomID string,
) (outboundPeeks []types.OutboundPeek, err error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectOutboundPeeksSQL, roomID).Rows()
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectOutboundPeeks: rows.close() failed")

	for rows.Next() {
		outboundPeek := types.OutboundPeek{}
		if err = rows.Scan(
			&outboundPeek.RoomID,
			&outboundPeek.ServerName,
			&outboundPeek.PeekID,
			&outboundPeek.CreationTimestamp,
			&outboundPeek.RenewedTimestamp,
			&outboundPeek.RenewalInterval,
		); err != nil {
			return
		}
		outboundPeeks = append(outboundPeeks, outboundPeek)
	}

	return outboundPeeks, rows.Err()
}

// DeleteOutboundPeek removes a specific outbound peek
func (s *outboundPeeksTable) DeleteOutboundPeek(
	ctx context.Context, serverName spec.ServerName, roomID, peekID string,
) (err error) {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteOutboundPeekSQL, roomID, serverName, peekID).Error
}

// DeleteOutboundPeeks removes all outbound peeks for a room
func (s *outboundPeeksTable) DeleteOutboundPeeks(
	ctx context.Context, roomID string,
) (err error) {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteOutboundPeeksSQL, roomID).Error
}
