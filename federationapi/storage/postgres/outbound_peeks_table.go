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
	"errors"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/federationapi/types"
	"github.com/antinvestor/matrix/internal"
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

// SQL query string constants
const (
	// insertOutboundPeekSQL inserts a new outbound peek record with creation timestamp
	insertOutboundPeekSQL = "" +
		"INSERT INTO federationsender_outbound_peeks (room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval) VALUES ($1, $2, $3, $4, $5, $6)"

	// selectOutboundPeekSQL retrieves a specific outbound peek by room, server and peek ID
	selectOutboundPeekSQL = "" +
		"SELECT room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_outbound_peeks WHERE room_id = $1 and server_name = $2 and peek_id = $3"

	// selectOutboundPeeksSQL retrieves all outbound peeks for a room, ordered by creation timestamp
	selectOutboundPeeksSQL = "" +
		"SELECT room_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_outbound_peeks WHERE room_id = $1 ORDER BY creation_ts"

	// renewOutboundPeekSQL updates the renewed timestamp and renewal interval for a peek
	renewOutboundPeekSQL = "" +
		"UPDATE federationsender_outbound_peeks SET renewed_ts=$1, renewal_interval=$2 WHERE room_id = $3 and server_name = $4 and peek_id = $5"

	// deleteOutboundPeekSQL removes a specific outbound peek entry
	deleteOutboundPeekSQL = "" +
		"DELETE FROM federationsender_outbound_peeks WHERE room_id = $1 and server_name = $2 and peek_id = $3"

	// deleteOutboundPeeksSQL removes all outbound peeks for a room
	deleteOutboundPeeksSQL = "" +
		"DELETE FROM federationsender_outbound_peeks WHERE room_id = $1"
)

// outboundPeeksTable contains the postgres-specific implementation
type outboundPeeksTable struct {
	cm *sqlutil.Connections

	insertOutboundPeekStmt  string
	selectOutboundPeekStmt  string
	selectOutboundPeeksStmt string
	renewOutboundPeekStmt   string
	deleteOutboundPeekStmt  string
	deleteOutboundPeeksStmt string
}

// NewPostgresOutboundPeeksTable creates a new postgres outbound peeks table and prepares all statements
func NewPostgresOutboundPeeksTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationOutboundPeeks, error) {
	// Initialize schema using GORM
	gormDB := cm.Connection(ctx, false)
	if err := gormDB.Exec(outboundPeeksSchema).Error; err != nil {
		return nil, err
	}
	
	s := &outboundPeeksTable{
		cm: cm,
		insertOutboundPeekStmt:  insertOutboundPeekSQL,
		selectOutboundPeekStmt:  selectOutboundPeekSQL,
		selectOutboundPeeksStmt: selectOutboundPeeksSQL,
		renewOutboundPeekStmt:   renewOutboundPeekSQL,
		deleteOutboundPeekStmt:  deleteOutboundPeekSQL,
		deleteOutboundPeeksStmt: deleteOutboundPeeksSQL,
	}

	return s, nil
}

// InsertOutboundPeek adds a new outbound peek record
func (s *outboundPeeksTable) InsertOutboundPeek(
	ctx context.Context, serverName spec.ServerName, roomID, peekID string, renewalInterval int64,
) (err error) {
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	
	// Get writable database connection
	db := s.cm.Connection(ctx, false)
	
	return db.Exec(
		s.insertOutboundPeekStmt,
		roomID, serverName, peekID, nowMilli, nowMilli, renewalInterval,
	).Error
}

// RenewOutboundPeek updates the renewal timestamp and interval for an existing peek
func (s *outboundPeeksTable) RenewOutboundPeek(
	ctx context.Context, serverName spec.ServerName, roomID, peekID string, renewalInterval int64,
) (err error) {
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	
	// Get writable database connection
	db := s.cm.Connection(ctx, false)
	
	return db.Exec(
		s.renewOutboundPeekStmt,
		nowMilli, renewalInterval, roomID, serverName, peekID,
	).Error
}

// SelectOutboundPeek retrieves a specific outbound peek
func (s *outboundPeeksTable) SelectOutboundPeek(
	ctx context.Context, serverName spec.ServerName, roomID, peekID string,
) (*types.OutboundPeek, error) {
	// Get read-only database connection
	db := s.cm.Connection(ctx, true)
	
	row := db.Raw(
		s.selectOutboundPeekStmt,
		roomID, serverName, peekID,
	).Row()
	
	outboundPeek := types.OutboundPeek{}
	err := row.Scan(
		&outboundPeek.RoomID,
		&outboundPeek.ServerName,
		&outboundPeek.PeekID,
		&outboundPeek.CreationTimestamp,
		&outboundPeek.RenewedTimestamp,
		&outboundPeek.RenewalInterval,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &outboundPeek, nil
}

// SelectOutboundPeeks retrieves all outbound peeks for a room
func (s *outboundPeeksTable) SelectOutboundPeeks(
	ctx context.Context, roomID string,
) (outboundPeeks []types.OutboundPeek, err error) {
	// Get read-only database connection
	db := s.cm.Connection(ctx, true)
	
	rows, err := db.Raw(s.selectOutboundPeeksStmt, roomID).Rows()
	if err != nil {
		return nil, err
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
			return nil, err
		}
		outboundPeeks = append(outboundPeeks, outboundPeek)
	}

	return outboundPeeks, rows.Err()
}

// DeleteOutboundPeek removes a specific outbound peek
func (s *outboundPeeksTable) DeleteOutboundPeek(
	ctx context.Context, serverName spec.ServerName, roomID, peekID string,
) (err error) {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)
	
	return db.Exec(s.deleteOutboundPeekStmt, roomID, serverName, peekID).Error
}

// DeleteOutboundPeeks removes all outbound peeks for a room
func (s *outboundPeeksTable) DeleteOutboundPeeks(
	ctx context.Context, roomID string,
) (err error) {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)
	
	return db.Exec(s.deleteOutboundPeeksStmt, roomID).Error
}
