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
	"gorm.io/gorm"
)

const inboundPeeksSchema = `
CREATE TABLE IF NOT EXISTS federationsender_inbound_peeks (
	origin TEXT NOT NULL,
	room_id TEXT NOT NULL,
	peek_id TEXT NOT NULL,
	creation_ts BIGINT NOT NULL,
	renewed_ts BIGINT NOT NULL,
	renewal_interval BIGINT NOT NULL,
	UNIQUE (origin, room_id, peek_id)
);
`

// inboundPeeksTable contains the postgres-specific implementation
type inboundPeeksTable struct {
	cm *sqlutil.Connections

	// insertInboundPeekSQL inserts a new inbound peek record with creation timestamp
	insertInboundPeekSQL string

	// selectInboundPeekSQL retrieves a specific inbound peek by origin, room, and peek ID
	selectInboundPeekSQL string

	// selectInboundPeekForRoomSQL retrieves all inbound peeks for a given room
	selectInboundPeekForRoomSQL string

	// selectExpiredInboundPeeksSQL retrieves all inbound peeks that have expired based on renewal time and interval
	selectExpiredInboundPeeksSQL string

	// renewInboundPeekSQL updates the renewed timestamp and renewal interval for a peek
	renewInboundPeekSQL string

	// deleteInboundPeekSQL removes a specific inbound peek
	deleteInboundPeekSQL string

	// deleteInboundPeeksSQL removes all inbound peeks for a given room
	deleteInboundPeeksSQL string
}

// NewPostgresInboundPeeksTable creates a new postgres inbound peeks table and prepares all statements
func NewPostgresInboundPeeksTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationInboundPeeks, error) {
	// Initialize schema using GORM
	gormDB := cm.Connection(ctx, false)
	if err := gormDB.Exec(inboundPeeksSchema).Error; err != nil {
		return nil, err
	}

	s := &inboundPeeksTable{
		cm:                           cm,
		insertInboundPeekSQL:         "INSERT INTO federationsender_inbound_peeks (origin, room_id, peek_id, creation_ts, renewed_ts, renewal_interval) VALUES ($1, $2, $3, $4, $5, $6)",
		selectInboundPeekSQL:         "SELECT origin, room_id, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_inbound_peeks WHERE origin = $1 AND room_id = $2 AND peek_id = $3",
		selectInboundPeekForRoomSQL:  "SELECT origin, room_id, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_inbound_peeks WHERE room_id = $1 ORDER BY creation_ts ASC",
		selectExpiredInboundPeeksSQL: "SELECT origin, room_id, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_inbound_peeks WHERE (renewed_ts + renewal_interval) <= $1",
		renewInboundPeekSQL:          "UPDATE federationsender_inbound_peeks SET renewed_ts = $1, renewal_interval = $2 WHERE origin = $3 AND room_id = $4 AND peek_id = $5",
		deleteInboundPeekSQL:         "DELETE FROM federationsender_inbound_peeks WHERE origin = $1 AND room_id = $2 AND peek_id = $3",
		deleteInboundPeeksSQL:        "DELETE FROM federationsender_inbound_peeks WHERE room_id = $1",
	}

	return s, nil
}

// InsertInboundPeek creates a new inbound peek record
func (s *inboundPeeksTable) InsertInboundPeek(
	ctx context.Context,
	serverName spec.ServerName, roomID, peekID string,
	renewalInterval int64,
) error {
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	db := s.cm.Connection(ctx, false)
	return db.Exec(
		s.insertInboundPeekSQL,
		serverName, roomID, peekID, nowMilli, nowMilli, renewalInterval,
	).Error
}

// RenewInboundPeek updates an existing inbound peek's renewal timestamp and interval
func (s *inboundPeeksTable) RenewInboundPeek(
	ctx context.Context,
	serverName spec.ServerName, roomID, peekID string,
	renewalInterval int64,
) error {
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	db := s.cm.Connection(ctx, false)
	return db.Exec(
		s.renewInboundPeekSQL,
		nowMilli, renewalInterval, serverName, roomID, peekID,
	).Error
}

// SelectInboundPeek retrieves a specific inbound peek
func (s *inboundPeeksTable) SelectInboundPeek(
	ctx context.Context,
	serverName spec.ServerName, roomID, peekID string,
) (*types.InboundPeek, error) {
	db := s.cm.Connection(ctx, true)
	row := db.Raw(
		s.selectInboundPeekSQL,
		serverName, roomID, peekID,
	).Row()

	inboundPeek := types.InboundPeek{}
	err := row.Scan(
		&inboundPeek.ServerName,
		&inboundPeek.RoomID,
		&inboundPeek.PeekID,
		&inboundPeek.CreationTimestamp,
		&inboundPeek.RenewedTimestamp,
		&inboundPeek.RenewalInterval,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &inboundPeek, nil
}

// SelectInboundPeeksInRoom retrieves all inbound peeks for a room
func (s *inboundPeeksTable) SelectInboundPeeksInRoom(
	ctx context.Context,
	roomID string,
) ([]types.InboundPeek, error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(
		s.selectInboundPeekForRoomSQL,
		roomID,
	).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close() // nolint:errcheck

	var result []types.InboundPeek
	for rows.Next() {
		inboundPeek := types.InboundPeek{}
		if err = rows.Scan(
			&inboundPeek.ServerName,
			&inboundPeek.RoomID,
			&inboundPeek.PeekID,
			&inboundPeek.CreationTimestamp,
			&inboundPeek.RenewedTimestamp,
			&inboundPeek.RenewalInterval,
		); err != nil {
			return nil, err
		}
		result = append(result, inboundPeek)
	}

	return result, rows.Err()
}

// SelectExpiredInboundPeeks retrieves all inbound peeks that have expired
func (s *inboundPeeksTable) SelectExpiredInboundPeeks(
	ctx context.Context,
) ([]types.InboundPeek, error) {
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(
		s.selectExpiredInboundPeeksSQL,
		nowMilli,
	).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close() // nolint:errcheck

	var result []types.InboundPeek
	for rows.Next() {
		inboundPeek := types.InboundPeek{}
		if err = rows.Scan(
			&inboundPeek.ServerName,
			&inboundPeek.RoomID,
			&inboundPeek.PeekID,
			&inboundPeek.CreationTimestamp,
			&inboundPeek.RenewedTimestamp,
			&inboundPeek.RenewalInterval,
		); err != nil {
			return nil, err
		}
		result = append(result, inboundPeek)
	}

	return result, rows.Err()
}

// DeleteInboundPeek removes a specific inbound peek
func (s *inboundPeeksTable) DeleteInboundPeek(
	ctx context.Context,
	serverName spec.ServerName, roomID, peekID string,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(
		s.deleteInboundPeekSQL,
		serverName, roomID, peekID,
	).Error
}

// DeleteInboundPeeks removes all inbound peeks for a room
func (s *inboundPeeksTable) DeleteInboundPeeks(
	ctx context.Context,
	roomID string,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(
		s.deleteInboundPeeksSQL,
		roomID,
	).Error
}
