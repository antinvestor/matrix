// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Matrix.org Foundation C.I.C.
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

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/federationapi/types"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
)

const joinedHostsSchema = `
-- The joined_hosts table stores a list of m.room.member event ids in the
-- current state for each room where the membership is "join".
-- There will be an entry for every user that is joined to the room.
CREATE TABLE IF NOT EXISTS federationsender_joined_hosts (
    -- The string ID of the room.
    room_id TEXT NOT NULL,
    -- The event ID of the m.room.member join event.
    event_id TEXT NOT NULL,
    -- The domain part of the user ID the m.room.member event is for.
    server_name TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS federatonsender_joined_hosts_event_id_idx
    ON federationsender_joined_hosts (event_id);

CREATE INDEX IF NOT EXISTS federatonsender_joined_hosts_room_id_idx
    ON federationsender_joined_hosts (room_id)
`

// SQL query constants
const (
	insertJoinedHostsSQL = "" +
		"INSERT INTO federationsender_joined_hosts (room_id, event_id, server_name)" +
		" VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"

	deleteJoinedHostsSQL = "" +
		"DELETE FROM federationsender_joined_hosts WHERE event_id = ANY($1)"

	deleteJoinedHostsForRoomSQL = "" +
		"DELETE FROM federationsender_joined_hosts WHERE room_id = $1"

	selectJoinedHostsSQL = "" +
		"SELECT event_id, server_name FROM federationsender_joined_hosts" +
		" WHERE room_id = $1"

	selectAllJoinedHostsSQL = "" +
		"SELECT DISTINCT server_name FROM federationsender_joined_hosts"

	selectJoinedHostsForRoomsSQL = "" +
		"SELECT DISTINCT server_name FROM federationsender_joined_hosts WHERE room_id = ANY($1)"

	selectJoinedHostsForRoomsExcludingBlacklistedSQL = "" +
		"SELECT DISTINCT server_name FROM federationsender_joined_hosts j WHERE room_id = ANY($1) AND NOT EXISTS (" +
		"  SELECT server_name FROM federationsender_blacklist WHERE j.server_name = server_name" +
		");"
)

type joinedHostsTable struct {
	cm *sqlutil.Connections

	insertJoinedHostsStmt                             string
	deleteJoinedHostsStmt                             string
	deleteJoinedHostsForRoomStmt                      string
	selectJoinedHostsStmt                             string
	selectAllJoinedHostsStmt                          string
	selectJoinedHostsForRoomsStmt                     string
	selectJoinedHostsForRoomsExcludingBlacklistedStmt string
}

func NewPostgresJoinedHostsTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationJoinedHosts, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(joinedHostsSchema).Error; err != nil {
		return nil, err
	}

	s := &joinedHostsTable{
		cm:                            cm,
		insertJoinedHostsStmt:         insertJoinedHostsSQL,
		deleteJoinedHostsStmt:         deleteJoinedHostsSQL,
		deleteJoinedHostsForRoomStmt:  deleteJoinedHostsForRoomSQL,
		selectJoinedHostsStmt:         selectJoinedHostsSQL,
		selectAllJoinedHostsStmt:      selectAllJoinedHostsSQL,
		selectJoinedHostsForRoomsStmt: selectJoinedHostsForRoomsSQL,
		selectJoinedHostsForRoomsExcludingBlacklistedStmt: selectJoinedHostsForRoomsExcludingBlacklistedSQL,
	}

	return s, nil
}

func (s *joinedHostsTable) InsertJoinedHosts(
	ctx context.Context,
	roomID, eventID string,
	serverName spec.ServerName,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(s.insertJoinedHostsStmt, roomID, eventID, serverName).Error
}

func (s *joinedHostsTable) DeleteJoinedHosts(
	ctx context.Context, eventIDs []string,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(s.deleteJoinedHostsStmt, pq.StringArray(eventIDs)).Error
}

func (s *joinedHostsTable) DeleteJoinedHostsForRoom(
	ctx context.Context, roomID string,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(s.deleteJoinedHostsForRoomStmt, roomID).Error
}

func (s *joinedHostsTable) SelectJoinedHostsWithTx(
	ctx context.Context, roomID string,
) ([]types.JoinedHost, error) {
	// Get readable database connection
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(s.selectJoinedHostsStmt, roomID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectJoinedHostsWithTx: rows.close() failed")

	var result []types.JoinedHost
	for rows.Next() {
		var eventID string
		var serverName string
		if err = rows.Scan(&eventID, &serverName); err != nil {
			return nil, err
		}
		result = append(result, types.JoinedHost{
			MemberEventID: eventID,
			ServerName:    spec.ServerName(serverName),
		})
	}

	return result, rows.Err()
}

func (s *joinedHostsTable) SelectJoinedHosts(
	ctx context.Context, roomID string,
) ([]types.JoinedHost, error) {
	// Get readable database connection
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(s.selectJoinedHostsStmt, roomID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectJoinedHosts: rows.close() failed")

	var result []types.JoinedHost
	for rows.Next() {
		var eventID string
		var serverName string
		if err = rows.Scan(&eventID, &serverName); err != nil {
			return nil, err
		}
		result = append(result, types.JoinedHost{
			MemberEventID: eventID,
			ServerName:    spec.ServerName(serverName),
		})
	}

	return result, rows.Err()
}

func (s *joinedHostsTable) SelectAllJoinedHosts(
	ctx context.Context,
) ([]spec.ServerName, error) {
	// Get readable database connection
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(s.selectAllJoinedHostsStmt).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectAllJoinedHosts: rows.close() failed")

	var result []spec.ServerName
	for rows.Next() {
		var serverName string
		if err = rows.Scan(&serverName); err != nil {
			return nil, err
		}
		result = append(result, spec.ServerName(serverName))
	}

	return result, rows.Err()
}

func (s *joinedHostsTable) SelectJoinedHostsForRooms(
	ctx context.Context, roomIDs []string, excludingBlacklisted bool,
) ([]spec.ServerName, error) {
	// Get readable database connection
	db := s.cm.Connection(ctx, true)

	var sqlQuery string
	if excludingBlacklisted {
		sqlQuery = s.selectJoinedHostsForRoomsExcludingBlacklistedStmt
	} else {
		sqlQuery = s.selectJoinedHostsForRoomsStmt
	}

	rows, err := db.Raw(sqlQuery, pq.StringArray(roomIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectJoinedHostsForRooms: rows.close() failed")

	var result []spec.ServerName
	for rows.Next() {
		var serverName string
		if err = rows.Scan(&serverName); err != nil {
			return nil, err
		}
		result = append(result, spec.ServerName(serverName))
	}

	return result, rows.Err()
}
