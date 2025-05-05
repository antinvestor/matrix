// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Global.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
)

// SQL queries for joined hosts operations
// joinedHostsSchema defines the schema for the joined_hosts table and its indexes.
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
    ON federationsender_joined_hosts (room_id);
`

// joinedHostsSchemaRevert defines how to revert the joined_hosts schema and indexes.
const joinedHostsSchemaRevert = `DROP TABLE IF EXISTS federationsender_joined_hosts; DROP INDEX IF EXISTS federatonsender_joined_hosts_event_id_idx; DROP INDEX IF EXISTS federatonsender_joined_hosts_room_id_idx;`

const (
	// Insert a joined host into the table
	insertJoinedHostsSQL = "INSERT INTO federationsender_joined_hosts (room_id, event_id, server_name) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"

	// Delete joined hosts by event IDs
	deleteJoinedHostsSQL = "DELETE FROM federationsender_joined_hosts WHERE event_id = ANY($1)"

	// Delete joined hosts for a specific room
	deleteJoinedHostsForRoomSQL = "DELETE FROM federationsender_joined_hosts WHERE room_id = $1"

	// Select joined hosts for a specific room
	selectJoinedHostsSQL = "SELECT event_id, server_name FROM federationsender_joined_hosts WHERE room_id = $1"

	// Select all joined hosts
	selectAllJoinedHostsSQL = "SELECT DISTINCT server_name FROM federationsender_joined_hosts"

	// Select joined hosts for multiple rooms
	selectJoinedHostsForRoomsSQL = "SELECT DISTINCT server_name FROM federationsender_joined_hosts WHERE room_id = ANY($1)"

	// Select joined hosts for multiple rooms, excluding blacklisted
	selectJoinedHostsForRoomsExcludingBlacklistedSQL = "SELECT DISTINCT server_name FROM federationsender_joined_hosts j WHERE room_id = ANY($1) AND NOT EXISTS (SELECT server_name FROM federationsender_blacklist WHERE j.server_name = server_name)"
)

// joinedHostsTable provides methods for joined hosts operations using GORM.
type joinedHostsTable struct {
	cm                                    *sqlutil.Connections // Connection manager for database access
	InsertSQL                             string
	DeleteSQL                             string
	DeleteForRoomSQL                      string
	SelectSQL                             string
	SelectAllSQL                          string
	SelectForRoomsSQL                     string
	SelectForRoomsExcludingBlacklistedSQL string
}

// NewPostgresJoinedHostsTable initializes a joinedHostsTable with SQL constants and a connection manager
func NewPostgresJoinedHostsTable(cm *sqlutil.Connections) tables.FederationJoinedHosts {
	return &joinedHostsTable{
		cm:                                    cm,
		InsertSQL:                             insertJoinedHostsSQL,
		DeleteSQL:                             deleteJoinedHostsSQL,
		DeleteForRoomSQL:                      deleteJoinedHostsForRoomSQL,
		SelectSQL:                             selectJoinedHostsSQL,
		SelectAllSQL:                          selectAllJoinedHostsSQL,
		SelectForRoomsSQL:                     selectJoinedHostsForRoomsSQL,
		SelectForRoomsExcludingBlacklistedSQL: selectJoinedHostsForRoomsExcludingBlacklistedSQL,
	}
}

// InsertJoinedHosts inserts a joined host event
func (t *joinedHostsTable) InsertJoinedHosts(ctx context.Context, roomID, eventID string, serverName spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.InsertSQL, roomID, eventID, serverName).Error
}

// DeleteJoinedHosts deletes joined hosts by event IDs
func (t *joinedHostsTable) DeleteJoinedHosts(ctx context.Context, eventIDs []string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteSQL, pq.Array(eventIDs)).Error
}

// DeleteJoinedHostsForRoom deletes all joined hosts for a room
func (t *joinedHostsTable) DeleteJoinedHostsForRoom(ctx context.Context, roomID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteForRoomSQL, roomID).Error
}

// SelectJoinedHostsWithTx selects joined hosts for a room (legacy compatibility)
func (t *joinedHostsTable) SelectJoinedHostsWithTx(ctx context.Context, roomID string) ([]types.JoinedHost, error) {
	return t.SelectJoinedHosts(ctx, roomID)
}

// SelectJoinedHosts selects joined hosts for a room
func (t *joinedHostsTable) SelectJoinedHosts(ctx context.Context, roomID string) ([]types.JoinedHost, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.SelectSQL, roomID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []types.JoinedHost
	for rows.Next() {
		var eventID, serverName string
		err := rows.Scan(&eventID, &serverName)
		if err != nil {
			return nil, err
		}
		result = append(result, types.JoinedHost{
			MemberEventID: eventID,
			ServerName:    spec.ServerName(serverName),
		})
	}
	return result, nil
}

// SelectAllJoinedHosts selects all unique joined hosts
func (t *joinedHostsTable) SelectAllJoinedHosts(ctx context.Context) ([]spec.ServerName, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.SelectAllSQL).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []spec.ServerName
	for rows.Next() {
		var serverName string
		err := rows.Scan(&serverName)
		if err != nil {
			return nil, err
		}
		result = append(result, spec.ServerName(serverName))
	}
	return result, nil
}

// SelectJoinedHostsForRooms selects joined hosts for multiple rooms, optionally excluding blacklisted
func (t *joinedHostsTable) SelectJoinedHostsForRooms(ctx context.Context, roomIDs []string, excludingBlacklisted bool) ([]spec.ServerName, error) {
	db := t.cm.Connection(ctx, true)
	var sql string
	if excludingBlacklisted {
		sql = t.SelectForRoomsExcludingBlacklistedSQL
	} else {
		sql = t.SelectForRoomsSQL
	}
	rows, err := db.Raw(sql, pq.Array(roomIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []spec.ServerName
	for rows.Next() {
		var serverName string
		err := rows.Scan(&serverName)
		if err != nil {
			return nil, err
		}
		result = append(result, spec.ServerName(serverName))
	}
	return result, nil
}
