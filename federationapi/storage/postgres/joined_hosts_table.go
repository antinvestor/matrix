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
	"github.com/antinvestor/matrix/federationapi/storage/tables"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/types"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/pitabwire/frame"
	"github.com/lib/pq"
)

// Schema for the joined hosts table
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

// Schema revert for the joined hosts table
const joinedHostsSchemaRevert = `
DROP TABLE IF EXISTS federationsender_joined_hosts;
`

// SQL for inserting a joined host into the table
const insertJoinedHostsSQL = "" +
	"INSERT INTO federationsender_joined_hosts (room_id, event_id, server_name)" +
	" VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"

// SQL for deleting joined hosts with specific event IDs
const deleteJoinedHostsSQL = "" +
	"DELETE FROM federationsender_joined_hosts WHERE event_id = ANY($1)"

// SQL for deleting all joined hosts for a specific room
const deleteJoinedHostsForRoomSQL = "" +
	"DELETE FROM federationsender_joined_hosts WHERE room_id = $1"

// SQL for selecting all joined hosts in a room
const selectJoinedHostsSQL = "" +
	"SELECT event_id, server_name FROM federationsender_joined_hosts" +
	" WHERE room_id = $1"

// SQL for selecting all joined hosts across all rooms
const selectAllJoinedHostsSQL = "" +
	"SELECT DISTINCT server_name FROM federationsender_joined_hosts"

// SQL for selecting joined hosts for specific rooms
const selectJoinedHostsForRoomsSQL = "" +
	"SELECT DISTINCT server_name FROM federationsender_joined_hosts WHERE room_id = ANY($1)"

// SQL for selecting joined hosts for specific rooms excluding blacklisted servers
const selectJoinedHostsForRoomsExcludingBlacklistedSQL = "" +
	"SELECT DISTINCT server_name FROM federationsender_joined_hosts j WHERE room_id = ANY($1) AND NOT EXISTS (" +
	"  SELECT server_name FROM federationsender_blacklist WHERE j.server_name = server_name" +
	");"

// joinedHostsTable stores information about which servers are joined to which rooms
type joinedHostsTable struct {
	cm *sqlutil.Connections
	// SQL query string fields, initialized at construction
	insertJoinedHostsSQL                             string
	deleteJoinedHostsSQL                             string
	deleteJoinedHostsForRoomSQL                      string
	selectJoinedHostsSQL                             string
	selectAllJoinedHostsSQL                          string
	selectJoinedHostsForRoomsSQL                     string
	selectJoinedHostsForRoomsExcludingBlacklistedSQL string
}

// NewPostgresJoinedHostsTable creates a new postgres joined hosts table
func NewPostgresJoinedHostsTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationJoinedHosts, error) {
	s := &joinedHostsTable{
		cm:                           cm,
		insertJoinedHostsSQL:         insertJoinedHostsSQL,
		deleteJoinedHostsSQL:         deleteJoinedHostsSQL,
		deleteJoinedHostsForRoomSQL:  deleteJoinedHostsForRoomSQL,
		selectJoinedHostsSQL:         selectJoinedHostsSQL,
		selectAllJoinedHostsSQL:      selectAllJoinedHostsSQL,
		selectJoinedHostsForRoomsSQL: selectJoinedHostsForRoomsSQL,
		selectJoinedHostsForRoomsExcludingBlacklistedSQL: selectJoinedHostsForRoomsExcludingBlacklistedSQL,
	}

	// Perform schema migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "federationapi_joined_hosts_table_schema_001",
		Patch:       joinedHostsSchema,
		RevertPatch: joinedHostsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertJoinedHosts adds a joined host to the table
func (s *joinedHostsTable) InsertJoinedHosts(
	ctx context.Context,

	roomID, eventID string,
	serverName spec.ServerName,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.insertJoinedHostsSQL, roomID, eventID, serverName).Error
}

// DeleteJoinedHosts removes joined hosts with specific event IDs
func (s *joinedHostsTable) DeleteJoinedHosts(
	ctx context.Context, eventIDs []string,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteJoinedHostsSQL, pq.StringArray(eventIDs)).Error
}

// DeleteJoinedHostsForRoom removes all joined hosts for a specific room
func (s *joinedHostsTable) DeleteJoinedHostsForRoom(
	ctx context.Context, roomID string,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteJoinedHostsForRoomSQL, roomID).Error
}

// SelectJoinedHosts gets all joined hosts in a room
func (s *joinedHostsTable) SelectJoinedHosts(
	ctx context.Context, roomID string,
) ([]types.JoinedHost, error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectJoinedHostsSQL, roomID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "joinedHostsFromDB: rows.close() failed")

	var result []types.JoinedHost
	for rows.Next() {
		var eventID, serverName string
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

// SelectAllJoinedHosts gets all joined hosts across all rooms
func (s *joinedHostsTable) SelectAllJoinedHosts(
	ctx context.Context,
) ([]spec.ServerName, error) {
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(s.selectAllJoinedHostsSQL).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectAllJoinedHosts: rows.close() failed")

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

// SelectJoinedHostsForRooms gets joined hosts for specific rooms
func (s *joinedHostsTable) SelectJoinedHostsForRooms(
	ctx context.Context, roomIDs []string, excludingBlacklisted bool,
) ([]spec.ServerName, error) {
	db := s.cm.Connection(ctx, true)

	query := s.selectJoinedHostsForRoomsSQL
	if excludingBlacklisted {
		query = s.selectJoinedHostsForRoomsExcludingBlacklistedSQL
	}

	rows, err := db.Raw(query, pq.StringArray(roomIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectJoinedHostsForRoomsStmt: rows.close() failed")

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
