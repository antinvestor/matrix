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
	"database/sql"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
)

// Schema for the membership table
const membershipSchema = `
-- The membership table is used to coordinate updates between the invite table
-- and the room state tables.
-- This table is updated in a transaction with the invite table, the room state
-- table and the event JSON table.
-- This table is used to look up users that are joined to a room when calculating
-- the joined hosts in a room.
-- It is also used to check which users we need to tell a remote server about
-- when joining a room.
CREATE TABLE IF NOT EXISTS roomserver_membership (
    -- The ID of the room.
    room_nid BIGINT NOT NULL,
    -- The state key of the event, the user ID the event refers to.
    target_nid BIGINT NOT NULL,
    -- The state key of the event, whether the event refers to a local user or not.
    -- This is the server name in the user ID.
    target_local BOOLEAN NOT NULL,
    -- The ID of the event.
    event_nid BIGINT NOT NULL DEFAULT 0,
    -- The state of the membership.
    -- Contains one of "JOIN", "INVITE", "LEAVE", "BAN"
    -- See state_block.go for the list of valid values.
    -- Default 0 means we never tried to find out the membership for this user.
    membership_nid SMALLINT NOT NULL DEFAULT 0,
    -- The user ID of the sender of the event.
    -- This is used to set the display name.
    sender_nid BIGINT NOT NULL DEFAULT 0,
    -- Whether the user has been marked as forgotten.
    forgotten BOOLEAN NOT NULL DEFAULT false,
    UNIQUE (room_nid, target_nid)
);

CREATE INDEX IF NOT EXISTS roomserver_membership_room_nid ON roomserver_membership (room_nid);
CREATE INDEX IF NOT EXISTS roomserver_membership_event_nid ON roomserver_membership (event_nid);
CREATE INDEX IF NOT EXISTS roomserver_membership_target_nid ON roomserver_membership (target_nid);
`

// Schema revert script for migration purposes
const membershipSchemaRevert = `DROP TABLE IF EXISTS roomserver_membership;`

// SQL queries for the membership table
var selectJoinedUsersSetForRoomsAndUserSQL = "" +
	"SELECT target_nid, COUNT(room_nid) FROM roomserver_membership" +
	" WHERE (target_local OR $1 = false)" +
	" AND room_nid = ANY($2) AND target_nid = ANY($3)" +
	" AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) +
	" AND forgotten = false" +
	" GROUP BY target_nid"

var selectJoinedUsersSetForRoomsSQL = "" +
	"SELECT target_nid, COUNT(room_nid) FROM roomserver_membership" +
	" WHERE (target_local OR $1 = false) " +
	" AND room_nid = ANY($2)" +
	" AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) +
	" AND forgotten = false" +
	" GROUP BY target_nid"

// Insert a row in to membership table so that it can be locked by SELECT FOR UPDATE
const insertMembershipSQL = "" +
	"INSERT INTO roomserver_membership (room_nid, target_nid, target_local)" +
	" VALUES ($1, $2, $3)" +
	" ON CONFLICT DO NOTHING"

const selectMembershipFromRoomAndTargetSQL = "" +
	"SELECT membership_nid, event_nid, forgotten FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0 AND target_nid = $2"

const selectMembershipsFromRoomAndMembershipSQL = "" +
	"SELECT event_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0 AND membership_nid = $2 and forgotten = false"

const selectLocalMembershipsFromRoomAndMembershipSQL = "" +
	"SELECT event_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0 AND membership_nid = $2" +
	" AND target_local = true and forgotten = false"

const selectMembershipsFromRoomSQL = "" +
	"SELECT event_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0 and forgotten = false"

const selectLocalMembershipsFromRoomSQL = "" +
	"SELECT event_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0" +
	" AND target_local = true and forgotten = false"

const selectMembershipForUpdateSQL = "" +
	"SELECT membership_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND target_nid = $2 FOR UPDATE"

const updateMembershipSQL = "" +
	"UPDATE roomserver_membership SET sender_nid = $3, membership_nid = $4, event_nid = $5, forgotten = $6" +
	" WHERE room_nid = $1 AND target_nid = $2"

const updateMembershipForgetRoom = "" +
	"UPDATE roomserver_membership SET forgotten = $3" +
	" WHERE room_nid = $1 AND target_nid = $2"

const deleteMembershipSQL = "" +
	"DELETE FROM roomserver_membership WHERE room_nid = $1 AND target_nid = $2"

const selectRoomsWithMembershipSQL = "" +
	"SELECT room_nid FROM roomserver_membership WHERE membership_nid = $1 AND target_nid = $2 and forgotten = false"

// selectKnownUsersSQL uses a sub-select statement here to find rooms that the user is
// joined to. Since this information is used to populate the user directory, we will
// only return users that the user would ordinarily be able to see anyway.
var selectKnownUsersSQL = "" +
	"SELECT DISTINCT event_state_key FROM roomserver_membership INNER JOIN roomserver_event_state_keys ON " +
	"roomserver_membership.target_nid = roomserver_event_state_keys.event_state_key_nid" +
	" WHERE room_nid = ANY(" +
	"  SELECT DISTINCT room_nid FROM roomserver_membership WHERE target_nid=$1 AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) +
	") AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) + " AND event_state_key LIKE $2 LIMIT $3"

// selectLocalServerInRoomSQL is an optimised case for checking if we, the local server,
// are in the room by using the target_local column of the membership table. Normally when
// we want to know if a server is in a room, we have to unmarshal the entire room state which
// is expensive. The presence of a single row from this query suggests we're still in the
// room, no rows returned suggests we aren't.
const selectLocalServerInRoomSQL = "" +
	"SELECT room_nid FROM roomserver_membership WHERE target_local = true AND membership_nid = $1 AND room_nid = $2 LIMIT 1"

// selectServerMembersInRoomSQL is an optimised case for checking for server members in a room.
// The JOIN is significantly leaner than the previous case of looking up event NIDs and reading the
// membership events from the database, as the JOIN query amounts to little more than two index
// scans which are very fast. The presence of a single row from this query suggests the server is
// in the room, no rows returned suggests they aren't.
const selectServerInRoomSQL = "" +
	"SELECT room_nid FROM roomserver_membership" +
	" JOIN roomserver_event_state_keys ON roomserver_membership.target_nid = roomserver_event_state_keys.event_state_key_nid" +
	" WHERE membership_nid = $1 AND room_nid = $2 AND event_state_key LIKE '%:' || $3 LIMIT 1"

const selectJoinedUsersSQL = `
SELECT DISTINCT target_nid
FROM roomserver_membership m
WHERE membership_nid > $1 AND target_nid = ANY($2)
`

// membershipTable implements the tables.Membership interface using GORM
type membershipTable struct {
	cm *sqlutil.Connections

	// SQL query strings loaded from constants
	selectJoinedUsersSetForRoomsAndUserSQL         string
	selectJoinedUsersSetForRoomsSQL                string
	insertMembershipSQL                            string
	selectMembershipFromRoomAndTargetSQL           string
	selectMembershipsFromRoomAndMembershipSQL      string
	selectLocalMembershipsFromRoomAndMembershipSQL string
	selectMembershipsFromRoomSQL                   string
	selectLocalMembershipsFromRoomSQL              string
	selectMembershipForUpdateSQL                   string
	updateMembershipSQL                            string
	selectRoomsWithMembershipSQL                   string
	selectKnownUsersSQL                            string
	updateMembershipForgetRoomSQL                  string
	selectLocalServerInRoomSQL                     string
	selectServerInRoomSQL                          string
	deleteMembershipSQL                            string
	selectJoinedUsersSQL                           string
}

// NewPostgresMembershipTable prepares the membership table with the connection manager
func NewPostgresMembershipTable(ctx context.Context, cm *sqlutil.Connections) (tables.Membership, error) {
	// Create the table if it doesn't exist using migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "roomserver_membership_table_schema_001",
		Patch:       membershipSchema,
		RevertPatch: membershipSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Initialize the table struct with just the connection manager
	t := &membershipTable{
		cm: cm,

		// Initialize SQL query strings from constants
		selectJoinedUsersSetForRoomsAndUserSQL:         selectJoinedUsersSetForRoomsAndUserSQL,
		selectJoinedUsersSetForRoomsSQL:                selectJoinedUsersSetForRoomsSQL,
		insertMembershipSQL:                            insertMembershipSQL,
		selectMembershipFromRoomAndTargetSQL:           selectMembershipFromRoomAndTargetSQL,
		selectMembershipsFromRoomAndMembershipSQL:      selectMembershipsFromRoomAndMembershipSQL,
		selectLocalMembershipsFromRoomAndMembershipSQL: selectLocalMembershipsFromRoomAndMembershipSQL,
		selectMembershipsFromRoomSQL:                   selectMembershipsFromRoomSQL,
		selectLocalMembershipsFromRoomSQL:              selectLocalMembershipsFromRoomSQL,
		selectMembershipForUpdateSQL:                   selectMembershipForUpdateSQL,
		updateMembershipSQL:                            updateMembershipSQL,
		selectRoomsWithMembershipSQL:                   selectRoomsWithMembershipSQL,
		selectKnownUsersSQL:                            selectKnownUsersSQL,
		updateMembershipForgetRoomSQL:                  updateMembershipForgetRoom,
		selectLocalServerInRoomSQL:                     selectLocalServerInRoomSQL,
		selectServerInRoomSQL:                          selectServerInRoomSQL,
		deleteMembershipSQL:                            deleteMembershipSQL,
		selectJoinedUsersSQL:                           selectJoinedUsersSQL,
	}

	return t, nil
}

func (t *membershipTable) SelectJoinedUsers(
	ctx context.Context,
	targetUserNIDs []types.EventStateKeyNID,
) ([]types.EventStateKeyNID, error) {
	result := make([]types.EventStateKeyNID, 0, len(targetUserNIDs))
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.selectJoinedUsersSQL, tables.MembershipStateLeaveOrBan, pq.Array(targetUserNIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectJoinedUsers: rows.close() failed")

	var targetNID types.EventStateKeyNID
	for rows.Next() {
		if err = rows.Scan(&targetNID); err != nil {
			return nil, err
		}
		result = append(result, targetNID)
	}

	return result, rows.Err()
}

func (t *membershipTable) InsertMembership(
	ctx context.Context,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID,
	localTarget bool,
) error {
	db := t.cm.Connection(ctx, false)

	result := db.Exec(t.insertMembershipSQL, roomNID, targetUserNID, localTarget)
	return result.Error
}

func (t *membershipTable) SelectMembershipForUpdate(
	ctx context.Context,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID,
) (membership tables.MembershipState, err error) {
	db := t.cm.Connection(ctx, false)

	row := db.Raw(t.selectMembershipForUpdateSQL, roomNID, targetUserNID).Row()
	err = row.Scan(&membership)
	return
}

func (t *membershipTable) SelectMembershipFromRoomAndTarget(
	ctx context.Context,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID,
) (eventNID types.EventNID, membership tables.MembershipState, forgotten bool, err error) {
	db := t.cm.Connection(ctx, true)

	err = db.Raw(t.selectMembershipFromRoomAndTargetSQL, roomNID, targetUserNID).Row().Scan(&membership, &eventNID, &forgotten)
	return
}

func (t *membershipTable) SelectMembershipsFromRoom(
	ctx context.Context,
	roomNID types.RoomNID, localOnly bool,
) (eventNIDs []types.EventNID, err error) {
	db := t.cm.Connection(ctx, true)

	var rows *sql.Rows
	if localOnly {
		rows, err = db.Raw(t.selectLocalMembershipsFromRoomSQL, roomNID).Rows()
	} else {
		rows, err = db.Raw(t.selectMembershipsFromRoomSQL, roomNID).Rows()
	}
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectMembershipsFromRoom: rows.close() failed")

	var eNID types.EventNID
	for rows.Next() {
		if err = rows.Scan(&eNID); err != nil {
			return
		}
		eventNIDs = append(eventNIDs, eNID)
	}
	return eventNIDs, rows.Err()
}

func (t *membershipTable) SelectMembershipsFromRoomAndMembership(
	ctx context.Context,
	roomNID types.RoomNID, membership tables.MembershipState, localOnly bool,
) (eventNIDs []types.EventNID, err error) {
	db := t.cm.Connection(ctx, true)

	var rows *sql.Rows
	if localOnly {
		rows, err = db.Raw(t.selectLocalMembershipsFromRoomAndMembershipSQL, roomNID, membership).Rows()
	} else {
		rows, err = db.Raw(t.selectMembershipsFromRoomAndMembershipSQL, roomNID, membership).Rows()
	}
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectMembershipsFromRoomAndMembership: rows.close() failed")

	var eNID types.EventNID
	for rows.Next() {
		if err = rows.Scan(&eNID); err != nil {
			return
		}
		eventNIDs = append(eventNIDs, eNID)
	}
	return eventNIDs, rows.Err()
}

func (t *membershipTable) UpdateMembership(
	ctx context.Context,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID, senderUserNID types.EventStateKeyNID, membership tables.MembershipState,
	eventNID types.EventNID, forgotten bool,
) (bool, error) {
	db := t.cm.Connection(ctx, false)

	result := db.Exec(t.updateMembershipSQL, roomNID, targetUserNID, senderUserNID, membership, eventNID, forgotten)
	if result.Error != nil {
		return false, result.Error
	}

	return result.RowsAffected > 0, nil
}

func (t *membershipTable) SelectRoomsWithMembership(
	ctx context.Context,
	userID types.EventStateKeyNID, membershipState tables.MembershipState,
) ([]types.RoomNID, error) {
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.selectRoomsWithMembershipSQL, membershipState, userID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectRoomsWithMembership: rows.close() failed")

	var roomNIDs []types.RoomNID
	var roomNID types.RoomNID
	for rows.Next() {
		if err := rows.Scan(&roomNID); err != nil {
			return nil, err
		}
		roomNIDs = append(roomNIDs, roomNID)
	}
	return roomNIDs, rows.Err()
}

func (t *membershipTable) SelectJoinedUsersSetForRooms(
	ctx context.Context,
	roomNIDs []types.RoomNID,
	userNIDs []types.EventStateKeyNID,
	localOnly bool,
) (map[types.EventStateKeyNID]int, error) {
	db := t.cm.Connection(ctx, true)

	var rows *sql.Rows
	var err error
	if len(userNIDs) > 0 {
		rows, err = db.Raw(t.selectJoinedUsersSetForRoomsAndUserSQL, localOnly, pq.Array(roomNIDs), pq.Array(userNIDs)).Rows()
	} else {
		rows, err = db.Raw(t.selectJoinedUsersSetForRoomsSQL, localOnly, pq.Array(roomNIDs)).Rows()
	}

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectJoinedUsersSetForRooms: rows.close() failed")

	result := make(map[types.EventStateKeyNID]int)
	var userID types.EventStateKeyNID
	var count int
	for rows.Next() {
		if err = rows.Scan(&userID, &count); err != nil {
			return nil, err
		}
		result[userID] = count
	}
	return result, rows.Err()
}

func (t *membershipTable) SelectKnownUsers(
	ctx context.Context,
	userID types.EventStateKeyNID, searchString string, limit int,
) ([]string, error) {
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.selectKnownUsersSQL, userID, fmt.Sprintf("%%%s%%", searchString), limit).Rows()
	if err != nil {
		return nil, err
	}

	result := []string{}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectKnownUsers: rows.close() failed")

	var resUserID string
	for rows.Next() {
		if err = rows.Scan(&resUserID); err != nil {
			return nil, err
		}
		result = append(result, resUserID)
	}
	return result, rows.Err()
}

func (t *membershipTable) UpdateForgetMembership(
	ctx context.Context,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID, forget bool,
) error {
	db := t.cm.Connection(ctx, false)

	result := db.Exec(t.updateMembershipForgetRoomSQL, roomNID, targetUserNID, forget)
	return result.Error
}

func (t *membershipTable) SelectLocalServerInRoom(
	ctx context.Context,
	roomNID types.RoomNID,
) (bool, error) {
	db := t.cm.Connection(ctx, true)

	var nid types.RoomNID
	row := db.Raw(t.selectLocalServerInRoomSQL, tables.MembershipStateJoin, roomNID).Row()
	err := row.Scan(&nid)
	if err != nil {
		if sqlutil.ErrorIsNoRows(err) {
			return false, nil
		}
		return false, err
	}
	found := nid > 0
	return found, nil
}

func (t *membershipTable) SelectServerInRoom(
	ctx context.Context,
	roomNID types.RoomNID, serverName spec.ServerName,
) (bool, error) {
	db := t.cm.Connection(ctx, true)

	var nid types.RoomNID
	err := db.Raw(t.selectServerInRoomSQL, tables.MembershipStateJoin, roomNID, serverName).Row().Scan(&nid)
	if err != nil {
		if sqlutil.ErrorIsNoRows(err) {
			return false, nil
		}
		return false, err
	}
	return roomNID == nid, nil
}

func (t *membershipTable) DeleteMembership(
	ctx context.Context,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID,
) error {
	db := t.cm.Connection(ctx, false)

	result := db.Exec(t.deleteMembershipSQL, roomNID, targetUserNID)
	return result.Error
}
