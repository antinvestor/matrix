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
	"database/sql"
	"errors"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
)

const membershipSchema = `
-- The membership table is used to coordinate updates between the invite table
-- and the room state tables.
-- This table is updated in one of 3 ways:
--   1) The membership of a user changes within the current state of the room.
--   2) An invite is received outside of a room over federation.
--   3) An invite is rejected outside of a room over federation.
CREATE TABLE IF NOT EXISTS roomserver_membership (
	room_nid BIGINT NOT NULL,
	-- Numeric state key ID for the user ID this state is for.
	target_nid BIGINT NOT NULL,
	-- Numeric state key ID for the user ID who changed the state.
	-- This may be 0 since it is not always possible to identify the user that
	-- changed the state.
	sender_nid BIGINT NOT NULL DEFAULT 0,
	-- The state the user is in within this room.
	-- Default value is "membershipStateLeaveOrBan"
	membership_nid BIGINT NOT NULL DEFAULT 1,
	-- The numeric ID of the membership event.
	-- It refers to the join membership event if the membership_nid is join (3),
	-- and to the leave/ban membership event if the membership_nid is leave or
	-- ban (1).
	-- If the membership_nid is invite (2) and the user has been in the room
	-- before, it will refer to the previous leave/ban membership event, and will
	-- be equals to 0 (its default) if the user never joined the room before.
	-- This NID is updated if the join event gets updated (e.g. profile update),
	-- or if the user leaves/joins the room.
	event_nid BIGINT NOT NULL DEFAULT 0,
	-- Local target is true if the target_nid refers to a local user rather than
	-- a federated one. This is an optimisation for resetting state on federated
	-- room joins.
	target_local BOOLEAN NOT NULL DEFAULT false,
	forgotten BOOLEAN NOT NULL DEFAULT FALSE,
	UNIQUE (room_nid, target_nid)
);
`

const membershipSchemaRevert = `DROP TABLE IF EXISTS roomserver_membership;`

const insertMembershipSQL = "" +
	"INSERT INTO roomserver_membership (room_nid, target_nid, target_local)" +
	" VALUES ($1, $2, $3)" +
	" ON CONFLICT DO NOTHING"

const selectMembershipForUpdateSQL = "" +
	"SELECT membership_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND target_nid = $2 FOR UPDATE"

const selectMembershipFromRoomAndTargetSQL = "" +
	"SELECT membership_nid, event_nid, forgotten FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0 AND target_nid = $2"

const selectMembershipsFromRoomSQL = "" +
	"SELECT event_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0 and forgotten = false"

const selectMembershipsFromRoomAndMembershipSQL = "" +
	"SELECT event_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0 AND membership_nid = $2 and forgotten = false"

const selectLocalMembershipsFromRoomAndMembershipSQL = "" +
	"SELECT event_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0 AND membership_nid = $2" +
	" AND target_local = true and forgotten = false"

const selectLocalMembershipsFromRoomSQL = "" +
	"SELECT event_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0" +
	" AND target_local = true and forgotten = false"

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

var selectKnownUsersSQL = "" +
	"SELECT DISTINCT event_state_key FROM roomserver_membership INNER JOIN roomserver_event_state_keys ON " +
	"roomserver_membership.target_nid = roomserver_event_state_keys.event_state_key_nid" +
	" WHERE room_nid = ANY(" +
	"  SELECT DISTINCT room_nid FROM roomserver_membership WHERE target_nid=$1 AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) +
	") AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) + " AND event_state_key LIKE $2 LIMIT $3"

const selectLocalServerInRoomSQL = "" +
	"SELECT room_nid FROM roomserver_membership WHERE target_local = true AND membership_nid = $1 AND room_nid = $2 LIMIT 1"

const selectServerInRoomSQL = "" +
	"SELECT room_nid FROM roomserver_membership" +
	" JOIN roomserver_event_state_keys ON roomserver_membership.target_nid = roomserver_event_state_keys.event_state_key_nid" +
	" WHERE membership_nid = $1 AND room_nid = $2 AND event_state_key LIKE '%:' || $3 LIMIT 1"

const selectJoinedUsersSQL = "" +
	"SELECT DISTINCT target_nid" +
	" FROM roomserver_membership m" +
	" WHERE membership_nid > $1 AND target_nid = ANY($2)"

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

type membershipTable struct {
	cm                                             *sqlutil.Connections
	insertMembershipSQL                            string
	selectMembershipForUpdateSQL                   string
	selectMembershipFromRoomAndTargetSQL           string
	selectMembershipsFromRoomSQL                   string
	selectMembershipsFromRoomAndMembershipSQL      string
	selectLocalMembershipsFromRoomAndMembershipSQL string
	selectLocalMembershipsFromRoomSQL              string
	updateMembershipSQL                            string
	updateMembershipForgetRoom                     string
	deleteMembershipSQL                            string
	selectRoomsWithMembershipSQL                   string
	selectKnownUsersSQL                            string
	selectLocalServerInRoomSQL                     string
	selectServerInRoomSQL                          string
	selectJoinedUsersSQL                           string
	selectJoinedUsersSetForRoomsAndUserSQL         string
	selectJoinedUsersSetForRoomsSQL                string
}

func NewPostgresMembershipTable(cm *sqlutil.Connections) tables.Membership {
	return &membershipTable{
		cm:                                   cm,
		insertMembershipSQL:                  insertMembershipSQL,
		selectMembershipForUpdateSQL:         selectMembershipForUpdateSQL,
		selectMembershipFromRoomAndTargetSQL: selectMembershipFromRoomAndTargetSQL,
		selectMembershipsFromRoomSQL:         selectMembershipsFromRoomSQL,
		selectMembershipsFromRoomAndMembershipSQL:      selectMembershipsFromRoomAndMembershipSQL,
		selectLocalMembershipsFromRoomAndMembershipSQL: selectLocalMembershipsFromRoomAndMembershipSQL,
		selectLocalMembershipsFromRoomSQL:              selectLocalMembershipsFromRoomSQL,
		updateMembershipSQL:                            updateMembershipSQL,
		updateMembershipForgetRoom:                     updateMembershipForgetRoom,
		deleteMembershipSQL:                            deleteMembershipSQL,
		selectRoomsWithMembershipSQL:                   selectRoomsWithMembershipSQL,
		selectKnownUsersSQL:                            selectKnownUsersSQL,
		selectLocalServerInRoomSQL:                     selectLocalServerInRoomSQL,
		selectServerInRoomSQL:                          selectServerInRoomSQL,
		selectJoinedUsersSQL:                           selectJoinedUsersSQL,
		selectJoinedUsersSetForRoomsAndUserSQL:         selectJoinedUsersSetForRoomsAndUserSQL,
		selectJoinedUsersSetForRoomsSQL:                selectJoinedUsersSetForRoomsSQL,
	}
}

func (t *membershipTable) InsertMembership(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID, localTarget bool) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.insertMembershipSQL, roomNID, targetUserNID, localTarget).Error
}

func (t *membershipTable) SelectMembershipForUpdate(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID) (tables.MembershipState, error) {
	db := t.cm.Connection(ctx, true)
	var membership tables.MembershipState
	row := db.Raw(t.selectMembershipForUpdateSQL, roomNID, targetUserNID).Row()
	err := row.Scan(&membership)
	return membership, err
}

func (t *membershipTable) SelectMembershipFromRoomAndTarget(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID) (types.EventNID, tables.MembershipState, bool, error) {
	db := t.cm.Connection(ctx, true)
	var membership tables.MembershipState
	var eventNID types.EventNID
	var forgotten bool
	row := db.Raw(t.selectMembershipFromRoomAndTargetSQL, roomNID, targetUserNID).Row()
	err := row.Scan(&membership, &eventNID, &forgotten)
	return eventNID, membership, forgotten, err
}

func (t *membershipTable) SelectMembershipsFromRoom(ctx context.Context, roomNID types.RoomNID, localOnly bool) ([]types.EventNID, error) {
	db := t.cm.Connection(ctx, true)
	var sqlStmt string
	if localOnly {
		sqlStmt = t.selectLocalMembershipsFromRoomSQL
	} else {
		sqlStmt = t.selectMembershipsFromRoomSQL
	}
	rows, err := db.Raw(sqlStmt, roomNID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var eventNIDs []types.EventNID
	for rows.Next() {
		var eventNID types.EventNID
		if err = rows.Scan(&eventNID); err != nil {
			return nil, err
		}
		eventNIDs = append(eventNIDs, eventNID)
	}
	return eventNIDs, rows.Err()
}

func (t *membershipTable) SelectMembershipsFromRoomAndMembership(ctx context.Context, roomNID types.RoomNID, membership tables.MembershipState, localOnly bool) ([]types.EventNID, error) {
	db := t.cm.Connection(ctx, true)
	var sqlStmt string
	if localOnly {
		sqlStmt = t.selectLocalMembershipsFromRoomAndMembershipSQL
	} else {
		sqlStmt = t.selectMembershipsFromRoomAndMembershipSQL
	}
	rows, err := db.Raw(sqlStmt, roomNID, membership).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var eventNIDs []types.EventNID
	for rows.Next() {
		var eventNID types.EventNID
		if err = rows.Scan(&eventNID); err != nil {
			return nil, err
		}
		eventNIDs = append(eventNIDs, eventNID)
	}
	return eventNIDs, rows.Err()
}

func (t *membershipTable) UpdateMembership(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID, senderUserNID types.EventStateKeyNID, membership tables.MembershipState, eventNID types.EventNID, forgotten bool) (bool, error) {
	db := t.cm.Connection(ctx, false)
	res := db.Exec(t.updateMembershipSQL, roomNID, targetUserNID, senderUserNID, membership, eventNID, forgotten)
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

func (t *membershipTable) SelectRoomsWithMembership(ctx context.Context, userID types.EventStateKeyNID, membershipState tables.MembershipState) ([]types.RoomNID, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectRoomsWithMembershipSQL, membershipState, userID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var roomNIDs []types.RoomNID
	for rows.Next() {
		var roomNID types.RoomNID
		if err = rows.Scan(&roomNID); err != nil {
			return nil, err
		}
		roomNIDs = append(roomNIDs, roomNID)
	}
	return roomNIDs, rows.Err()
}

func (t *membershipTable) SelectKnownUsers(ctx context.Context, userID types.EventStateKeyNID, searchString string, limit int) ([]string, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectKnownUsersSQL, userID, searchString, limit).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []string
	for rows.Next() {
		var user string
		if err = rows.Scan(&user); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

func (t *membershipTable) UpdateForgetMembership(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID, forget bool) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.updateMembershipForgetRoom, roomNID, targetUserNID, forget).Error
}

func (t *membershipTable) SelectLocalServerInRoom(ctx context.Context, roomNID types.RoomNID) (bool, error) {
	db := t.cm.Connection(ctx, true)
	var exists bool
	row := db.Raw(t.selectLocalServerInRoomSQL, tables.MembershipStateJoin, roomNID).Row()
	err := row.Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return exists, err
}

func (t *membershipTable) SelectServerInRoom(ctx context.Context, roomNID types.RoomNID, serverName spec.ServerName) (bool, error) {
	db := t.cm.Connection(ctx, true)
	var exists bool
	row := db.Raw(t.selectServerInRoomSQL, tables.MembershipStateJoin, roomNID, serverName).Row()
	err := row.Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return exists, err
}

func (t *membershipTable) DeleteMembership(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteMembershipSQL, roomNID, targetUserNID).Error
}

func (t *membershipTable) SelectJoinedUsers(ctx context.Context, targetUserNIDs []types.EventStateKeyNID) ([]types.EventStateKeyNID, error) {
	db := t.cm.Connection(ctx, true)
	pqUserNIDs := make([]int64, len(targetUserNIDs))
	for i, nid := range targetUserNIDs {
		pqUserNIDs[i] = int64(nid)
	}
	rows, err := db.Raw(t.selectJoinedUsersSQL, tables.MembershipStateLeaveOrBan, pqUserNIDs).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []types.EventStateKeyNID
	for rows.Next() {
		var targetNID int64
		if err = rows.Scan(&targetNID); err != nil {
			return nil, err
		}
		result = append(result, types.EventStateKeyNID(targetNID))
	}
	return result, rows.Err()
}

func (t *membershipTable) SelectJoinedUsersSetForRooms(ctx context.Context, roomNIDs []types.RoomNID, userNIDs []types.EventStateKeyNID, localOnly bool) (map[types.EventStateKeyNID]int, error) {
	db := t.cm.Connection(ctx, true)
	pqRoomNIDs := make([]int64, len(roomNIDs))
	for i, nid := range roomNIDs {
		pqRoomNIDs[i] = int64(nid)
	}
	pqUserNIDs := make([]int64, len(userNIDs))
	for i, nid := range userNIDs {
		pqUserNIDs[i] = int64(nid)
	}
	rows, err := db.Raw(t.selectJoinedUsersSetForRoomsAndUserSQL, localOnly, pqRoomNIDs, pqUserNIDs).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[types.EventStateKeyNID]int)
	for rows.Next() {
		var targetNID int64
		var count int
		if err = rows.Scan(&targetNID, &count); err != nil {
			return nil, err
		}
		result[types.EventStateKeyNID(targetNID)] = count
	}
	return result, rows.Err()
}

func (t *membershipTable) SelectJoinedUsersSetForRoomsSQL(ctx context.Context, roomNIDs []types.RoomNID, localOnly bool) (map[types.EventStateKeyNID]int, error) {
	db := t.cm.Connection(ctx, true)
	pqRoomNIDs := make([]int64, len(roomNIDs))
	for i, nid := range roomNIDs {
		pqRoomNIDs[i] = int64(nid)
	}
	rows, err := db.Raw(t.selectJoinedUsersSetForRoomsSQL, localOnly, pqRoomNIDs).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[types.EventStateKeyNID]int)
	for rows.Next() {
		var targetNID int64
		var count int
		if err = rows.Scan(&targetNID, &count); err != nil {
			return nil, err
		}
		result[types.EventStateKeyNID(targetNID)] = count
	}
	return result, rows.Err()
}
