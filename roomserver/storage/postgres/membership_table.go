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
	"errors"
	"fmt"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
)

const membershipSchema = `
-- Stores the membership states for local users in local rooms.
CREATE TABLE IF NOT EXISTS roomserver_membership (
	-- The ID of the room.
	room_nid BIGINT NOT NULL,
	-- The ID of the user.
	target_nid BIGINT NOT NULL,
	-- The membership event type of the user in the room, e.g. "join".
	membership_nid BIGINT NOT NULL,
	-- The ID of the event that changed the membership state.
	event_nid BIGINT NOT NULL,
	-- The state_snapshot ID when the event was added.
	target_order BIGINT NOT NULL,
	-- The outbound topology position of the event associated with this membership update
	event_position BIGINT NOT NULL,
	-- Unique index on (room_nid, target_nid).
	-- There can only be one entry per user per room.
	CONSTRAINT roomserver_membership_unique UNIQUE (room_nid, target_nid),
	PRIMARY KEY(room_nid, target_nid)
);
CREATE INDEX IF NOT EXISTS roomserver_membership_room_nid_membership_nid_idx ON roomserver_membership (room_nid, membership_nid);
CREATE INDEX IF NOT EXISTS roomserver_membership_membership_nid_idx ON roomserver_membership (membership_nid) WHERE membership_nid > 3;
`

// SQL query constants for membership operations
const (
	// Insert or update membership state for a user (upsert)
	upsertMembershipSQL = "" +
		"INSERT INTO roomserver_membership AS m" +
		" (room_nid, target_nid, membership_nid, event_nid, target_order, event_position)" +
		" VALUES ($1, $2, $3, $4, $5, $6)" +
		" ON CONFLICT (room_nid, target_nid)" +
		" DO UPDATE SET membership_nid = $3, event_nid = $4, target_order = $5, event_position = $6" +
		" WHERE m.event_position <= $6"

	// Select membership states for a target user NID
	selectMembershipForUserSQL = "" +
		"SELECT room_nid, membership_nid FROM roomserver_membership" +
		" WHERE target_nid = $1"

	// Bulk select membership states for a list of room/user combinations
	bulkSelectMembershipSQL = "" +
		"SELECT room_nid, target_nid, membership_nid FROM roomserver_membership" +
		" WHERE (room_nid, target_nid) = ANY($1)"

	// Select event NIDs for all members who are joined to a room
	selectJoinedUsersInRoomSQL = "" +
		"SELECT target_nid, event_nid FROM roomserver_membership" +
		" WHERE room_nid = $1 AND membership_nid = $2"

	// Select membership state for a room and user
	selectMembershipForRoomAndTargetSQL = "" +
		"SELECT membership_nid FROM roomserver_membership" +
		" WHERE room_nid = $1 AND target_nid = $2"

	// Select all rooms a user is joined to
	selectRoomsWithMembershipSQL = "" +
		"SELECT room_nid FROM roomserver_membership" +
		" WHERE target_nid = $1 AND membership_nid = $2"

	// Select all joined members in a room
	selectJoinedMembersInRoomSQL = "" +
		"SELECT target_nid FROM roomserver_membership" +
		" WHERE room_nid = $1 AND membership_nid = $2"

	// Select membership events for a room and membership type
	selectMembershipsInRoomSQL = "" +
		"SELECT event_nid FROM roomserver_membership" +
		" WHERE room_nid = $1 AND membership_nid = $2"

	// Select all membership events for a room
	selectMembershipsInRoomByTargetSQL = "" +
		"SELECT target_nid, membership_nid FROM roomserver_membership" +
		" WHERE room_nid = $1"
		
	// Count members in a room by membership type
	countMembershipsInRoomSQL = "" +
		"SELECT COUNT(*) FROM roomserver_membership" +
		" WHERE room_nid = $1 AND membership_nid = $2"
)

type membershipStatements struct {
	cm *sqlutil.Connections
	
	// SQL statements stored as struct fields
	upsertMembershipStmt                 string
	selectMembershipForUserStmt          string
	bulkSelectMembershipStmt             string
	selectJoinedUsersInRoomStmt          string
	selectMembershipForRoomAndTargetStmt string
	selectRoomsWithMembershipStmt        string
	selectJoinedMembersInRoomStmt        string
	selectMembershipsInRoomStmt          string
	selectMembershipsInRoomByTargetStmt  string
	countMembershipsInRoomStmt           string
}

// NewPostgresMembershipTable creates a new PostgreSQL membership table and prepares all statements
func NewPostgresMembershipTable(ctx context.Context, cm *sqlutil.Connections) (tables.Membership, error) {
	// Create the table first
	if err := cm.Writer.ExecSQL(ctx, membershipSchema); err != nil {
		return nil, err
	}
	
	// Initialize the statements
	s := &membershipStatements{
		cm: cm,
		
		// Initialize SQL statement fields with the constants
		upsertMembershipStmt:                 upsertMembershipSQL,
		selectMembershipForUserStmt:          selectMembershipForUserSQL,
		bulkSelectMembershipStmt:             bulkSelectMembershipSQL,
		selectJoinedUsersInRoomStmt:          selectJoinedUsersInRoomSQL,
		selectMembershipForRoomAndTargetStmt: selectMembershipForRoomAndTargetSQL,
		selectRoomsWithMembershipStmt:        selectRoomsWithMembershipSQL,
		selectJoinedMembersInRoomStmt:        selectJoinedMembersInRoomSQL,
		selectMembershipsInRoomStmt:          selectMembershipsInRoomSQL,
		selectMembershipsInRoomByTargetStmt:  selectMembershipsInRoomByTargetSQL,
		countMembershipsInRoomStmt:           countMembershipsInRoomSQL,
	}
	
	return s, nil
}

// UpsertMembershipRoom inserts or updates a user's membership in a room
func (s *membershipStatements) UpsertMembershipRoom(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID,
	targetUserNID types.EventStateKeyNID,
	membership tables.MembershipState,
	eventNID types.EventNID,
	roomVersion string,
	targetOrder tables.TargetOrder,
	eventOrder tables.EventOrder,
) error {
	// Get database connection
	var result error
	
	if txn != nil {
		// Use existing transaction
		_, err := txn.ExecContext(
			ctx,
			s.upsertMembershipStmt,
			roomNID,
			targetUserNID,
			membership,
			eventNID,
			targetOrder,
			eventOrder,
		)
		result = err
	} else {
		// Use a new connection
		db := s.cm.Connection(ctx, false)
		result = db.Exec(
			s.upsertMembershipStmt,
			roomNID,
			targetUserNID,
			membership,
			eventNID,
			targetOrder,
			eventOrder,
		).Error
	}
	
	return result
}

// SelectMembershipForUser returns the membership states for a user
func (s *membershipStatements) SelectMembershipForUser(
	ctx context.Context, txn *sql.Tx,
	targetUserNID types.EventStateKeyNID,
) (memberships map[types.RoomNID]tables.MembershipState, err error) {
	// Get database connection
	var rows *sql.Rows
	
	if txn != nil {
		// Use existing transaction
		rows, err = txn.QueryContext(ctx, s.selectMembershipForUserStmt, targetUserNID)
	} else {
		// Use a new connection
		db := s.cm.Connection(ctx, true)
		rows, err = db.Raw(s.selectMembershipForUserStmt, targetUserNID).Rows()
	}
	
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectMembershipForUser: rows.close() failed")
	
	// Initialize the result map
	memberships = map[types.RoomNID]tables.MembershipState{}
	
	// Process rows
	var roomNID types.RoomNID
	var membershipState tables.MembershipState
	
	for rows.Next() {
		if err = rows.Scan(&roomNID, &membershipState); err != nil {
			return nil, err
		}
		memberships[roomNID] = membershipState
	}
	
	return memberships, rows.Err()
}

// BulkSelectMembership returns membership states for a list of room/user combinations
func (s *membershipStatements) BulkSelectMembership(
	ctx context.Context, txn *sql.Tx,
	roomNIDs []types.RoomNID,
	targetUserNIDs []types.EventStateKeyNID,
) (memberships map[types.RoomNID]map[types.EventStateKeyNID]tables.MembershipState, err error) {
	// Get database connection
	var rows *sql.Rows
	
	tuples := make([]string, len(roomNIDs))
	params := make([]interface{}, len(roomNIDs)*2)
	
	for i := range roomNIDs {
		tuples[i] = fmt.Sprintf("($%d, $%d)", (i*2)+1, (i*2)+2)
		params[i*2] = roomNIDs[i]
		params[i*2+1] = targetUserNIDs[i]
	}
	
	// Create the query
	query := fmt.Sprintf(
		s.bulkSelectMembershipStmt,
		strings.Join(tuples, ","),
	)
	
	if txn != nil {
		// Use existing transaction
		rows, err = txn.QueryContext(ctx, query, params...)
	} else {
		// Use a new connection
		db := s.cm.Connection(ctx, true)
		rows, err = db.Raw(query, params...).Rows()
	}
	
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "BulkSelectMembership: rows.close() failed")
	
	// Initialize the result map
	memberships = make(map[types.RoomNID]map[types.EventStateKeyNID]tables.MembershipState)
	
	// Process rows
	var roomNID types.RoomNID
	var targetUserNID types.EventStateKeyNID
	var membershipState tables.MembershipState
	
	for rows.Next() {
		if err = rows.Scan(&roomNID, &targetUserNID, &membershipState); err != nil {
			return nil, err
		}
		
		if _, ok := memberships[roomNID]; !ok {
			memberships[roomNID] = make(map[types.EventStateKeyNID]tables.MembershipState)
		}
		
		memberships[roomNID][targetUserNID] = membershipState
	}
	
	return memberships, rows.Err()
}

// SelectJoinedUsersInRoom returns a map of user NID -> event NID for all joined users in a room
func (s *membershipStatements) SelectJoinedUsersInRoom(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID,
) (map[types.EventStateKeyNID]types.EventNID, error) {
	// Get database connection
	var rows *sql.Rows
	var err error
	
	if txn != nil {
		// Use existing transaction
		rows, err = txn.QueryContext(
			ctx,
			s.selectJoinedUsersInRoomStmt,
			roomNID,
			tables.MembershipStateJoin,
		)
	} else {
		// Use a new connection
		db := s.cm.Connection(ctx, true)
		rows, err = db.Raw(
			s.selectJoinedUsersInRoomStmt,
			roomNID,
			tables.MembershipStateJoin,
		).Rows()
	}
	
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectJoinedUsersInRoom: rows.close() failed")
	
	// Initialize the result map
	result := map[types.EventStateKeyNID]types.EventNID{}
	
	// Process rows
	var userNID types.EventStateKeyNID
	var eventNID types.EventNID
	
	for rows.Next() {
		if err = rows.Scan(&userNID, &eventNID); err != nil {
			return nil, err
		}
		result[userNID] = eventNID
	}
	
	return result, rows.Err()
}

// SelectMembershipForRoomAndTarget returns the membership state for a user in a room
func (s *membershipStatements) SelectMembershipForRoomAndTarget(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID,
	targetUserNID types.EventStateKeyNID,
) (membership tables.MembershipState, err error) {
	// Get database connection
	
	if txn != nil {
		// Use existing transaction
		err = txn.QueryRowContext(
			ctx,
			s.selectMembershipForRoomAndTargetStmt,
			roomNID,
			targetUserNID,
		).Scan(&membership)
	} else {
		// Use a new connection
		db := s.cm.Connection(ctx, true)
		err = db.Raw(
			s.selectMembershipForRoomAndTargetStmt,
			roomNID,
			targetUserNID,
		).Row().Scan(&membership)
	}
	
	if err == sql.ErrNoRows {
		// If there is no membership state then the user is not a member of the room.
		return tables.MembershipStateLeaveOrBan, nil
	}
	
	return
}

// SelectRoomsWithMembership returns a list of room NIDs that a user has membership with
func (s *membershipStatements) SelectRoomsWithMembership(
	ctx context.Context, txn *sql.Tx,
	targetUserNID types.EventStateKeyNID,
	membershipState tables.MembershipState,
) (roomNIDs []types.RoomNID, err error) {
	// Get database connection
	var rows *sql.Rows
	
	if txn != nil {
		// Use existing transaction
		rows, err = txn.QueryContext(
			ctx,
			s.selectRoomsWithMembershipStmt,
			targetUserNID,
			membershipState,
		)
	} else {
		// Use a new connection
		db := s.cm.Connection(ctx, true)
		rows, err = db.Raw(
			s.selectRoomsWithMembershipStmt,
			targetUserNID,
			membershipState,
		).Rows()
	}
	
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectRoomsWithMembership: rows.close() failed")
	
	// Process rows
	var roomNID types.RoomNID
	
	for rows.Next() {
		if err = rows.Scan(&roomNID); err != nil {
			return nil, err
		}
		roomNIDs = append(roomNIDs, roomNID)
	}
	
	return roomNIDs, rows.Err()
}

// SelectJoinedMembersInRoom returns a list of all users who are joined to a room
func (s *membershipStatements) SelectJoinedMembersInRoom(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID,
) ([]types.EventStateKeyNID, error) {
	// Get database connection
	var rows *sql.Rows
	var err error
	
	if txn != nil {
		// Use existing transaction
		rows, err = txn.QueryContext(
			ctx,
			s.selectJoinedMembersInRoomStmt,
			roomNID,
			tables.MembershipStateJoin,
		)
	} else {
		// Use a new connection
		db := s.cm.Connection(ctx, true)
		rows, err = db.Raw(
			s.selectJoinedMembersInRoomStmt,
			roomNID,
			tables.MembershipStateJoin,
		).Rows()
	}
	
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectJoinedMembersInRoom: rows.close() failed")
	
	// Process rows
	var targetUserNIDs []types.EventStateKeyNID
	var targetUserNID types.EventStateKeyNID
	
	for rows.Next() {
		if err = rows.Scan(&targetUserNID); err != nil {
			return nil, err
		}
		targetUserNIDs = append(targetUserNIDs, targetUserNID)
	}
	
	return targetUserNIDs, rows.Err()
}

// SelectMembershipsInRoom returns the event NIDs for all memberships in a room with a specific membership state
func (s *membershipStatements) SelectMembershipsInRoom(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID,
	membershipState tables.MembershipState,
) (eventNIDs []types.EventNID, err error) {
	// Get database connection
	var rows *sql.Rows
	
	if txn != nil {
		// Use existing transaction
		rows, err = txn.QueryContext(
			ctx,
			s.selectMembershipsInRoomStmt,
			roomNID,
			membershipState,
		)
	} else {
		// Use a new connection
		db := s.cm.Connection(ctx, true)
		rows, err = db.Raw(
			s.selectMembershipsInRoomStmt,
			roomNID,
			membershipState,
		).Rows()
	}
	
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectMembershipsInRoom: rows.close() failed")
	
	// Process rows
	var eventNID types.EventNID
	
	for rows.Next() {
		if err = rows.Scan(&eventNID); err != nil {
			return nil, err
		}
		eventNIDs = append(eventNIDs, eventNID)
	}
	
	return eventNIDs, rows.Err()
}

// SelectMembershipsInRoomByTarget returns a map of target user NID -> membership state for all users in a room
func (s *membershipStatements) SelectMembershipsInRoomByTarget(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID,
) (map[types.EventStateKeyNID]tables.MembershipState, error) {
	// Get database connection
	var rows *sql.Rows
	var err error
	
	if txn != nil {
		// Use existing transaction
		rows, err = txn.QueryContext(
			ctx,
			s.selectMembershipsInRoomByTargetStmt,
			roomNID,
		)
	} else {
		// Use a new connection
		db := s.cm.Connection(ctx, true)
		rows, err = db.Raw(
			s.selectMembershipsInRoomByTargetStmt,
			roomNID,
		).Rows()
	}
	
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectMembershipsInRoomByTarget: rows.close() failed")
	
	// Initialize result map
	result := make(map[types.EventStateKeyNID]tables.MembershipState)
	
	// Process rows
	var targetUserNID types.EventStateKeyNID
	var membershipState tables.MembershipState
	
	for rows.Next() {
		if err = rows.Scan(&targetUserNID, &membershipState); err != nil {
			return nil, err
		}
		result[targetUserNID] = membershipState
	}
	
	return result, rows.Err()
}

// CountMembershipsInRoom counts the number of members with a given membership state in a room
func (s *membershipStatements) CountMembershipsInRoom(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID,
	membershipState tables.MembershipState,
) (count int, err error) {
	// Get database connection
	
	if txn != nil {
		// Use existing transaction
		err = txn.QueryRowContext(
			ctx,
			s.countMembershipsInRoomStmt,
			roomNID,
			membershipState,
		).Scan(&count)
	} else {
		// Use a new connection
		db := s.cm.Connection(ctx, true)
		err = db.Raw(
			s.countMembershipsInRoomStmt,
			roomNID,
			membershipState,
		).Row().Scan(&count)
	}
	
	return
}
