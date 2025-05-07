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

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
)

const inviteSchema = `
CREATE TABLE IF NOT EXISTS roomserver_invites (
	-- The string ID of the invite event itself.
	-- We can't use a numeric event ID here because we don't always have
	-- enough information to store an invite in the event table.
	-- In particular we don't always have a chain of auth_events for invites
	-- received over federation.
	invite_event_id TEXT PRIMARY KEY,
	-- The numeric ID of the room the invite m.room.member event is in.
	room_nid BIGINT NOT NULL,
	-- The numeric ID for the state key of the invite m.room.member event.
	-- This tells us who the invite is for.
	-- This is used to query the active invites for a user.
	target_nid BIGINT NOT NULL,
	-- The numeric ID for the sender of the invite m.room.member event.
	-- This tells us who sent the invite.
	-- This is used to work out which matrix server we should talk to when
	-- we try to join the room.
	sender_nid BIGINT NOT NULL DEFAULT 0,
	-- This is used to track whether the invite is still active.
	-- This is set implicitly when processing new join and leave events and
	-- explicitly when rejecting events over federation.
	retired BOOLEAN NOT NULL DEFAULT FALSE,
	-- The invite event JSON.
	invite_event_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS roomserver_invites_active_idx ON roomserver_invites (target_nid, room_nid)
	WHERE NOT retired;
`

// SQL query constants for the invites table
const (
	// insertInviteEventSQL inserts a new invite event into the invites table
	insertInviteEventSQL = "" +
		"INSERT INTO roomserver_invites (invite_event_id, room_nid, target_nid, sender_nid, invite_event_json)" +
		" VALUES ($1, $2, $3, $4, $5)" +
		" ON CONFLICT DO NOTHING"

	// selectInviteActiveForUserInRoomSQL selects active invites for a user in a room
	selectInviteActiveForUserInRoomSQL = "" +
		"SELECT invite_event_id, sender_nid, invite_event_json FROM roomserver_invites" +
		" WHERE target_nid = $1 AND room_nid = $2" +
		" AND NOT retired"

	// updateInviteRetiredSQL marks invites as retired (no longer active)
	updateInviteRetiredSQL = "" +
		"UPDATE roomserver_invites SET retired = TRUE" +
		" WHERE room_nid = $1 AND target_nid = $2 AND NOT retired" +
		" RETURNING invite_event_id"
)

type inviteTable struct {
	cm *sqlutil.Connections
	
	// SQL statements stored as struct fields
	insertInviteEventStmt              string
	updateInviteRetiredStmt            string
	selectInviteActiveForUserInRoomStmt string
}

// NewPostgresInvitesTable creates a new PostgreSQL invites table and initializes it
func NewPostgresInvitesTable(ctx context.Context, cm *sqlutil.Connections) (tables.Invites, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(inviteSchema).Error; err != nil {
		return nil, err
	}

	// Initialize the table
	s := &inviteTable{
		cm: cm,
		// Initialize SQL statement fields with the constants
		insertInviteEventStmt:              insertInviteEventSQL,
		updateInviteRetiredStmt:            updateInviteRetiredSQL,
		selectInviteActiveForUserInRoomStmt: selectInviteActiveForUserInRoomSQL,
	}

	return s, nil
}

// InsertInviteEvent adds an invite event to the table.
// Returns a boolean that is true if the invite event was inserted into the database,
// or false if the event was already present in the database.
func (s *inviteTable) InsertInviteEvent(
	ctx context.Context,
	inviteEventID string, roomNID types.RoomNID,
	targetUserNID, senderUserNID types.EventStateKeyNID,
	inviteEventJSON []byte,
) (bool, error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)

	result := db.Exec(
		s.insertInviteEventStmt,
		inviteEventID,
		roomNID,
		targetUserNID,
		senderUserNID,
		inviteEventJSON,
	)
	if result.Error != nil {
		return false, result.Error
	}
	
	// Check if anything was inserted
	return result.RowsAffected > 0, nil
}

// UpdateInviteRetired marks the invite as retired (no longer active).
// This effectively "removes" the invite from the active invites list.
// Returns a list of invite event IDs that were retired.
func (s *inviteTable) UpdateInviteRetired(
	ctx context.Context,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID,
) ([]string, error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)

	rows, err := db.Raw(
		s.updateInviteRetiredStmt,
		roomNID,
		targetUserNID,
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "UpdateInviteRetired: rows.close() failed")

	// Collect all the invite event IDs that were retired
	var eventIDs []string
	for rows.Next() {
		var eventID string
		if err = rows.Scan(&eventID); err != nil {
			return nil, err
		}
		eventIDs = append(eventIDs, eventID)
	}

	return eventIDs, rows.Err()
}

// SelectInviteActiveForUserInRoom returns a list of sender state key NIDs and invite event IDs matching a given
// target user and room. Also returns the JSON for the invite events.
// Returns an empty list if no invites exist for this user and room.
func (s *inviteTable) SelectInviteActiveForUserInRoom(
	ctx context.Context,
	targetUserNID types.EventStateKeyNID, roomNID types.RoomNID,
) ([]types.EventStateKeyNID, []string, []byte, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(
		s.selectInviteActiveForUserInRoomStmt,
		targetUserNID,
		roomNID,
	).Rows()
	if err != nil {
		return nil, nil, nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectInviteActiveForUserInRoom: rows.close() failed")

	// Collect the results
	var senderUserNIDs []types.EventStateKeyNID
	var eventIDs []string
	var eventJSONs []byte
	
	// Track if this is our first result
	first := true
	
	for rows.Next() {
		var (
			inviteEventID   string
			senderUserNID   int64
			inviteEventJSON []byte
		)
		if err = rows.Scan(&inviteEventID, &senderUserNID, &inviteEventJSON); err != nil {
			return nil, nil, nil, err
		}
		
		senderUserNIDs = append(senderUserNIDs, types.EventStateKeyNID(senderUserNID))
		eventIDs = append(eventIDs, inviteEventID)
		
		// Preserve the first invite_event_json
		if first {
			eventJSONs = inviteEventJSON
			first = false
		}
	}
	
	return senderUserNIDs, eventIDs, eventJSONs, rows.Err()
}
