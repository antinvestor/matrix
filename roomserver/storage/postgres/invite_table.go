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
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/pitabwire/frame"
)

// Schema for the invites table
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

// Schema revert script for migration purposes
const inviteSchemaRevert = `DROP TABLE IF EXISTS roomserver_invites;`

// SQL to insert a new invite event
const insertInviteEventSQL = "" +
	"INSERT INTO roomserver_invites (invite_event_id, room_nid, target_nid," +
	" sender_nid, invite_event_json) VALUES ($1, $2, $3, $4, $5)" +
	" ON CONFLICT DO NOTHING"

// SQL to select active invite events for a user in a room
const selectInviteActiveForUserInRoomSQL = "" +
	"SELECT invite_event_id, sender_nid, invite_event_json FROM roomserver_invites" +
	" WHERE target_nid = $1 AND room_nid = $2" +
	" AND NOT retired"

// SQL to retire invite events for a user in a room
// Retire every active invite for a user in a room.
// Ideally we'd know which invite events were retired by a given update so we
// wouldn't need to remove every active invite.
// However the matrix protocol doesn't give us a way to reliably identify the
// invites that were retired, so we are forced to retire all of them.
const updateInviteRetiredSQL = "" +
	"UPDATE roomserver_invites SET retired = TRUE" +
	" WHERE room_nid = $1 AND target_nid = $2 AND NOT retired" +
	" RETURNING invite_event_id"

// inviteTable implements the tables.Invites interface using GORM
type inviteTable struct {
	cm *sqlutil.Connections

	// SQL query strings loaded from constants
	insertInviteEventSQL               string
	selectInviteActiveForUserInRoomSQL string
	updateInviteRetiredSQL             string
}

// NewPostgresInvitesTable creates a new invites table
func NewPostgresInvitesTable(ctx context.Context, cm *sqlutil.Connections) (tables.Invites, error) {
	// Create the table if it doesn't exist using migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "roomserver_invites_table_schema_001",
		Patch:       inviteSchema,
		RevertPatch: inviteSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Initialize the table struct with just the connection manager
	t := &inviteTable{
		cm: cm,

		// Initialize SQL query strings from constants
		insertInviteEventSQL:               insertInviteEventSQL,
		selectInviteActiveForUserInRoomSQL: selectInviteActiveForUserInRoomSQL,
		updateInviteRetiredSQL:             updateInviteRetiredSQL,
	}

	return t, nil
}

func (t *inviteTable) InsertInviteEvent(
	ctx context.Context,
	inviteEventID string, roomNID types.RoomNID,
	targetUserNID, senderUserNID types.EventStateKeyNID,
	inviteEventJSON []byte,
) (bool, error) {
	db := t.cm.Connection(ctx, false)

	result := db.Exec(t.insertInviteEventSQL, inviteEventID, roomNID, targetUserNID, senderUserNID, inviteEventJSON)
	if result.Error != nil {
		return false, result.Error
	}

	return result.RowsAffected > 0, nil
}

func (t *inviteTable) UpdateInviteRetired(
	ctx context.Context,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID,
) ([]string, error) {
	db := t.cm.Connection(ctx, false)

	rows, err := db.Raw(t.updateInviteRetiredSQL, roomNID, targetUserNID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "updateInviteRetired: rows.close() failed")

	var eventIDs []string
	var inviteEventID string
	for rows.Next() {
		if err = rows.Scan(&inviteEventID); err != nil {
			return nil, err
		}
		eventIDs = append(eventIDs, inviteEventID)
	}
	return eventIDs, rows.Err()
}

// SelectInviteActiveForUserInRoom returns a list of sender state key NIDs
func (t *inviteTable) SelectInviteActiveForUserInRoom(
	ctx context.Context,
	targetUserNID types.EventStateKeyNID, roomNID types.RoomNID,
) ([]types.EventStateKeyNID, []string, []byte, error) {
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.selectInviteActiveForUserInRoomSQL, targetUserNID, roomNID).Rows()
	if err != nil {
		return nil, nil, nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectInviteActiveForUserInRoom: rows.close() failed")

	var result []types.EventStateKeyNID
	var eventIDs []string
	var inviteEventID string
	var senderUserNID int64
	var eventJSON []byte
	for rows.Next() {
		if err = rows.Scan(&inviteEventID, &senderUserNID, &eventJSON); err != nil {
			return nil, nil, nil, err
		}
		result = append(result, types.EventStateKeyNID(senderUserNID))
		eventIDs = append(eventIDs, inviteEventID)
	}
	return result, eventIDs, eventJSON, rows.Err()
}
