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

const inviteSchemaRevert = `DROP TABLE IF EXISTS roomserver_invites;`

const insertInviteEventSQL = "" +
	"INSERT INTO roomserver_invites (invite_event_id, room_nid, target_nid," +
	" sender_nid, invite_event_json) VALUES ($1, $2, $3, $4, $5)" +
	" ON CONFLICT DO NOTHING"

const selectInviteActiveForUserInRoomSQL = "" +
	"SELECT invite_event_id, sender_nid, invite_event_json FROM roomserver_invites" +
	" WHERE target_nid = $1 AND room_nid = $2" +
	" AND NOT retired"

// Retire every active invite for a user in a room.
// Ideally we'd know which invite events were retired by a given update so we
// wouldn't need to remove every active invite.
// However the matrix protocol doesn't give us a way to reliably identify the
// invites that were retired, so we are forced to retire all of them.
const updateInviteRetiredSQL = "" +
	"UPDATE roomserver_invites SET retired = TRUE" +
	" WHERE room_nid = $1 AND target_nid = $2 AND NOT retired" +
	" RETURNING invite_event_id"

type inviteTable struct {
	cm                     *sqlutil.Connections
	insertInviteEventSQL   string
	selectInviteActiveSQL  string
	updateInviteRetiredSQL string
}

func NewPostgresInvitesTable(cm *sqlutil.Connections) tables.Invites {
	return &inviteTable{
		cm:                     cm,
		insertInviteEventSQL:   insertInviteEventSQL,
		selectInviteActiveSQL:  selectInviteActiveForUserInRoomSQL,
		updateInviteRetiredSQL: updateInviteRetiredSQL,
	}
}

func (t *inviteTable) InsertInviteEvent(ctx context.Context, inviteEventID string, roomNID types.RoomNID, targetUserNID, senderUserNID types.EventStateKeyNID, inviteEventJSON []byte) (bool, error) {
	db := t.cm.Connection(ctx, false)
	res := db.Exec(t.insertInviteEventSQL, inviteEventID, roomNID, targetUserNID, senderUserNID, string(inviteEventJSON))
	if res.Error != nil {
		return false, res.Error
	}
	rowsAffected := res.RowsAffected
	return rowsAffected > 0, nil
}

func (t *inviteTable) UpdateInviteRetired(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID) ([]string, error) {
	db := t.cm.Connection(ctx, false)
	rows, err := db.Raw(t.updateInviteRetiredSQL, roomNID, targetUserNID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var inviteEventIDs []string
	for rows.Next() {
		var inviteEventID string
		if err := rows.Scan(&inviteEventID); err != nil {
			return nil, err
		}
		inviteEventIDs = append(inviteEventIDs, inviteEventID)
	}
	return inviteEventIDs, nil
}

func (t *inviteTable) SelectInviteActiveForUserInRoom(ctx context.Context, targetUserNID types.EventStateKeyNID, roomNID types.RoomNID) ([]types.EventStateKeyNID, []string, []byte, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectInviteActiveSQL, targetUserNID, roomNID).Rows()
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()
	var senderNIDs []types.EventStateKeyNID
	var inviteEventIDs []string
	var inviteEventJSON []byte
	for rows.Next() {
		var inviteEventID string
		var senderNID types.EventStateKeyNID
		var eventJSON []byte
		if err := rows.Scan(&inviteEventID, &senderNID, &eventJSON); err != nil {
			return nil, nil, nil, err
		}
		senderNIDs = append(senderNIDs, senderNID)
		inviteEventIDs = append(inviteEventIDs, inviteEventID)
		inviteEventJSON = eventJSON
	}
	return senderNIDs, inviteEventIDs, inviteEventJSON, rows.Err()
}
