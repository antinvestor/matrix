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
	"encoding/json"

	"github.com/antinvestor/matrix/internal/sqlutil"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
)

const inviteEventsSchema = `
CREATE TABLE IF NOT EXISTS syncapi_invite_events (
	id BIGINT PRIMARY KEY DEFAULT nextval('syncapi_stream_id'),
	event_id TEXT NOT NULL,
	room_id TEXT NOT NULL,
	target_user_id TEXT NOT NULL,
	headered_event_json TEXT NOT NULL,
	deleted BOOL NOT NULL
);

-- For looking up the invites for a given user.
CREATE INDEX IF NOT EXISTS syncapi_invites_target_user_id_idx
	ON syncapi_invite_events (target_user_id, id);

-- For deleting old invites
CREATE INDEX IF NOT EXISTS syncapi_invites_event_id_idx
	ON syncapi_invite_events (event_id);
`

const invitesSchemaRevert = `DROP TABLE IF EXISTS syncapi_invite_events;`

const insertInviteEventSQL = "" +
	"INSERT INTO syncapi_invite_events (" +
	" room_id, event_id, target_user_id, headered_event_json, deleted" +
	") VALUES ($1, $2, $3, $4, FALSE) RETURNING id"

const deleteInviteEventSQL = "" +
	"UPDATE syncapi_invite_events SET deleted=TRUE, id=nextval('syncapi_stream_id') WHERE event_id = $1 AND deleted=FALSE RETURNING id"

const selectInviteEventsInRangeSQL = "" +
	"SELECT id, room_id, headered_event_json, deleted FROM syncapi_invite_events" +
	" WHERE target_user_id = $1 AND id > $2 AND id <= $3" +
	" ORDER BY id DESC"

const selectMaxInviteIDSQL = "" +
	"SELECT MAX(id) FROM syncapi_invite_events"

const purgeInvitesSQL = "" +
	"DELETE FROM syncapi_invite_events WHERE room_id = $1"

// inviteEventsTable implements tables.Invites using a connection manager and SQL constants.
type inviteEventsTable struct {
	cm                           *sqlutil.Connections
	insertInviteEventSQL         string
	selectInviteEventsInRangeSQL string
	deleteInviteEventSQL         string
	selectMaxInviteIDSQL         string
	purgeInvitesSQL              string
}

// NewPostgresInvitesTable creates a new Invites table using a connection manager.
func NewPostgresInvitesTable(cm *sqlutil.Connections) tables.Invites {
	return &inviteEventsTable{
		cm:                           cm,
		insertInviteEventSQL:         insertInviteEventSQL,
		selectInviteEventsInRangeSQL: selectInviteEventsInRangeSQL,
		deleteInviteEventSQL:         deleteInviteEventSQL,
		selectMaxInviteIDSQL:         selectMaxInviteIDSQL,
		purgeInvitesSQL:              purgeInvitesSQL,
	}
}

// InsertInviteEvent stores a new invite event for a user.
// The invite event is marshaled as JSON and stored in the database.
// Returns the stream position of the inserted invite event.
func (t *inviteEventsTable) InsertInviteEvent(
	ctx context.Context,
	inviteEvent *rstypes.HeaderedEvent,
) (streamPos types.StreamPosition, err error) {
	// Marshal the invite event to JSON for storage.
	headeredJSON, err := json.Marshal(inviteEvent)
	if err != nil {
		return
	}
	// Insert the invite event using the defined SQL constant.
	db := t.cm.Connection(ctx, false)
	err = db.Raw(
		t.insertInviteEventSQL,
		inviteEvent.RoomID().String(),
		inviteEvent.EventID(),
		inviteEvent.StateKey(),
		string(headeredJSON),
	).Row().Scan(&streamPos)
	return
}

// DeleteInviteEvent removes an invite event by its ID.
// Marks the invite as deleted and updates its stream position.
func (t *inviteEventsTable) DeleteInviteEvent(
	ctx context.Context,
	inviteEventID string,
) (sp types.StreamPosition, err error) {
	// Update the invite event as deleted using the defined SQL constant.
	db := t.cm.Connection(ctx, false)
	err = db.Raw(t.deleteInviteEventSQL, inviteEventID).Row().Scan(&sp)
	return
}

// SelectInviteEventsInRange returns invites and retired (deleted) invites for a user in a given range.
// The returned maps are keyed by room ID. The maxID is the highest stream position in the result set.
func (t *inviteEventsTable) SelectInviteEventsInRange(
	ctx context.Context,
	targetUserID string,
	r types.Range,
) (invites map[string]*rstypes.HeaderedEvent, retired map[string]*rstypes.HeaderedEvent, maxID types.StreamPosition, err error) {
	invites = make(map[string]*rstypes.HeaderedEvent)
	retired = make(map[string]*rstypes.HeaderedEvent)
	// Query for invite events in the specified range.
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectInviteEventsInRangeSQL, targetUserID, r.Low(), r.High()).Rows()
	if err != nil {
		return
	}
	defer rows.Close()
	var (
		id                types.StreamPosition
		roomID            string
		headeredEventJSON string
		deleted           bool
	)
	for rows.Next() {
		if err = rows.Scan(&id, &roomID, &headeredEventJSON, &deleted); err != nil {
			return
		}
		// Unmarshal the stored JSON back into a HeaderedEvent.
		var event rstypes.HeaderedEvent
		if err = json.Unmarshal([]byte(headeredEventJSON), &event); err != nil {
			return
		}
		// Place in the correct map depending on deleted status.
		if deleted {
			retired[roomID] = &event
		} else {
			invites[roomID] = &event
		}
		// Track the highest stream position seen.
		if id > maxID {
			maxID = id
		}
	}
	return invites, retired, maxID, rows.Err()
}

// SelectMaxInviteID returns the maximum stream position for invites.
func (t *inviteEventsTable) SelectMaxInviteID(
	ctx context.Context,
) (id int64, err error) {
	// Query for the maximum invite ID using the defined SQL constant.
	db := t.cm.Connection(ctx, true)
	var nullableID sql.NullInt64
	err = db.Raw(t.selectMaxInviteIDSQL).Row().Scan(&nullableID)
	if nullableID.Valid {
		id = nullableID.Int64
	}
	return
}

// PurgeInvites removes all invites for a given room.
// This is typically used during room cleanup.
func (t *inviteEventsTable) PurgeInvites(
	ctx context.Context,
	roomID string,
) error {
	// Execute the purge using the defined SQL constant.
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.purgeInvitesSQL, roomID).Error
}
