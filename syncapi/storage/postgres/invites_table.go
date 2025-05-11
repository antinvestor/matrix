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
	"encoding/json"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/pitabwire/frame"
)

// Schema for creating invite events table
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

// Revert schema for invite events table
const inviteEventsSchemaRevert = `
DROP INDEX IF EXISTS syncapi_invites_event_id_idx;
DROP INDEX IF EXISTS syncapi_invites_target_user_id_idx;
DROP TABLE IF EXISTS syncapi_invite_events;
`

// SQL query to insert invite event
const insertInviteEventSQL = `
INSERT INTO syncapi_invite_events (
 room_id, event_id, target_user_id, headered_event_json, deleted
) VALUES ($1, $2, $3, $4, FALSE) RETURNING id
`

// SQL query to delete invite event
const deleteInviteEventSQL = `
UPDATE syncapi_invite_events SET deleted=TRUE, id=nextval('syncapi_stream_id') WHERE event_id = $1 AND deleted=FALSE RETURNING id
`

// SQL query to select invite events in range
const selectInviteEventsInRangeSQL = `
SELECT id, room_id, headered_event_json, deleted FROM syncapi_invite_events
WHERE target_user_id = $1 AND id > $2 AND id <= $3
ORDER BY id DESC
`

// SQL query to select maximum invite ID
const selectMaxInviteIDSQL = `
SELECT MAX(id) FROM syncapi_invite_events
`

// SQL query to purge invites
const purgeInvitesSQL = `
DELETE FROM syncapi_invite_events WHERE room_id = $1
`

// inviteEventsTable implements tables.Invites
type inviteEventsTable struct {
	cm                           sqlutil.ConnectionManager
	insertInviteEventSQL         string
	selectInviteEventsInRangeSQL string
	deleteInviteEventSQL         string
	selectMaxInviteIDSQL         string
	purgeInvitesSQL              string
}

// NewPostgresInvitesTable creates a new invites table
func NewPostgresInvitesTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.Invites, error) {
	t := &inviteEventsTable{
		cm:                           cm,
		insertInviteEventSQL:         insertInviteEventSQL,
		selectInviteEventsInRangeSQL: selectInviteEventsInRangeSQL,
		deleteInviteEventSQL:         deleteInviteEventSQL,
		selectMaxInviteIDSQL:         selectMaxInviteIDSQL,
		purgeInvitesSQL:              purgeInvitesSQL,
	}

	// Perform the migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "syncapi_invite_events_table_schema_001",
		Patch:       inviteEventsSchema,
		RevertPatch: inviteEventsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// InsertInviteEvent inserts a new invite event
func (t *inviteEventsTable) InsertInviteEvent(
	ctx context.Context, inviteEvent *rstypes.HeaderedEvent,
) (streamPos types.StreamPosition, err error) {
	var headeredJSON []byte
	headeredJSON, err = json.Marshal(inviteEvent)
	if err != nil {
		return
	}

	db := t.cm.Connection(ctx, false)
	row := db.Raw(
		t.insertInviteEventSQL,
		inviteEvent.RoomID().String(),
		inviteEvent.EventID(),
		inviteEvent.UserID.String(),
		headeredJSON,
	).Row()
	err = row.Scan(&streamPos)
	return
}

// DeleteInviteEvent marks an invite event as deleted
func (t *inviteEventsTable) DeleteInviteEvent(
	ctx context.Context, inviteEventID string,
) (sp types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	row := db.Raw(t.deleteInviteEventSQL, inviteEventID).Row()
	err = row.Scan(&sp)
	return
}

// SelectInviteEventsInRange returns a map of room ID to invite event for the
// active invites for the target user ID in the supplied range.
func (t *inviteEventsTable) SelectInviteEventsInRange(
	ctx context.Context, targetUserID string, r types.Range,
) (map[string]*rstypes.HeaderedEvent, map[string]*rstypes.HeaderedEvent, types.StreamPosition, error) {
	var lastPos types.StreamPosition
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectInviteEventsInRangeSQL, targetUserID, r.Low(), r.High()).Rows()
	if err != nil {
		return nil, nil, lastPos, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectInviteEventsInRange: rows.close() failed")
	result := map[string]*rstypes.HeaderedEvent{}
	retired := map[string]*rstypes.HeaderedEvent{}
	for rows.Next() {
		var (
			id        types.StreamPosition
			roomID    string
			eventJSON []byte
			deleted   bool
		)
		if err = rows.Scan(&id, &roomID, &eventJSON, &deleted); err != nil {
			return nil, nil, lastPos, err
		}
		if id > lastPos {
			lastPos = id
		}

		// if we have seen this room before, it has a higher stream position and hence takes priority
		// because the query is ORDER BY id DESC so drop them
		_, isRetired := retired[roomID]
		_, isInvited := result[roomID]
		if isRetired || isInvited {
			continue
		}

		var event *rstypes.HeaderedEvent
		if err := json.Unmarshal(eventJSON, &event); err != nil {
			return nil, nil, lastPos, err
		}

		if deleted {
			retired[roomID] = event
		} else {
			result[roomID] = event
		}
	}
	if lastPos == 0 {
		lastPos = r.To
	}
	return result, retired, lastPos, rows.Err()
}

// SelectMaxInviteID returns the maximum invite ID
func (t *inviteEventsTable) SelectMaxInviteID(
	ctx context.Context,
) (id int64, err error) {
	var nullableID sql.NullInt64
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectMaxInviteIDSQL).Row()
	err = row.Scan(&nullableID)
	if nullableID.Valid {
		id = nullableID.Int64
	}
	return
}

// PurgeInvites purges all invites for a room
func (t *inviteEventsTable) PurgeInvites(
	ctx context.Context, roomID string,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.purgeInvitesSQL, roomID).Error
}
