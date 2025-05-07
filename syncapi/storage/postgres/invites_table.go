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
	"encoding/json"

	"github.com/antinvestor/matrix/internal"
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

type invitesTable struct {
	cm                           *sqlutil.Connections
	insertInviteEventSQL         string
	selectInviteEventsInRangeSQL string
	deleteInviteEventSQL         string
	selectMaxInviteIDSQL         string
	purgeInvitesSQL              string
}

func NewPostgresInvitesTable(ctx context.Context, cm *sqlutil.Connections) (tables.Invites, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(inviteEventsSchema).Error; err != nil {
		return nil, err
	}
	
	// Initialize the table with SQL statements
	s := &invitesTable{
		cm:                           cm,
		insertInviteEventSQL:         insertInviteEventSQL,
		selectInviteEventsInRangeSQL: selectInviteEventsInRangeSQL,
		deleteInviteEventSQL:         deleteInviteEventSQL,
		selectMaxInviteIDSQL:         selectMaxInviteIDSQL,
		purgeInvitesSQL:              purgeInvitesSQL,
	}
	
	return s, nil
}

func (s *invitesTable) InsertInviteEvent(
	ctx context.Context, inviteEvent *rstypes.HeaderedEvent,
) (streamPos types.StreamPosition, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	var headeredJSON []byte
	headeredJSON, err = json.Marshal(inviteEvent)
	if err != nil {
		return
	}

	row := db.Raw(s.insertInviteEventSQL,
		inviteEvent.RoomID().String(),
		inviteEvent.EventID(),
		inviteEvent.UserID.String(),
		headeredJSON,
	).Row()
	
	err = row.Scan(&streamPos)
	return
}

func (s *invitesTable) DeleteInviteEvent(
	ctx context.Context, inviteEventID string,
) (sp types.StreamPosition, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	row := db.Raw(s.deleteInviteEventSQL, inviteEventID).Row()
	err = row.Scan(&sp)
	return
}

// SelectInviteEventsInRange returns a map of room ID to invite event for the
// active invites for the target user ID in the supplied range.
func (s *invitesTable) SelectInviteEventsInRange(
	ctx context.Context, targetUserID string, r types.Range,
) (map[string]*rstypes.HeaderedEvent, map[string]*rstypes.HeaderedEvent, types.StreamPosition, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	var lastPos types.StreamPosition
	rows, err := db.Raw(s.selectInviteEventsInRangeSQL, targetUserID, r.Low(), r.High()).Rows()
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

func (s *invitesTable) SelectMaxInviteID(
	ctx context.Context,
) (id int64, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	err = db.Raw(s.selectMaxInviteIDSQL).Scan(&id).Error
	return
}

func (s *invitesTable) PurgeInvites(
	ctx context.Context, roomID string,
) error {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	return db.Exec(s.purgeInvitesSQL, roomID).Error
}
