// Copyright 2022 The Global.org Foundation C.I.C.
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

// SQL queries for purging rooms
const (
	purgeEventJSONSQL = `
		DELETE FROM roomserver_event_json WHERE event_nid = ANY(
			SELECT event_nid FROM roomserver_events WHERE room_nid = $1
		)
	`

	purgeEventsSQL = `
		DELETE FROM roomserver_events WHERE room_nid = $1
	`

	purgeInvitesSQL = `
		DELETE FROM roomserver_invites WHERE room_nid = $1
	`

	purgeMembershipsSQL = `
		DELETE FROM roomserver_membership WHERE room_nid = $1
	`

	purgePreviousEventsSQL = `
		DELETE FROM roomserver_previous_events WHERE event_nids && ANY(
			SELECT ARRAY_AGG(event_nid) FROM roomserver_events WHERE room_nid = $1
		)
	`

	// This removes the majority of prev events and is way faster than the above.
	// The above query is still needed to delete the remaining prev events.
	purgePreviousEvents2SQL = `
		DELETE FROM roomserver_previous_events rpe WHERE EXISTS(
			SELECT event_id FROM roomserver_events re WHERE room_nid = $1 AND re.event_id = rpe.previous_event_id
		)
	`

	purgePublishedSQL = `
		DELETE FROM roomserver_published WHERE room_id = $1
	`

	purgeRedactionsSQL = `
		DELETE FROM roomserver_redactions WHERE redaction_event_id = ANY(
			SELECT event_id FROM roomserver_events WHERE room_nid = $1
		)
	`

	purgeRoomAliasesSQL = `
		DELETE FROM roomserver_room_aliases WHERE room_id = $1
	`

	purgeRoomSQL = `
		DELETE FROM roomserver_rooms WHERE room_nid = $1
	`

	purgeStateBlockEntriesSQL = `
		DELETE FROM roomserver_state_block WHERE state_block_nid = ANY(
			SELECT DISTINCT UNNEST(state_block_nids) FROM roomserver_state_snapshots WHERE room_nid = $1
		)
	`

	purgeStateSnapshotEntriesSQL = `
		DELETE FROM roomserver_state_snapshots WHERE room_nid = $1
	`
)

// purgeStatements holds the SQL queries for purging rooms
type purgeStatements struct {
	cm sqlutil.ConnectionManager

	// SQL query string fields
	purgeEventJSONSQL            string
	purgeEventsSQL               string
	purgeInvitesSQL              string
	purgeMembershipsSQL          string
	purgePreviousEventsSQL       string
	purgePreviousEvents2SQL      string
	purgePublishedSQL            string
	purgeRedactionsSQL           string
	purgeRoomAliasesSQL          string
	purgeRoomSQL                 string
	purgeStateBlockEntriesSQL    string
	purgeStateSnapshotEntriesSQL string
}

// NewPostgresPurgeTable creates a new instance of the purge statements.
func NewPostgresPurgeTable(_ context.Context, cm sqlutil.ConnectionManager) (tables.Purge, error) {
	s := &purgeStatements{
		cm: cm,

		purgeEventJSONSQL:            purgeEventJSONSQL,
		purgeEventsSQL:               purgeEventsSQL,
		purgeInvitesSQL:              purgeInvitesSQL,
		purgeMembershipsSQL:          purgeMembershipsSQL,
		purgePreviousEventsSQL:       purgePreviousEventsSQL,
		purgePreviousEvents2SQL:      purgePreviousEvents2SQL,
		purgePublishedSQL:            purgePublishedSQL,
		purgeRedactionsSQL:           purgeRedactionsSQL,
		purgeRoomAliasesSQL:          purgeRoomAliasesSQL,
		purgeRoomSQL:                 purgeRoomSQL,
		purgeStateBlockEntriesSQL:    purgeStateBlockEntriesSQL,
		purgeStateSnapshotEntriesSQL: purgeStateSnapshotEntriesSQL,
	}

	return s, nil
}

// PurgeRoom purges a room and all associated data from the database.
func (s *purgeStatements) PurgeRoom(
	ctx context.Context, roomNID types.RoomNID, roomID string,
) error {
	// Get a connection for writing
	db := s.cm.Connection(ctx, false)

	// purge by roomID
	purgeByRoomIDQueries := []string{
		s.purgeRoomAliasesSQL,
		s.purgePublishedSQL,
	}

	for _, query := range purgeByRoomIDQueries {
		if err := db.Exec(query, roomID).Error; err != nil {
			return err
		}
	}

	// purge by roomNID
	purgeByRoomNIDQueries := []string{
		s.purgeStateBlockEntriesSQL,
		s.purgeStateSnapshotEntriesSQL,
		s.purgeInvitesSQL,
		s.purgeMembershipsSQL,
		s.purgePreviousEvents2SQL, // Fast purge the majority of events
		s.purgePreviousEventsSQL,  // Slow purge the remaining events
		s.purgeEventJSONSQL,
		s.purgeRedactionsSQL,
		s.purgeEventsSQL,
		s.purgeRoomSQL,
	}

	for _, query := range purgeByRoomNIDQueries {
		if err := db.Exec(query, roomNID).Error; err != nil {
			return err
		}
	}

	return nil
}
