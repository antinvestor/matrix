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
	"github.com/antinvestor/matrix/roomserver/storage/tables"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/types"
)

// SQL: Delete event JSON for all events in a room
const purgeEventJSONSQL = "DELETE FROM roomserver_event_json WHERE event_nid = ANY(SELECT event_nid FROM roomserver_events WHERE room_nid = $1)"

// SQL: Delete all events in a room
const purgeEventsSQL = "DELETE FROM roomserver_events WHERE room_nid = $1"

// SQL: Delete invites for a room
const purgeInvitesSQL = "DELETE FROM roomserver_invites WHERE room_nid = $1"

// SQL: Delete memberships for a room
const purgeMembershipsSQL = "DELETE FROM roomserver_membership WHERE room_nid = $1"

// SQL: Delete previous events for a room (slow path)
const purgePreviousEventsSQL = "DELETE FROM roomserver_previous_events WHERE event_nids && ANY(SELECT ARRAY_AGG(event_nid) FROM roomserver_events WHERE room_nid = $1)"

// SQL: Delete previous events for a room (fast path)
const purgePreviousEvents2SQL = "DELETE FROM roomserver_previous_events rpe WHERE EXISTS(SELECT event_id FROM roomserver_events re WHERE room_nid = $1 AND re.event_id = rpe.previous_event_id)"

// SQL: Delete published rooms
const purgePublishedSQL = "DELETE FROM roomserver_published WHERE room_id = $1"

// SQL: Delete redactions for a room
const purgeRedactionsSQL = "DELETE FROM roomserver_redactions WHERE redaction_event_id = ANY(SELECT event_id FROM roomserver_events WHERE room_nid = $1)"

// SQL: Delete room aliases
const purgeRoomAliasesSQL = "DELETE FROM roomserver_room_aliases WHERE room_id = $1"

// SQL: Delete the room
const purgeRoomSQL = "DELETE FROM roomserver_rooms WHERE room_nid = $1"

// SQL: Delete state block entries for a room
const purgeStateBlockEntriesSQL = "DELETE FROM roomserver_state_block WHERE state_block_nid = ANY(SELECT DISTINCT UNNEST(state_block_nids) FROM roomserver_state_snapshots WHERE room_nid = $1)"

// SQL: Delete state snapshot entries for a room
const purgeStateSnapshotEntriesSQL = "DELETE FROM roomserver_state_snapshots WHERE room_nid = $1"

// purgeTable implements the tables.Purge interface for purging all data for a room.
type purgeTable struct {
	cm                           *sqlutil.Connections
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

// NewPostgresPurgeTable returns a new tables.Purge implementation using the provided connection manager.
func NewPostgresPurgeTable(cm *sqlutil.Connections) tables.Purge {
	return &purgeTable{
		cm:                           cm,
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
}

// PurgeRoom deletes all data for a given room, identified by roomNID and roomID.
// All SQL operations use the GORM connection from the connection manager.
// This method executes each purge step in order, using db.Exec or db.Raw as appropriate.
func (t *purgeTable) PurgeRoom(ctx context.Context, roomNID types.RoomNID, roomID string) error {
	// Acquire a database connection (write mode)
	db := t.cm.Connection(ctx, false)
	// Purge by roomID
	for _, sqlStr := range []string{t.purgeRoomAliasesSQL, t.purgePublishedSQL} {
		err := db.Exec(sqlStr, roomID).Error
		if err != nil {
			return err
		}
	}
	// Purge by roomNID
	for _, sqlStr := range []string{
		t.purgeStateBlockEntriesSQL,
		t.purgeStateSnapshotEntriesSQL,
		t.purgeInvitesSQL,
		t.purgeMembershipsSQL,
		t.purgePreviousEvents2SQL, // Fast purge the majority of events
		t.purgePreviousEventsSQL,  // Slow purge the remaining events
		t.purgeEventJSONSQL,
		t.purgeRedactionsSQL,
		t.purgeEventsSQL,
		t.purgeRoomSQL,
	} {
		err := db.Exec(sqlStr, roomNID).Error
		if err != nil {
			return err
		}
	}
	return nil
}
