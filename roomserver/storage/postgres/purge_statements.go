// Copyright 2022 The Matrix.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/roomserver/types"
)

// SQL query constants for purge operations
const (
	// purgeEventJSONSQL deletes event JSON entries for a room
	purgeEventJSONSQL = "" +
		"DELETE FROM roomserver_event_json WHERE event_nid = ANY(" +
		"	SELECT event_nid FROM roomserver_events WHERE room_nid = $1" +
		")"

	// purgeEventsSQL deletes all events for a room
	purgeEventsSQL = "" +
		"DELETE FROM roomserver_events WHERE room_nid = $1"

	// purgeInvitesSQL deletes all invites for a room
	purgeInvitesSQL = "" +
		"DELETE FROM roomserver_invites WHERE room_nid = $1"

	// purgeMembershipsSQL deletes all memberships for a room
	purgeMembershipsSQL = "" +
		"DELETE FROM roomserver_membership WHERE room_nid = $1"

	// purgePreviousEventsSQL deletes previous events references for a room
	purgePreviousEventsSQL = "" +
		"DELETE FROM roomserver_previous_events WHERE event_nids && ANY(" +
		"	SELECT ARRAY_AGG(event_nid) FROM roomserver_events WHERE room_nid = $1" +
		")"

	// purgePreviousEvents2SQL removes the majority of prev events and is way faster than the above.
	// The above query is still needed to delete the remaining prev events.
	purgePreviousEvents2SQL = "" +
		"DELETE FROM roomserver_previous_events rpe WHERE EXISTS(SELECT event_id FROM roomserver_events re WHERE room_nid = $1 AND re.event_id = rpe.previous_event_id)"

	// purgePublishedSQL deletes published room entries
	purgePublishedSQL = "" +
		"DELETE FROM roomserver_published WHERE room_id = $1"

	// purgeRedactionsSQL deletes redaction events for a room
	purgeRedactionsSQL = "" +
		"DELETE FROM roomserver_redactions WHERE redaction_event_id = ANY(" +
		"	SELECT event_id FROM roomserver_events WHERE room_nid = $1" +
		")"

	// purgeRoomAliasesSQL deletes room aliases
	purgeRoomAliasesSQL = "" +
		"DELETE FROM roomserver_room_aliases WHERE room_id = $1"

	// purgeRoomSQL deletes a room entry
	purgeRoomSQL = "" +
		"DELETE FROM roomserver_rooms WHERE room_nid = $1"

	// purgeStateBlockEntriesSQL deletes state block entries for a room
	purgeStateBlockEntriesSQL = "" +
		"DELETE FROM roomserver_state_block WHERE state_block_nid = ANY(" +
		"	SELECT DISTINCT UNNEST(state_block_nids) FROM roomserver_state_snapshots WHERE room_nid = $1" +
		")"

	// purgeStateSnapshotEntriesSQL deletes state snapshot entries for a room
	purgeStateSnapshotEntriesSQL = "" +
		"DELETE FROM roomserver_state_snapshots WHERE room_nid = $1"
)

type purgeStatements struct {
	cm *sqlutil.Connections
	
	// SQL statements stored as struct fields
	purgeEventJSONStmt             string
	purgeEventsStmt                string
	purgeInvitesStmt               string
	purgeMembershipsStmt           string
	purgePreviousEventsStmt        string
	purgePreviousEvents2Stmt       string
	purgePublishedStmt             string
	purgeRedactionsStmt            string
	purgeRoomAliasesStmt           string
	purgeRoomStmt                  string
	purgeStateBlockEntriesStmt     string
	purgeStateSnapshotEntriesStmt  string
}

// PreparePurgeStatements creates and initializes the purge statements
func PreparePurgeStatements(cm *sqlutil.Connections) (*purgeStatements, error) {
	// Initialize statements with SQL constants
	s := &purgeStatements{
		cm: cm,
		
		// Initialize SQL statement fields with the constants
		purgeEventJSONStmt:             purgeEventJSONSQL,
		purgeEventsStmt:                purgeEventsSQL, 
		purgeInvitesStmt:               purgeInvitesSQL,
		purgeMembershipsStmt:           purgeMembershipsSQL,
		purgePreviousEventsStmt:        purgePreviousEventsSQL,
		purgePreviousEvents2Stmt:       purgePreviousEvents2SQL,
		purgePublishedStmt:             purgePublishedSQL,
		purgeRedactionsStmt:            purgeRedactionsSQL,
		purgeRoomAliasesStmt:           purgeRoomAliasesSQL,
		purgeRoomStmt:                  purgeRoomSQL,
		purgeStateBlockEntriesStmt:     purgeStateBlockEntriesSQL,
		purgeStateSnapshotEntriesStmt:  purgeStateSnapshotEntriesSQL,
	}

	return s, nil
}

// PurgeRoom deletes all data related to a room from the database
func (s *purgeStatements) PurgeRoom(
	ctx context.Context, roomNID types.RoomNID, roomID string,
) error {
	db := s.cm.Connection(ctx, false)

	// purge by roomID
	purgeByRoomID := map[string]string{
		"purge room aliases": s.purgeRoomAliasesStmt,
		"purge published room": s.purgePublishedStmt,
	}
	for name, query := range purgeByRoomID {
		if err := db.Exec(query, roomID).Error; err != nil {
			return err
		}
	}

	// purge by roomNID
	purgeByRoomNID := map[string]string{
		"purge state block entries": s.purgeStateBlockEntriesStmt,
		"purge state snapshot entries": s.purgeStateSnapshotEntriesStmt,
		"purge invites": s.purgeInvitesStmt,
		"purge memberships": s.purgeMembershipsStmt,
		"purge previous events (fast)": s.purgePreviousEvents2Stmt, // Fast purge the majority of events
		"purge previous events (thorough)": s.purgePreviousEventsStmt, // Slow purge the remaining events
		"purge event JSON": s.purgeEventJSONStmt,
		"purge redactions": s.purgeRedactionsStmt, 
		"purge events": s.purgeEventsStmt,
		"purge room": s.purgeRoomStmt,
	}
	for name, query := range purgeByRoomNID {
		if err := db.Exec(query, roomNID).Error; err != nil {
			return err
		}
	}
	
	return nil
}
