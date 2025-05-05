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
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/lib/pq"
)

const currentRoomStateSchema = `
-- Stores the current room state for every room.
CREATE TABLE IF NOT EXISTS syncapi_current_room_state (
    -- The 'room_id' key for the state event.
    room_id TEXT NOT NULL,
    -- The state event ID
    event_id TEXT NOT NULL,
    -- The state event type e.g 'm.room.member'
    type TEXT NOT NULL,
    -- The 'sender' property of the event.
    sender TEXT NOT NULL,
	-- true if the event content contains a url key
    contains_url BOOL NOT NULL,
    -- The state_key value for this state event e.g ''
    state_key TEXT NOT NULL,
    -- The JSON for the event. Stored as TEXT because this should be valid UTF-8.
    headered_event_json TEXT NOT NULL,
    -- The 'content.membership' value if this event is an m.room.member event. For other
    -- events, this will be NULL.
    membership TEXT,
    -- The serial ID of the output_room_events table when this event became
    -- part of the current state of the room.
    added_at BIGINT,
    history_visibility SMALLINT NOT NULL DEFAULT 2,
    -- Clobber based on 3-uple of room_id, type and state_key
    CONSTRAINT syncapi_room_state_unique UNIQUE (room_id, type, state_key)
);
-- for event deletion
CREATE UNIQUE INDEX IF NOT EXISTS syncapi_event_id_idx ON syncapi_current_room_state(event_id, room_id, type, sender, contains_url);
-- for querying membership states of users
CREATE INDEX IF NOT EXISTS syncapi_membership_idx ON syncapi_current_room_state(type, state_key, membership) WHERE membership IS NOT NULL AND membership != 'leave';
-- for querying state by event IDs
CREATE UNIQUE INDEX IF NOT EXISTS syncapi_current_room_state_eventid_idx ON syncapi_current_room_state(event_id);
-- for improving selectRoomIDsWithAnyMembershipSQL
CREATE INDEX IF NOT EXISTS syncapi_current_room_state_type_state_key_idx ON syncapi_current_room_state(type, state_key);
`

const currentRoomStateSchemaRevert = `DROP TABLE IF EXISTS syncapi_current_room_state;`

const upsertRoomStateSQL = "" +
	"INSERT INTO syncapi_current_room_state (room_id, event_id, type, sender, contains_url, state_key, headered_event_json, membership, added_at, history_visibility)" +
	" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)" +
	" ON CONFLICT ON CONSTRAINT syncapi_room_state_unique" +
	" DO UPDATE SET event_id = $2, sender=$4, contains_url=$5, headered_event_json = $7, membership = $8, added_at = $9"

const deleteRoomStateByEventIDSQL = "" +
	"DELETE FROM syncapi_current_room_state WHERE event_id = $1"

const deleteRoomStateForRoomSQL = "" +
	"DELETE FROM syncapi_current_room_state WHERE room_id = $1"

const selectRoomIDsWithMembershipSQL = "" +
	"SELECT DISTINCT room_id FROM syncapi_current_room_state WHERE type = 'm.room.member' AND state_key = $1 AND membership = $2"

const selectRoomIDsWithAnyMembershipSQL = "" +
	"SELECT room_id, membership FROM syncapi_current_room_state WHERE type = 'm.room.member' AND state_key = $1"

const selectCurrentStateSQL = "" +
	"SELECT event_id, headered_event_json FROM syncapi_current_room_state WHERE room_id = $1" +
	" AND ( $2::text[] IS NULL OR     sender  = ANY($2)  )" +
	" AND ( $3::text[] IS NULL OR NOT(sender  = ANY($3)) )" +
	" AND ( $4::text[] IS NULL OR     type LIKE ANY($4)  )" +
	" AND ( $5::text[] IS NULL OR NOT(type LIKE ANY($5)) )" +
	" AND ( $6::bool IS NULL   OR     contains_url = $6  )" +
	" AND (event_id = ANY($7)) IS NOT TRUE"

const selectJoinedUsersSQL = "" +
	"SELECT room_id, state_key FROM syncapi_current_room_state WHERE type = 'm.room.member' AND membership = 'join'"

const selectJoinedUsersInRoomSQL = "" +
	"SELECT room_id, state_key FROM syncapi_current_room_state WHERE type = 'm.room.member' AND membership = 'join' AND room_id = ANY($1)"

const selectStateEventSQL = "" +
	"SELECT headered_event_json FROM syncapi_current_room_state WHERE room_id = $1 AND type = $2 AND state_key = $3"

const selectEventsWithEventIDsSQL = "" +
	"SELECT event_id, added_at, headered_event_json, history_visibility FROM syncapi_current_room_state WHERE event_id = ANY($1)"

const selectSharedUsersSQL = "" +
	"SELECT state_key FROM syncapi_current_room_state WHERE room_id = ANY(" +
	"	SELECT DISTINCT room_id FROM syncapi_current_room_state WHERE state_key = $1 AND membership='join'" +
	") AND type = 'm.room.member' AND state_key = ANY($2) AND membership IN ('join', 'invite');"

const selectMembershipCount = `SELECT count(*) FROM syncapi_current_room_state WHERE type = 'm.room.member' AND room_id = $1 AND membership = $2`

const selectRoomHeroes = `
SELECT state_key FROM syncapi_current_room_state
WHERE type = 'm.room.member' AND room_id = $1 AND membership = ANY($2) AND state_key != $3
ORDER BY added_at, state_key
LIMIT 5
`

// currentRoomStateTable implements tables.CurrentRoomState using a connection manager and SQL constants.
type currentRoomStateTable struct {
	cm                                *sqlutil.Connections
	upsertRoomStateSQL                string
	deleteRoomStateByEventIDSQL       string
	deleteRoomStateForRoomSQL         string
	selectRoomIDsWithMembershipSQL    string
	selectRoomIDsWithAnyMembershipSQL string
	selectCurrentStateSQL             string
	selectJoinedUsersSQL              string
	selectJoinedUsersInRoomSQL        string
	selectEventsWithEventIDsSQL       string
	selectStateEventSQL               string
	selectSharedUsersSQL              string
	selectMembershipCountSQL          string
	selectRoomHeroesSQL               string
}

// NewPostgresCurrentRoomStateTable creates a new CurrentRoomState table using a connection manager.
func NewPostgresCurrentRoomStateTable(cm *sqlutil.Connections) tables.CurrentRoomState {
	return &currentRoomStateTable{
		cm:                                cm,
		upsertRoomStateSQL:                upsertRoomStateSQL,
		deleteRoomStateByEventIDSQL:       deleteRoomStateByEventIDSQL,
		deleteRoomStateForRoomSQL:         deleteRoomStateForRoomSQL,
		selectRoomIDsWithMembershipSQL:    selectRoomIDsWithMembershipSQL,
		selectRoomIDsWithAnyMembershipSQL: selectRoomIDsWithAnyMembershipSQL,
		selectCurrentStateSQL:             selectCurrentStateSQL,
		selectJoinedUsersSQL:              selectJoinedUsersSQL,
		selectJoinedUsersInRoomSQL:        selectJoinedUsersInRoomSQL,
		selectEventsWithEventIDsSQL:       selectEventsWithEventIDsSQL,
		selectStateEventSQL:               selectStateEventSQL,
		selectSharedUsersSQL:              selectSharedUsersSQL,
		selectMembershipCountSQL:          selectMembershipCount,
		selectRoomHeroesSQL:               selectRoomHeroes,
	}
}

// SelectJoinedUsers returns a map of room ID to a list of joined user IDs.
func (t *currentRoomStateTable) SelectJoinedUsers(
	ctx context.Context,
) (map[string][]string, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectJoinedUsersSQL).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string][]string)
	var roomID, userID string
	for rows.Next() {
		if err = rows.Scan(&roomID, &userID); err != nil {
			return nil, err
		}
		result[roomID] = append(result[roomID], userID)
	}
	return result, rows.Err()
}

// SelectJoinedUsersInRoom returns a map of room ID to a list of joined user IDs for the given rooms.
func (t *currentRoomStateTable) SelectJoinedUsersInRoom(
	ctx context.Context,
	roomIDs []string,
) (map[string][]string, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectJoinedUsersInRoomSQL, pq.StringArray(roomIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string][]string)
	var userID, roomID string
	for rows.Next() {
		if err = rows.Scan(&roomID, &userID); err != nil {
			return nil, err
		}
		result[roomID] = append(result[roomID], userID)
	}
	return result, rows.Err()
}

// SelectRoomIDsWithMembership returns the list of room IDs which have the given user in the given membership state.
func (t *currentRoomStateTable) SelectRoomIDsWithMembership(
	ctx context.Context,
	userID string,
	membership string,
) ([]string, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectRoomIDsWithMembershipSQL, userID, membership).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var roomID string
		if err = rows.Scan(&roomID); err != nil {
			return nil, err
		}
		result = append(result, roomID)
	}
	return result, rows.Err()
}

// SelectRoomIDsWithAnyMembership returns a map of all memberships for the given user.
func (t *currentRoomStateTable) SelectRoomIDsWithAnyMembership(
	ctx context.Context,
	userID string,
) (map[string]string, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectRoomIDsWithAnyMembershipSQL, userID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]string{}
	for rows.Next() {
		var roomID, membership string
		if err := rows.Scan(&roomID, &membership); err != nil {
			return nil, err
		}
		result[roomID] = membership
	}
	return result, rows.Err()
}

// SelectCurrentState returns all the current state events for the given room.
func (t *currentRoomStateTable) SelectCurrentState(
	ctx context.Context,
	roomID string,
	stateFilter *synctypes.StateFilter,
	excludeEventIDs []string,
) ([]*rstypes.HeaderedEvent, error) {
	db := t.cm.Connection(ctx, true)
	senders, notSenders := getSendersStateFilterFilter(stateFilter)
	if stateFilter.LazyLoadMembers && !stateFilter.IncludeRedundantMembers {
		notTypes := &[]string{spec.MRoomMember}
		if stateFilter.NotTypes != nil {
			*stateFilter.NotTypes = append(*stateFilter.NotTypes, spec.MRoomMember)
		} else {
			stateFilter.NotTypes = notTypes
		}
	}
	rows, err := db.Raw(
		t.selectCurrentStateSQL,
		roomID,
		pq.StringArray(senders),
		pq.StringArray(notSenders),
		pq.StringArray(filterConvertTypeWildcardToSQL(stateFilter.Types)),
		pq.StringArray(filterConvertTypeWildcardToSQL(stateFilter.NotTypes)),
		stateFilter.ContainsURL,
		pq.StringArray(excludeEventIDs),
	).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rowsToEvents(rows)
}

// DeleteRoomStateByEventID deletes a room state by event ID.
func (t *currentRoomStateTable) DeleteRoomStateByEventID(
	ctx context.Context,
	eventID string,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteRoomStateByEventIDSQL, eventID).Error
}

// DeleteRoomStateForRoom deletes all room state for a given room.
func (t *currentRoomStateTable) DeleteRoomStateForRoom(
	ctx context.Context,
	roomID string,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteRoomStateForRoomSQL, roomID).Error
}

// UpsertRoomState inserts or updates a state event for a room.
func (t *currentRoomStateTable) UpsertRoomState(
	ctx context.Context,
	event *rstypes.HeaderedEvent,
	membership *string,
	addedAt types.StreamPosition,
) error {
	containsURL := false
	var content map[string]interface{}
	if json.Unmarshal(event.Content(), &content) == nil {
		_, containsURL = content["url"]
	}
	headeredJSON, err := json.Marshal(event)
	if err != nil {
		return err
	}
	db := t.cm.Connection(ctx, false)
	return db.Exec(
		t.upsertRoomStateSQL,
		event.RoomID().String(),
		event.EventID(),
		event.Type(),
		event.UserID.String(),
		containsURL,
		*event.StateKeyResolved,
		headeredJSON,
		membership,
		addedAt,
		event.Visibility,
	).Error
}

// SelectEventsWithEventIDs returns all events with the given event IDs.
func (t *currentRoomStateTable) SelectEventsWithEventIDs(
	ctx context.Context,
	eventIDs []string,
) ([]types.StreamEvent, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectEventsWithEventIDsSQL, pq.StringArray(eventIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return currentRoomStateRowsToStreamEvents(rows)
}

// SelectStateEvent returns a single state event for the given room, type, and state key.
func (t *currentRoomStateTable) SelectStateEvent(
	ctx context.Context,
	roomID, evType, stateKey string,
) (*rstypes.HeaderedEvent, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectStateEventSQL, roomID, evType, stateKey).Row()
	var eventBytes []byte
	if err := row.Scan(&eventBytes); err != nil {
		return nil, err
	}
	var ev rstypes.HeaderedEvent
	if err := json.Unmarshal(eventBytes, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}

// SelectSharedUsers returns all users who share a room with the given user.
func (t *currentRoomStateTable) SelectSharedUsers(
	ctx context.Context,
	userID string,
	otherUserIDs []string,
) ([]string, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectSharedUsersSQL, userID, pq.StringArray(otherUserIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var user string
		if err := rows.Scan(&user); err != nil {
			return nil, err
		}
		result = append(result, user)
	}
	return result, rows.Err()
}

// SelectRoomHeroes returns up to 5 user IDs for heroes in a room, excluding the given user.
func (t *currentRoomStateTable) SelectRoomHeroes(
	ctx context.Context,
	roomID, excludeUserID string,
	memberships []string,
) ([]string, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectRoomHeroesSQL, roomID, pq.StringArray(memberships), excludeUserID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var heroes []string
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		heroes = append(heroes, userID)
	}
	return heroes, rows.Err()
}

// SelectMembershipCount returns the count of users with a given membership in a room.
func (t *currentRoomStateTable) SelectMembershipCount(
	ctx context.Context,
	roomID, membership string,
) (count int, err error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectMembershipCountSQL, roomID, membership).Row()
	err = row.Scan(&count)
	return
}

func currentRoomStateRowsToStreamEvents(rows *sql.Rows) ([]types.StreamEvent, error) {
	var events []types.StreamEvent
	for rows.Next() {
		var (
			eventID           string
			streamPos         types.StreamPosition
			eventBytes        []byte
			historyVisibility gomatrixserverlib.HistoryVisibility
		)
		if err := rows.Scan(&eventID, &streamPos, &eventBytes, &historyVisibility); err != nil {
			return nil, err
		}
		// TODO: Handle redacted events
		var ev rstypes.HeaderedEvent
		if err := json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, err
		}

		ev.Visibility = historyVisibility

		events = append(events, types.StreamEvent{
			HeaderedEvent:  &ev,
			StreamPosition: streamPos,
		})
	}

	return events, rows.Err()
}

func rowsToEvents(rows *sql.Rows) ([]*rstypes.HeaderedEvent, error) {
	result := []*rstypes.HeaderedEvent{}
	for rows.Next() {
		var eventID string
		var eventBytes []byte
		if err := rows.Scan(&eventID, &eventBytes); err != nil {
			return nil, err
		}
		// TODO: Handle redacted events
		var ev rstypes.HeaderedEvent
		if err := json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, err
		}
		result = append(result, &ev)
	}
	return result, rows.Err()
}
