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
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
)

// SQL query definition constants
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

const currentRoomStateSchemaRevert = `
DROP TABLE IF EXISTS syncapi_current_room_state;
`

// SQL query for upserting room state
const upsertRoomStateSQL = "" +
	"INSERT INTO syncapi_current_room_state (room_id, event_id, type, sender, contains_url, state_key, headered_event_json, membership, added_at, history_visibility)" +
	" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)" +
	" ON CONFLICT ON CONSTRAINT syncapi_room_state_unique" +
	" DO UPDATE SET event_id = $2, sender=$4, contains_url=$5, headered_event_json = $7, membership = $8, added_at = $9"

// SQL query for deleting room state by event ID
const deleteRoomStateByEventIDSQL = "" +
	"DELETE FROM syncapi_current_room_state WHERE event_id = $1"

// SQL query for deleting room state for a room
const deleteRoomStateForRoomSQL = "" +
	"DELETE FROM syncapi_current_room_state WHERE room_id = $1"

// SQL query for selecting room IDs with specific membership
const selectRoomIDsWithMembershipSQL = "" +
	"SELECT DISTINCT room_id FROM syncapi_current_room_state WHERE type = 'm.room.member' AND state_key = $1 AND membership = $2"

// SQL query for selecting room IDs with any membership
const selectRoomIDsWithAnyMembershipSQL = "" +
	"SELECT room_id, membership FROM syncapi_current_room_state WHERE type = 'm.room.member' AND state_key = $1"

// SQL query for selecting current state
const selectCurrentStateSQL = "" +
	"SELECT event_id, headered_event_json FROM syncapi_current_room_state WHERE room_id = $1" +
	" AND ( $2::text[] IS NULL OR     sender  = ANY($2)  )" +
	" AND ( $3::text[] IS NULL OR NOT(sender  = ANY($3)) )" +
	" AND ( $4::text[] IS NULL OR     type LIKE ANY($4)  )" +
	" AND ( $5::text[] IS NULL OR NOT(type LIKE ANY($5)) )" +
	" AND ( $6::bool IS NULL   OR     contains_url = $6  )" +
	" AND (event_id = ANY($7)) IS NOT TRUE"

// SQL query for selecting joined users
const selectJoinedUsersSQL = "" +
	"SELECT room_id, state_key FROM syncapi_current_room_state WHERE type = 'm.room.member' AND membership = 'join'"

// SQL query for selecting joined users in a room
const selectJoinedUsersInRoomSQL = "" +
	"SELECT room_id, state_key FROM syncapi_current_room_state WHERE type = 'm.room.member' AND membership = 'join' AND room_id = ANY($1)"

// SQL query for selecting a state event
const selectStateEventSQL = "" +
	"SELECT headered_event_json FROM syncapi_current_room_state WHERE room_id = $1 AND type = $2 AND state_key = $3"

// SQL query for selecting events with event IDs
const selectEventsWithEventIDsSQL = "" +
	"SELECT event_id, added_at, headered_event_json, history_visibility FROM syncapi_current_room_state WHERE event_id = ANY($1)"

// SQL query for selecting shared users
const selectSharedUsersSQL = "" +
	"SELECT state_key FROM syncapi_current_room_state WHERE room_id = ANY(" +
	"	SELECT DISTINCT room_id FROM syncapi_current_room_state WHERE state_key = $1 AND membership='join'" +
	") AND type = 'm.room.member' AND state_key = ANY($2) AND membership IN ('join', 'invite');"

// SQL query for selecting membership count
const selectMembershipCount = `SELECT count(*) FROM syncapi_current_room_state WHERE type = 'm.room.member' AND room_id = $1 AND membership = $2`

// SQL query for selecting room heroes
const selectRoomHeroes = `
SELECT state_key FROM syncapi_current_room_state
WHERE type = 'm.room.member' AND room_id = $1 AND membership = ANY($2) AND state_key != $3
ORDER BY added_at, state_key
LIMIT 5
`

// currentRoomStateTable implements tables.CurrentRoomState
type currentRoomStateTable struct {
	cm sqlutil.ConnectionManager

	// SQL query fields
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

// NewPostgresCurrentRoomStateTable creates a new CurrentRoomState table
func NewPostgresCurrentRoomStateTable(_ context.Context, cm sqlutil.ConnectionManager) (tables.CurrentRoomState, error) {

	// Run migrations
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "syncapi_current_room_state_table_schema_001",
		Patch:       currentRoomStateSchema,
		RevertPatch: currentRoomStateSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	t := &currentRoomStateTable{
		cm: cm,

		// Initialise SQL query fields
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

	return t, nil
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
	defer internal.CloseAndLogIfError(ctx, rows, "selectJoinedUsers: rows.close() failed")

	result := make(map[string][]string)
	var roomID string
	var userID string
	for rows.Next() {
		if err := rows.Scan(&roomID, &userID); err != nil {
			return nil, err
		}
		users := result[roomID]
		users = append(users, userID)
		result[roomID] = users
	}
	return result, rows.Err()
}

// SelectJoinedUsersInRoom returns a map of room ID to a list of joined user IDs for the given rooms.
func (t *currentRoomStateTable) SelectJoinedUsersInRoom(
	ctx context.Context, roomIDs []string,
) (map[string][]string, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectJoinedUsersInRoomSQL, pq.Array(roomIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectJoinedUsersInRoom: rows.close() failed")

	result := make(map[string][]string)
	var roomID string
	var userID string
	for rows.Next() {
		if err := rows.Scan(&roomID, &userID); err != nil {
			return nil, err
		}
		users := result[roomID]
		users = append(users, userID)
		result[roomID] = users
	}
	return result, rows.Err()
}

// SelectRoomIDsWithMembership returns the list of room IDs which have the given user in the given membership state.
func (t *currentRoomStateTable) SelectRoomIDsWithMembership(
	ctx context.Context,
	userID string,
	membership string, // nolint: unparam
) ([]string, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectRoomIDsWithMembershipSQL, userID, membership).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectRoomIDsWithMembership: rows.close() failed")

	var result []string
	var roomID string
	for rows.Next() {
		if err := rows.Scan(&roomID); err != nil {
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
	defer internal.CloseAndLogIfError(ctx, rows, "selectRoomIDsWithAnyMembership: rows.close() failed")

	result := make(map[string]string)
	var roomID string
	var membership string
	for rows.Next() {
		if err := rows.Scan(&roomID, &membership); err != nil {
			return nil, err
		}
		result[roomID] = membership
	}
	return result, rows.Err()
}

// SelectCurrentState returns all the current state events for the given room.
func (t *currentRoomStateTable) SelectCurrentState(
	ctx context.Context, roomID string,
	stateFilter *synctypes.StateFilter,
	excludeEventIDs []string,
) ([]*rstypes.HeaderedEvent, error) {
	db := t.cm.Connection(ctx, true)

	senders, notSenders := getSendersStateFilterFilter(stateFilter)
	// We're going to query members later, so remove them from this request
	if stateFilter.LazyLoadMembers && !stateFilter.IncludeRedundantMembers {
		notTypes := &[]string{spec.MRoomMember}
		if stateFilter.NotTypes != nil {
			*stateFilter.NotTypes = append(*stateFilter.NotTypes, spec.MRoomMember)
		} else {
			stateFilter.NotTypes = notTypes
		}
	}
	rows, err := db.Raw(t.selectCurrentStateSQL, roomID,
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
	defer internal.CloseAndLogIfError(ctx, rows, "selectCurrentState: rows.close() failed")

	return rowsToEvents(rows)
}

// DeleteRoomStateByEventID deletes the room state for a given event ID.
func (t *currentRoomStateTable) DeleteRoomStateByEventID(
	ctx context.Context, eventID string,
) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteRoomStateByEventIDSQL, eventID)
	return result.Error
}

// DeleteRoomStateForRoom deletes the room state for a given room ID.
func (t *currentRoomStateTable) DeleteRoomStateForRoom(
	ctx context.Context, roomID string,
) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteRoomStateForRoomSQL, roomID)
	return result.Error
}

// UpsertRoomState upserts the room state for a given event.
func (t *currentRoomStateTable) UpsertRoomState(
	ctx context.Context,
	event *rstypes.HeaderedEvent, membership *string, addedAt types.StreamPosition,
) error {
	// Parse content as JSON and search for an "url" key
	containsURL := false
	var content map[string]interface{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		// Set containsURL to false if there was an error unmarshalling the content
		// This means that we can't check whether the event contains a URL, so we
		// shouldn't consider this event as one that contains a URL
	} else {
		// Contains url?
		_, containsURL = content["url"]
	}

	// Marshall the event
	headeredJSON, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// upsert state event
	db := t.cm.Connection(ctx, false)
	err = db.Exec(
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
	return err
}

// SelectEventsWithEventIDs returns the events for the given event IDs.
func (t *currentRoomStateTable) SelectEventsWithEventIDs(
	ctx context.Context, eventIDs []string,
) ([]types.StreamEvent, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectEventsWithEventIDsSQL, pq.Array(eventIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectEventsWithEventIDs: rows.close() failed")

	return currentRoomStateRowsToStreamEvents(rows)
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
	return result, nil
}

// SelectStateEvent returns the current state event for the given event type and state key in the given room.
func (t *currentRoomStateTable) SelectStateEvent(
	ctx context.Context, roomID, evType, stateKey string,
) (*rstypes.HeaderedEvent, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectStateEventSQL, roomID, evType, stateKey).Row()

	var eventBytes []byte
	err := row.Scan(&eventBytes)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	var ev rstypes.HeaderedEvent
	if err = json.Unmarshal(eventBytes, &ev); err != nil {
		return nil, err
	}

	return &ev, nil
}

// SelectSharedUsers returns a subset of otherUserIDs that share a room with userID.
func (t *currentRoomStateTable) SelectSharedUsers(
	ctx context.Context, userID string, otherUserIDs []string,
) ([]string, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectSharedUsersSQL, userID, pq.Array(otherUserIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectSharedUsers: rows.close() failed")

	var result []string
	var sharedUserID string
	for rows.Next() {
		if err = rows.Scan(&sharedUserID); err != nil {
			return nil, err
		}
		result = append(result, sharedUserID)
	}
	return result, rows.Err()
}

// SelectRoomHeroes returns up to 5 users joined or invited to a room.
func (t *currentRoomStateTable) SelectRoomHeroes(ctx context.Context, roomID, excludeUserID string, memberships []string) ([]string, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectRoomHeroesSQL, roomID, pq.Array(memberships), excludeUserID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectRoomHeroes: rows.close() failed")

	var heroes []string
	for rows.Next() {
		var hero string
		if err = rows.Scan(&hero); err != nil {
			return nil, err
		}
		heroes = append(heroes, hero)
	}
	return heroes, rows.Err()
}

func (t *currentRoomStateTable) SelectMembershipCount(ctx context.Context, roomID, membership string) (count int, err error) {
	db := t.cm.Connection(ctx, true)
	err = db.Raw(t.selectMembershipCountSQL, roomID, membership).Row().Scan(&count)
	return
}
